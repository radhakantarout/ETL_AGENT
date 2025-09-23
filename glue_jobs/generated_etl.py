import argparse
import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd

def create_spark_session(app_name="ETL_Job"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        # Read JSON file from S3
        df = spark.read.text(rules_s3_path)
        json_content = df.collect()[0][0]
        return json.loads(json_content)
    except Exception as e:
        print(f"Error reading DQ rules: {e}")
        return {}

def read_business_mapping(spark, s3_bucket):
    """Read business mapping from Excel file"""
    try:
        mapping_path = f"s3a://{s3_bucket}/business_mapping.xlsx"
        # Use pandas to read Excel, then convert to Spark DataFrame
        pandas_df = pd.read_excel(mapping_path.replace("s3a://", "s3://"))
        return spark.createDataFrame(pandas_df)
    except Exception as e:
        print(f"Error reading business mapping: {e}")
        return None

def discover_csv_files(spark, source_path):
    """Discover all CSV files in the source path"""
    try:
        # Read all CSV files using wildcard
        df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{source_path}*.csv")
        
        # Add source file name
        df = df.withColumn("source_file", input_file_name())
        return df
    except Exception as e:
        print(f"Error reading CSV files: {e}")
        return None

def apply_dq_rules(df, dq_rules):
    """Apply data quality rules and separate clean and reject records"""
    if not dq_rules or df is None:
        return df, None
    
    clean_df = df
    reject_conditions = []
    
    for rule_name, rule_config in dq_rules.items():
        rule_type = rule_config.get("type", "")
        column = rule_config.get("column", "")
        
        if rule_type == "not_null" and column:
            condition = col(column).isNull()
            reject_conditions.append(condition)
            
        elif rule_type == "range" and column:
            min_val = rule_config.get("min")
            max_val = rule_config.get("max")
            if min_val is not None and max_val is not None:
                condition = (col(column) < min_val) | (col(column) > max_val)
                reject_conditions.append(condition)
                
        elif rule_type == "regex" and column:
            pattern = rule_config.get("pattern", "")
            if pattern:
                condition = ~col(column).rlike(pattern)
                reject_conditions.append(condition)
                
        elif rule_type == "unique" and column:
            # Handle duplicates
            window_spec = Window.partitionBy(column)
            clean_df = clean_df.withColumn("row_count", count("*").over(window_spec))
            condition = col("row_count") > 1
            reject_conditions.append(condition)
    
    # Combine all reject conditions
    if reject_conditions:
        combined_reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            combined_reject_condition = combined_reject_condition | condition
        
        # Separate clean and reject records
        reject_df = clean_df.filter(combined_reject_condition) \
            .withColumn("reject_reason", lit("DQ_RULE_VIOLATION")) \
            .withColumn("reject_timestamp", current_timestamp())
        
        clean_df = clean_df.filter(~combined_reject_condition)
        
        # Remove helper columns
        if "row_count" in clean_df.columns:
            clean_df = clean_df.drop("row_count")
            reject_df = reject_df.drop("row_count")
            
        return clean_df, reject_df
    
    return clean_df, None

def apply_business_mapping(clean_df, mapping_df):
    """Apply business transformations based on mapping file"""
    if mapping_df is None:
        return clean_df
    
    try:
        # Collect mapping rules
        mapping_rules = mapping_df.collect()
        
        mart_df = clean_df
        for row in mapping_rules:
            source_col = row.get("source_column", "")
            target_col = row.get("target_column", "")
            transformation = row.get("transformation", "")
            
            if source_col and target_col:
                if transformation == "upper":
                    mart_df = mart_df.withColumn(target_col, upper(col(source_col)))
                elif transformation == "lower":
                    mart_df = mart_df.withColumn(target_col, lower(col(source_col)))
                elif transformation == "trim":
                    mart_df = mart_df.withColumn(target_col, trim(col(source_col)))
                elif transformation == "date_format":
                    format_str = row.get("format", "yyyy-MM-dd")
                    mart_df = mart_df.withColumn(target_col, date_format(col(source_col), format_str))
                else:
                    # Direct mapping
                    mart_df = mart_df.withColumn(target_col, col(source_col))
        
        return mart_df
    except Exception as e:
        print(f"Error applying business mapping: {e}")
        return clean_df

def write_to_s3(df, output_path, format_type="parquet", mode="overwrite"):
    """Write DataFrame to S3"""
    try:
        if df is not None and df.count() > 0:
            df.write.mode(mode).format(format_type).save(output_path)
            print(f"Successfully wrote data to {output_path}")
        else:
            print(f"No data to write to {output_path}")
    except Exception as e:
        print(f"Error writing to {output_path}: {e}")

def create_glue_table(database_name, table_name, s3_location, aws_region):
    """Create or update Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Get table schema from the parquet files
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'column1', 'Type': 'string'},  # This should be dynamically generated
                ],
                'Location': s3_location,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                }
            },
            'PartitionKeys': [
                {'Name': 'ingest_date', 'Type': 'string'}
            ]
        }
        
        try:
            glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Updated Glue table: {table_name}")
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Created Glue table: {table_name}")
            
    except Exception as e:
        print(f"Error creating/updating Glue table {table_name}: {e}")

def extract_category_from_path(source_path):
    """Extract category from source path for partitioning"""
    # This is a simple extraction - modify based on your path structure
    parts = source_path.strip('/').split('/')
    return parts[-1] if parts else "default"

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = create_spark_session()
    
    try:
        # Define paths
        source_path = f"s3a://{args.s3_bucket}/raw/"
        category = extract_category_from_path(source_path)
        
        access_clean_path = f"s3a://{args.s3_bucket}/access/{category}/ingest_date={args.ingest_date}/"
        access_reject_path = f"s3a://{args.s3_bucket}/access/{category}/rejects/ingest_date={args.ingest_date}/"
        mart_path = f"s3a://{args.s3_bucket}/mart/{category}/ingest_date={args.ingest_date}/"
        
        # Read DQ rules
        print("Reading DQ rules...")
        dq_rules = read_dq_rules(spark, args.rules_s3)
        
        # Read business mapping
        print("Reading business mapping...")
        mapping_df = read_business_mapping(spark, args.s3_bucket)
        
        # Discover and read CSV files
        print("Reading CSV files...")
        raw_df = discover_csv_files(spark, source_path)
        
        if raw_df is None:
            print("No data found in source path")
            return
        
        # Add processing metadata
        raw_df = raw_df.withColumn("load_date", lit(args.ingest_date)) \
                      .withColumn("processing_timestamp", current_timestamp())
        
        # Apply DQ rules
        print("Applying data quality rules...")
        clean_df, reject_df = apply_dq_rules(raw_df, dq_rules)
        
        # Write clean data to access layer
        print("Writing clean data to access layer...")
        write_to_s3(clean_df, access_clean_path)
        
        # Write reject data
        if reject_df is not None:
            print("Writing reject data...")
            write_to_s3(reject_df, access_reject_path)
        
        # Apply business mapping for mart layer
        if clean_df is not None:
            print("Applying business transformations...")
            mart_df = apply_business_mapping(clean_df, mapping_df)
            
            # Write to mart layer
            print("Writing data to mart layer...")
            write_to_s3(mart_df, mart_path)
        
        # Create/update Glue tables
        print("Creating/updating Glue tables...")
        create_glue_table("default", f"{category}_access", access_clean_path, args.aws_region)
        create_glue_table("default", f"{category}_mart", mart_path, args.aws_region)
        
        print("ETL job completed successfully!")
        
    except Exception as e:
        print(f"ETL job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()