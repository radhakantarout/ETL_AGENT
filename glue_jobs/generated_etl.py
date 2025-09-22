import sys
import argparse
import json
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="ETL_Job"):
    """Create Spark session with S3 configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        # Read rules file from S3
        rules_df = spark.read.text(rules_s3_path)
        rules_content = rules_df.collect()[0][0]
        return json.loads(rules_content)
    except Exception as e:
        logger.error(f"Error reading DQ rules: {e}")
        return {}

def read_business_mapping(s3_bucket, aws_region):
    """Read business mapping from Excel file in S3"""
    try:
        s3_client = boto3.client('s3', region_name=aws_region)
        
        # Download Excel file from S3
        mapping_key = "raw/business_mapping.xlsx"
        response = s3_client.get_object(Bucket=s3_bucket, Key=mapping_key)
        
        # Read Excel file
        mapping_df = pd.read_excel(response['Body'].read())
        return mapping_df.to_dict('records')
    except Exception as e:
        logger.error(f"Error reading business mapping: {e}")
        return []

def get_csv_files_from_s3(s3_bucket, aws_region, prefix="raw/"):
    """Get list of CSV files from S3"""
    try:
        s3_client = boto3.client('s3', region_name=aws_region)
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        
        csv_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    csv_files.append(f"s3a://{s3_bucket}/{obj['Key']}")
        
        return csv_files
    except Exception as e:
        logger.error(f"Error listing S3 files: {e}")
        return []

def apply_dq_rules(df, dq_rules, category):
    """Apply data quality rules and separate clean and reject records"""
    if category not in dq_rules:
        logger.warning(f"No DQ rules found for category: {category}")
        return df, df.filter(lit(False))
    
    rules = dq_rules[category]
    conditions = []
    
    for rule in rules:
        rule_type = rule.get('type')
        column = rule.get('column')
        
        if rule_type == 'not_null':
            conditions.append(col(column).isNotNull())
        elif rule_type == 'unique':
            # For unique constraint, we'll mark duplicates as rejects
            window_spec = Window.partitionBy(column)
            df = df.withColumn(f"{column}_count", count(column).over(window_spec))
            conditions.append(col(f"{column}_count") == 1)
        elif rule_type == 'range':
            min_val = rule.get('min_value')
            max_val = rule.get('max_value')
            if min_val is not None and max_val is not None:
                conditions.append((col(column) >= min_val) & (col(column) <= max_val))
        elif rule_type == 'regex':
            pattern = rule.get('pattern')
            if pattern:
                conditions.append(col(column).rlike(pattern))
        elif rule_type == 'length':
            max_length = rule.get('max_length')
            if max_length:
                conditions.append(length(col(column)) <= max_length)
    
    if conditions:
        # Combine all conditions with AND
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition & condition
        
        clean_df = df.filter(combined_condition)
        reject_df = df.filter(~combined_condition)
    else:
        clean_df = df
        reject_df = df.filter(lit(False))
    
    # Clean up temporary columns
    temp_columns = [col_name for col_name in df.columns if col_name.endswith('_count')]
    for temp_col in temp_columns:
        if temp_col in clean_df.columns:
            clean_df = clean_df.drop(temp_col)
        if temp_col in reject_df.columns:
            reject_df = reject_df.drop(temp_col)
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_rules):
    """Apply business transformations based on mapping rules"""
    try:
        for rule in mapping_rules:
            source_col = rule.get('source_column')
            target_col = rule.get('target_column')
            transformation = rule.get('transformation', 'direct')
            
            if transformation == 'direct' and source_col in df.columns:
                df = df.withColumnRenamed(source_col, target_col)
            elif transformation == 'upper' and source_col in df.columns:
                df = df.withColumn(target_col, upper(col(source_col)))
            elif transformation == 'lower' and source_col in df.columns:
                df = df.withColumn(target_col, lower(col(source_col)))
            elif transformation == 'trim' and source_col in df.columns:
                df = df.withColumn(target_col, trim(col(source_col)))
            elif transformation == 'concat':
                columns_to_concat = rule.get('source_columns', [])
                separator = rule.get('separator', '')
                if all(col_name in df.columns for col_name in columns_to_concat):
                    df = df.withColumn(target_col, concat_ws(separator, *[col(c) for c in columns_to_concat]))
        
        return df
    except Exception as e:
        logger.error(f"Error applying business mapping: {e}")
        return df

def create_glue_table(aws_region, database_name, table_name, s3_location, columns):
    """Create or update Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Create database if it doesn't exist
        try:
            glue_client.create_database(
                DatabaseInput={'Name': database_name}
            )
        except glue_client.exceptions.AlreadyExistsException:
            pass
        
        # Prepare columns for Glue table
        glue_columns = []
        for col_name, col_type in columns:
            glue_type = 'string'  # Default type
            if 'int' in col_type.lower():
                glue_type = 'bigint'
            elif 'double' in col_type.lower() or 'float' in col_type.lower():
                glue_type = 'double'
            elif 'boolean' in col_type.lower():
                glue_type = 'boolean'
            elif 'timestamp' in col_type.lower():
                glue_type = 'timestamp'
            elif 'date' in col_type.lower():
                glue_type = 'date'
            
            glue_columns.append({'Name': col_name, 'Type': glue_type})
        
        # Create/update table
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': glue_columns,
                'Location': s3_location,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            },
            'PartitionKeys': [{'Name': 'ingest_date', 'Type': 'string'}]
        }
        
        try:
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            logger.info(f"Created Glue table: {database_name}.{table_name}")
        except glue_client.exceptions.AlreadyExistsException:
            glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            logger.info(f"Updated Glue table: {database_name}.{table_name}")
            
    except Exception as e:
        logger.error(f"Error creating/updating Glue table: {e}")

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read DQ rules
        dq_rules = read_dq_rules(spark, args.rules_s3)
        logger.info("DQ rules loaded successfully")
        
        # Read business mapping
        business_mapping = read_business_mapping(args.s3_bucket, args.aws_region)
        logger.info("Business mapping loaded successfully")
        
        # Get CSV files from S3
        csv_files = get_csv_files_from_s3(args.s3_bucket, args.aws_region)
        logger.info(f"Found {len(csv_files)} CSV files")
        
        for csv_file in csv_files:
            # Extract category from file path
            category = csv_file.split('/')[-1].replace('.csv', '')
            logger.info(f"Processing category: {category}")
            
            # Read CSV file
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file)
            
            # Apply DQ rules
            clean_df, reject_df = apply_dq_rules(df, dq_rules, category)
            
            # Add audit columns
            clean_df = clean_df.withColumn("load_timestamp", current_timestamp()) \
                             .withColumn("load_date", lit(args.ingest_date))
            
            reject_df = reject_df.withColumn("load_timestamp", current_timestamp()) \
                               .withColumn("load_date", lit(args.ingest_date)) \
                               .withColumn("reject_reason", lit("DQ_VIOLATION"))
            
            # Write clean data to access layer
            access_path = f"s3a://{args.s3_bucket}/access/{category}"
            clean_df.write.mode("append").partitionBy("ingest_date") \
                   .parquet(access_path)
            
            # Write rejects
            reject_path = f"s3a://{args.s3_bucket}/access/{category}/rejects"
            if reject_df.count() > 0:
                reject_df.write.mode("append").partitionBy("ingest_date") \
                       .parquet(reject_path)
            
            # Apply business mapping for mart layer
            mart_df = apply_business_mapping(clean_df, business_mapping)
            
            # Write to mart layer
            mart_path = f"s3a://{args.s3_bucket}/mart/{category}"
            mart_df.write.mode("append").partitionBy("ingest_date") \
                  .parquet(mart_path)
            
            # Create Glue tables
            clean_columns = [(field.name, str(field.dataType)) for field in clean_df.schema.fields]
            mart_columns = [(field.name, str(field.dataType)) for field in mart_df.schema.fields]
            
            create_glue_table(
                args.aws_region, 
                "access_db", 
                f"{category}_clean", 
                access_path.replace("s3a://", "s3://"),
                clean_columns
            )
            
            create_glue_table(
                args.aws_region, 
                "mart_db", 
                f"{category}_mart", 
                mart_path.replace("s3a://", "s3://"),
                mart_columns
            )
            
            logger.info(f"Completed processing for category: {category}")
        
        logger.info("ETL job completed successfully")
        
    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()