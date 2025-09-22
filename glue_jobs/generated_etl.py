import sys
import json
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd

def create_spark_session(app_name="ETL_Data_Pipeline"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def read_json_from_s3(spark, s3_path):
    """Read JSON file from S3"""
    try:
        rdd = spark.sparkContext.textFile(s3_path)
        json_str = rdd.collect()[0] if rdd.count() > 0 else "{}"
        return json.loads(json_str)
    except Exception as e:
        print(f"Error reading JSON from S3: {e}")
        return {}

def read_excel_from_s3(s3_bucket, s3_key):
    """Read Excel file from S3 using boto3 and pandas"""
    try:
        s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        return pd.read_excel(obj['Body'].read())
    except Exception as e:
        print(f"Error reading Excel from S3: {e}")
        return pd.DataFrame()

def discover_csv_files(spark, s3_path):
    """Discover all CSV files in S3 path"""
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(s3_path)
        
        files = []
        if fs.exists(path):
            file_status = fs.listStatus(path)
            for status in file_status:
                file_path = str(status.getPath())
                if file_path.endswith('.csv'):
                    files.append(file_path)
        return files
    except Exception as e:
        print(f"Error discovering CSV files: {e}")
        return []

def apply_data_quality_rules(df, rules):
    """Apply data quality rules and separate clean vs reject records"""
    if not rules:
        return df, df.filter(lit(False))
    
    clean_conditions = []
    
    for rule in rules.get('rules', []):
        rule_type = rule.get('type')
        column = rule.get('column')
        
        if rule_type == 'not_null':
            clean_conditions.append(col(column).isNotNull())
        elif rule_type == 'not_empty':
            clean_conditions.append((col(column).isNotNull()) & (trim(col(column)) != ""))
        elif rule_type == 'numeric':
            clean_conditions.append(col(column).rlike(r'^-?\d+\.?\d*$'))
        elif rule_type == 'min_length':
            min_len = rule.get('value', 0)
            clean_conditions.append(length(col(column)) >= min_len)
        elif rule_type == 'max_length':
            max_len = rule.get('value', 1000)
            clean_conditions.append(length(col(column)) <= max_len)
        elif rule_type == 'regex':
            pattern = rule.get('pattern', '.*')
            clean_conditions.append(col(column).rlike(pattern))
    
    if clean_conditions:
        overall_condition = clean_conditions[0]
        for condition in clean_conditions[1:]:
            overall_condition = overall_condition & condition
        
        clean_df = df.filter(overall_condition)
        reject_df = df.filter(~overall_condition)
    else:
        clean_df = df
        reject_df = df.filter(lit(False))
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_df):
    """Apply business mappings from Excel file"""
    if mapping_df.empty:
        return df
    
    try:
        # Assume mapping_df has columns: source_column, target_column, transformation
        mapped_df = df
        
        for _, row in mapping_df.iterrows():
            source_col = row.get('source_column')
            target_col = row.get('target_column', source_col)
            transformation = row.get('transformation', 'direct')
            
            if source_col in df.columns:
                if transformation == 'direct':
                    mapped_df = mapped_df.withColumnRenamed(source_col, target_col)
                elif transformation == 'upper':
                    mapped_df = mapped_df.withColumn(target_col, upper(col(source_col)))
                elif transformation == 'lower':
                    mapped_df = mapped_df.withColumn(target_col, lower(col(source_col)))
                elif transformation == 'trim':
                    mapped_df = mapped_df.withColumn(target_col, trim(col(source_col)))
        
        return mapped_df
    except Exception as e:
        print(f"Error applying business mapping: {e}")
        return df

def create_glue_table(database, table_name, s3_location, columns, aws_region):
    """Create or update AWS Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Create database if not exists
        try:
            glue_client.create_database(DatabaseInput={'Name': database})
        except glue_client.exceptions.AlreadyExistsException:
            pass
        
        # Prepare table input
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': s3_location,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            },
            'PartitionKeys': [{'Name': 'ingest_date', 'Type': 'string'}],
            'TableType': 'EXTERNAL_TABLE'
        }
        
        # Try to update table first, if not exists then create
        try:
            glue_client.update_table(DatabaseName=database, TableInput=table_input)
            print(f"Updated Glue table: {database}.{table_name}")
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_table(DatabaseName=database, TableInput=table_input)
            print(f"Created Glue table: {database}.{table_name}")
            
    except Exception as e:
        print(f"Error creating Glue table: {e}")

def get_glue_columns_from_df(df):
    """Convert Spark DataFrame schema to Glue table columns"""
    columns = []
    for field in df.schema.fields:
        if field.name != 'ingest_date':  # Exclude partition column
            glue_type = 'string'  # Default to string
            if isinstance(field.dataType, IntegerType):
                glue_type = 'int'
            elif isinstance(field.dataType, LongType):
                glue_type = 'bigint'
            elif isinstance(field.dataType, DoubleType):
                glue_type = 'double'
            elif isinstance(field.dataType, FloatType):
                glue_type = 'float'
            elif isinstance(field.dataType, BooleanType):
                glue_type = 'boolean'
            elif isinstance(field.dataType, DateType):
                glue_type = 'date'
            elif isinstance(field.dataType, TimestampType):
                glue_type = 'timestamp'
                
            columns.append({'Name': field.name, 'Type': glue_type})
    
    return columns

def main():
    parser = argparse.ArgumentParser(description='ETL Data Pipeline')
    parser.add_argument('--rules_s3', required=True, help='S3 path to dq_rules.json')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            # Read data quality rules
            print("Reading data quality rules...")
            dq_rules = read_json_from_s3(spark, args.rules_s3)
            
            # Read business mapping
            print("Reading business mapping...")
            mapping_df = read_excel_from_s3(args.s3_bucket, 'business_mapping.xlsx')
            
            # Discover and read CSV files
            source_path = f"s3://{args.s3_bucket}/raw/"
            print(f"Discovering CSV files in {source_path}")
            csv_files = discover_csv_files(spark, source_path)
            
            if not csv_files:
                # Fallback to wildcard read
                csv_files = [f"{source_path}*.csv"]
            
            # Read all CSV files
            print(f"Reading {len(csv_files)} CSV files...")
            df_list = []
            for csv_file in csv_files:
                try:
                    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file)
                    df_list.append(df)
                except Exception as e:
                    print(f"Warning: Could not read {csv_file}: {e}")
            
            if not df_list:
                print("No CSV files found or readable")
                return
            
            # Union all DataFrames
            combined_df = df_list[0]
            for df in df_list[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            
            # Add ingest_date column
            combined_df = combined_df.withColumn("ingest_date", lit(args.ingest_date))
            
            print(f"Total records read: {combined_df.count()}")
            
            # Apply data quality rules
            print("Applying data quality rules...")
            clean_df, reject_df = apply_data_quality_rules(combined_df, dq_rules)
            
            print(f"Clean records: {clean_df.count()}")
            print(f"Rejected records: {reject_df.count()}")
            
            # Determine category (assuming single category for now)
            category = "general"  # This could be derived from rules or file names
            
            # Write clean data to access layer
            access_path = f"s3://{args.s3_bucket}/access/{category}"
            print(f"Writing clean data to {access_path}")
            clean_df.write.mode("overwrite").partitionBy("ingest_date").parquet(access_path)
            
            # Write rejects
            rejects_path = f"s3://{args.s3_bucket}/access/{category}/rejects"
            print(f"Writing rejects to {rejects_path}")
            if reject_df.count() > 0:
                reject_df.write.mode("overwrite").partitionBy("ingest_date").parquet(rejects_path)
            
            # Apply business mapping for mart layer
            print("Applying business mapping...")
            mart_df = apply_business_mapping(clean_df, mapping_df)
            
            # Write to mart layer
            mart_path = f"s3://{args.s3_bucket}/mart/{category}"
            print(f"Writing mart data to {mart_path}")
            mart_df.write.mode("overwrite").partitionBy("ingest_date").parquet(mart_path)
            
            # Create Glue tables
            print("Creating/updating Glue tables...")
            
            # Access layer table
            access_columns = get_glue_columns_from_df(clean_df)
            create_glue_table("access_db", f"{category}_clean", access_path, access_columns, args.aws_region)
            
            # Mart layer table
            mart_columns = get_glue_columns_from_df(mart_df)
            create_glue_table("mart_db", f"{category}_mart", mart_path, mart_columns, args.aws_region)
            
            # Rejects table
            if reject_df.count() > 0:
                reject_columns = get_glue_columns_from_df(reject_df)
                create_glue_table("access_db", f"{category}_rejects", rejects_path, reject_columns, args.aws_region)
            
            print("ETL pipeline completed successfully!")
            break
            
        except Exception as e:
            retry_count += 1
            print(f"Error occurred (attempt {retry_count}/{max_retries}): {e}")
            
            if retry_count >= max_retries:
                print("Max retries reached. Pipeline failed.")
                raise e
            else:
                print(f"Retrying in 30 seconds...")
                import time
                time.sleep(30)
    
    spark.stop()

if __name__ == "__main__":
    main()