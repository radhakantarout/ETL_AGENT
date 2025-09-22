import sys
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="ETL_Job"):
    """Create Spark session with optimized configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def load_dq_rules(spark, rules_s3_path):
    """Load data quality rules from S3"""
    try:
        # Read JSON file from S3
        rules_df = spark.read.option("multiline", "true").text(rules_s3_path)
        rules_content = rules_df.collect()[0]['value']
        return json.loads(rules_content)
    except Exception as e:
        logger.error(f"Error loading DQ rules: {str(e)}")
        return {}

def load_business_mapping(s3_bucket, aws_region):
    """Load business mapping from Excel file in S3"""
    try:
        s3_client = boto3.client('s3', region_name=aws_region)
        
        # Download Excel file to local temp
        local_path = "/tmp/business_mapping.xlsx"
        s3_client.download_file(s3_bucket, "business_mapping.xlsx", local_path)
        
        # Read Excel file
        mapping_df = pd.read_excel(local_path)
        return mapping_df.to_dict('records')
    except Exception as e:
        logger.error(f"Error loading business mapping: {str(e)}")
        return []

def discover_csv_files(spark, s3_bucket):
    """Discover all CSV files in the raw S3 bucket"""
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"s3a://{s3_bucket}/raw/")
        
        csv_files = []
        file_status = fs.listStatus(path)
        
        for status in file_status:
            file_path = status.getPath().toString()
            if file_path.endswith('.csv'):
                csv_files.append(file_path)
                
        return csv_files
    except Exception as e:
        logger.error(f"Error discovering CSV files: {str(e)}")
        return []

def apply_data_quality_rules(df, dq_rules, table_name):
    """Apply data quality rules and separate clean and rejected records"""
    if not dq_rules or table_name not in dq_rules:
        logger.warning(f"No DQ rules found for table: {table_name}")
        return df, df.filter(lit(False))  # Return all as clean, empty rejects
    
    rules = dq_rules[table_name]
    reject_conditions = []
    
    # Apply null checks
    if 'null_checks' in rules:
        for column in rules['null_checks']:
            if column in df.columns:
                reject_conditions.append(col(column).isNull())
    
    # Apply range checks
    if 'range_checks' in rules:
        for rule in rules['range_checks']:
            column = rule.get('column')
            min_val = rule.get('min')
            max_val = rule.get('max')
            if column in df.columns:
                if min_val is not None:
                    reject_conditions.append(col(column) < min_val)
                if max_val is not None:
                    reject_conditions.append(col(column) > max_val)
    
    # Apply regex patterns
    if 'pattern_checks' in rules:
        for rule in rules['pattern_checks']:
            column = rule.get('column')
            pattern = rule.get('pattern')
            if column in df.columns and pattern:
                reject_conditions.append(~col(column).rlike(pattern))
    
    # Apply duplicate checks
    if 'duplicate_checks' in rules:
        key_columns = rules['duplicate_checks']
        existing_columns = [c for c in key_columns if c in df.columns]
        if existing_columns:
            window_spec = Window.partitionBy(*existing_columns)
            df = df.withColumn("row_number", row_number().over(window_spec.orderBy(monotonically_increasing_id())))
            reject_conditions.append(col("row_number") > 1)
            df = df.drop("row_number")
    
    # Combine all reject conditions
    if reject_conditions:
        combined_reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            combined_reject_condition = combined_reject_condition | condition
        
        rejected_df = df.filter(combined_reject_condition)
        clean_df = df.filter(~combined_reject_condition)
    else:
        clean_df = df
        rejected_df = df.filter(lit(False))
    
    return clean_df, rejected_df

def get_table_name_from_path(file_path):
    """Extract table name from file path"""
    return file_path.split('/')[-1].replace('.csv', '').lower()

def create_glue_table(table_name, s3_path, df_schema, database_name, aws_region, partition_keys=None):
    """Create or update Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Convert Spark schema to Glue columns
        columns = []
        for field in df_schema.fields:
            if partition_keys and field.name in partition_keys:
                continue  # Skip partition columns in regular columns
            
            glue_type = "string"  # Default
            if isinstance(field.dataType, IntegerType):
                glue_type = "int"
            elif isinstance(field.dataType, LongType):
                glue_type = "bigint"
            elif isinstance(field.dataType, DoubleType):
                glue_type = "double"
            elif isinstance(field.dataType, BooleanType):
                glue_type = "boolean"
            elif isinstance(field.dataType, TimestampType):
                glue_type = "timestamp"
            elif isinstance(field.dataType, DateType):
                glue_type = "date"
            
            columns.append({
                'Name': field.name,
                'Type': glue_type
            })
        
        # Partition columns
        partition_columns = []
        if partition_keys:
            for key in partition_keys:
                partition_columns.append({
                    'Name': key,
                    'Type': 'string'
                })
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': s3_path,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.