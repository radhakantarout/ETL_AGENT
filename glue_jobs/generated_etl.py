```python
import argparse
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import re
from urllib.parse import urlparse

def create_spark_session(app_name="ETL_Job"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def read_s3_json(s3_path, aws_region):
    s3_client = boto3.client('s3', region_name=aws_region)
    parsed_url = urlparse(s3_path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"Error reading {s3_path}: {str(e)}")
        return {}

def read_excel_from_s3(s3_path, aws_region):
    s3_client = boto3.client('s3', region_name=aws_region)
    parsed_url = urlparse(s3_path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        df = pd.read_excel(content, engine='openpyxl')
        return df
    except Exception as e:
        print(f"Error reading Excel file {s3_path}: {str(e)}")
        return pd.DataFrame()

def discover_csv_files(spark, source_path):
    try:
        # Try to read with wildcard pattern
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{source_path}/*.csv")
        return df
    except Exception as e:
        print(f"Error reading CSV files from {source_path}: {str(e)}")
        return None

def apply_dq_rules(df, rules, source_file=None):
    clean_df = df
    reject_df = None
    
    if not rules:
        return clean_df, reject_df
    
    # Initialize reject condition
    reject_condition = lit(False)
    
    # Apply rules based on common DQ patterns
    for rule_name, rule_config in rules.items():
        if isinstance(rule_config, dict):
            rule_type = rule_config.get('type', '').lower()
            column = rule_config.get('column', '')
            
            if rule_type == 'not_null' and column in df.columns:
                reject_condition = reject_condition | col(column).isNull()
                
            elif rule_type == 'unique' and column in df.columns:
                window_spec = Window.partitionBy(column)
                df_with_count = df.withColumn("_count", count("*").over(window_spec))
                reject_condition = reject_condition | (col("_count") > 1)
                clean_df = clean_df.drop("_count") if "_count" in clean_df.columns else clean_df
                
            elif rule_type == 'range' and column in df.columns:
                min_val = rule_config.get('min')
                max_val = rule_config.get('max')
                if min_val is not None:
                    reject_condition = reject_condition | (col(column) < min_val)
                if max_val is not None:
                    reject_condition = reject_condition | (col(column) > max_val)
                    
            elif rule_type == 'regex' and column in df.columns:
                pattern = rule_config.get('pattern', '')
                if pattern:
                    reject_condition = reject_condition | ~col(column).rlike(pattern)
                    
            elif rule_type == 'length' and column in df.columns:
                min_length = rule_config.get('min_length')
                max_length = rule_config.get('max_length')
                if min_length is not None:
                    reject_condition = reject_condition | (length(col(column)) < min_length)
                if max_length is not None:
                    reject_condition = reject_condition | (length(col(column)) > max_length)
    
    # Split into clean and reject datasets
    if reject_condition != lit(False):
        reject_df = df.filter(reject_condition).withColumn("reject_reason", lit("DQ_RULE_VIOLATION"))
        clean_df = df.filter(~reject_condition)
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_df):
    if mapping_df.empty or df is None:
        return df
    
    try:
        # Assume mapping has columns: source_column, target_column, transformation
        for _, row in mapping_df.iterrows():
            source_col = row.get('source_column', '')
            target_col = row.get('target_column', '')
            transformation = row.get('transformation', '')
            
            if source_col in df.columns:
                if transformation and transformation.lower() != 'none':
                    # Apply basic transformations
                    if transformation.lower() == 'upper':
                        df = df.withColumn(target_col, upper(col(source_col)))
                    elif transformation.lower() == 'lower':
                        df = df.withColumn(target_col, lower(col(source_col)))
                    elif transformation.lower() == 'trim':
                        df = df.withColumn(target_col, trim(col(source_col)))
                    else:
                        df = df.withColumn(target_col, col(source_col))
                else:
                    # Direct mapping
                    if target_col != source_col:
                        df = df.withColumn(target_col, col(source_col))
        
        return df
    except Exception as e:
        print(f"Error applying business mapping: {str(e)}")
        return df

def create_glue_table(database_name, table_name, s3_location, schema, aws_region, partition_keys=None):
    glue_client = boto3.client('glue', region_name=aws_region)
    
    # Convert Spark schema to Glue format
    columns = []
    for field in schema.fields:
        if partition_keys is None or field.name not in partition_keys:
            glue_type = spark_to_glue_type(field.dataType)
            columns.append({
                'Name': field.name,
                'Type': glue_type
            })
    
    partitions = []
    if partition_keys:
        for partition_key in partition_keys:
            for field in schema.fields:
                if field.name == partition_key:
                    partitions.append({
                        'Name': field.name,
                        'Type': spark_to_glue_type(field.dataType)
                    })
                    break
    
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 