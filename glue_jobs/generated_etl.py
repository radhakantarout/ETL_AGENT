```python
#!/usr/bin/env python3

import argparse
import json
import sys
from datetime import datetime
from typing import Dict, List, Tuple

import boto3
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session(app_name: str) -> SparkSession:
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def read_s3_file_content(s3_path: str) -> str:
    """Read file content from S3"""
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def load_dq_rules(rules_s3_path: str) -> Dict:
    """Load data quality rules from S3"""
    try:
        content = read_s3_file_content(rules_s3_path)
        return json.loads(content)
    except Exception as e:
        print(f"Error loading DQ rules: {e}")
        return {}

def load_business_mapping(bucket: str) -> Dict:
    """Load business mapping from Excel file in S3"""
    try:
        s3_path = f"s3://{bucket}/business_mapping.xlsx"
        s3 = boto3.client('s3')
        bucket_name, key = s3_path.replace('s3://', '').split('/', 1)
        
        # Download file to temporary location
        temp_file = "/tmp/business_mapping.xlsx"
        s3.download_file(bucket_name, key, temp_file)
        
        # Read Excel file
        df = pd.read_excel(temp_file)
        
        # Convert to dictionary mapping
        mapping = {}
        for _, row in df.iterrows():
            if 'source_column' in df.columns and 'target_column' in df.columns:
                mapping[row['source_column']] = row['target_column']
        
        return mapping
    except Exception as e:
        print(f"Error loading business mapping: {e}")
        return {}

def discover_csv_files(spark: SparkSession, source_path: str) -> List[str]:
    """Discover all CSV files in S3 source path"""
    try:
        # Try to read with wildcard pattern
        csv_path = f"{source_path}/*.csv" if not source_path.endswith('.csv') else source_path
        
        # Test read to discover files
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df.limit(1).collect()  # Trigger execution to validate path
        
        return [csv_path]
    except Exception as e:
        print(f"Error discovering CSV files: {e}")
        return []

def apply_data_quality_rules(df: DataFrame, rules: Dict) -> Tuple[DataFrame, DataFrame]:
    """Apply data quality rules and separate clean vs rejected records"""
    if not rules:
        return df.withColumn("dq_status", lit("PASS")), spark.createDataFrame([], df.schema)
    
    clean_df = df
    reject_conditions = []
    
    for rule_name, rule_config in rules.items():
        if rule_config.get("type") == "not_null":
            columns = rule_config.get("columns", [])
            for col_name in columns:
                if col_name in df.columns:
                    reject_conditions.append(col(col_name).isNull())
        
        elif rule_config.get("type") == "range":
            col_name = rule_config.get("column")
            min_val = rule_config.get("min")
            max_val = rule_config.get("max")
            if col_name and col_name in df.columns:
                if min_val is not None:
                    reject_conditions.append(col(col_name) < min_val)
                if max_val is not None:
                    reject_conditions.append(col(col_name) > max_val)
        
        elif rule_config.get("type") == "regex":
            col_name = rule_config.get("column")
            pattern = rule_config.get("pattern")
            if col_name and pattern and col_name in df.columns:
                reject_conditions.append(~col(col_name).rlike(pattern))
    
    if reject_conditions:
        # Combine all reject conditions with OR
        reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            reject_condition = reject_condition | condition
        
        # Split into clean and reject dataframes
        rejects_df = clean_df.filter(reject_condition).withColumn("dq_status", lit("REJECT"))
        clean_df = clean_df.filter(~reject_condition).withColumn("dq_status", lit("PASS"))
    else:
        rejects_df = spark.createDataFrame([], clean_df.schema.add(StructField("dq_status", StringType())))
        clean_df = clean_df.withColumn("dq_status", lit("PASS"))
    
    return clean_df, rejects_df

def apply_business_mapping(df: DataFrame, mapping: Dict) -> DataFrame:
    """Apply business mapping transformations"""
    if not mapping:
        return df
    
    # Apply column renaming based on mapping
    for source_col, target_col in mapping.items():
        if source_col in df.columns:
            df = df.withColumnRenamed(source_col, target_col)
    
    return df

def write_partitioned_data(df: DataFrame, output_path: str, partition_col: str = "ingest_date"):
    """Write DataFrame to S3 with partitioning"""
    if df.count() > 0:
        df.coalesce(1).write \
            .mode("overwrite") \
            .partitionBy(partition_col) \
            .parquet(output_path)

def create_glue_table(database: str, table_name: str, s3_location: str, columns: List, 
                     partition_keys: List, region: str):
    """Create or update AWS Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=region)
        
        # Create database if it doesn't exist
        try:
            glue_client.create_database(
                DatabaseInput={
                    'Name': database,
                    'Description': 'ETL processed data'
                }
            )
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
            