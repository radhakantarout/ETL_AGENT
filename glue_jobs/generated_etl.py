```python
#!/usr/bin/env python3

import sys
import argparse
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from io import BytesIO

def create_spark_session(app_name="ETL_Job"):
    """Create Spark session with AWS configurations"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_s3_json(s3_client, bucket, key):
    """Read JSON file from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"Error reading {key} from S3: {str(e)}")
        return None

def read_s3_excel(s3_client, bucket, key):
    """Read Excel file from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        excel_data = BytesIO(response['Body'].read())
        return pd.read_excel(excel_data)
    except Exception as e:
        print(f"Error reading {key} from S3: {str(e)}")
        return None

def apply_data_quality_rules(spark, df, dq_rules, table_name):
    """Apply data quality rules and separate clean/reject records"""
    if not dq_rules or table_name not in dq_rules:
        print(f"No DQ rules found for table: {table_name}")
        return df, spark.createDataFrame([], df.schema)
    
    rules = dq_rules[table_name]
    clean_df = df
    reject_conditions = []
    reject_reasons = []
    
    # Apply each rule
    for rule in rules:
        rule_name = rule.get('rule_name', 'unknown')
        rule_type = rule.get('rule_type', '')
        column = rule.get('column', '')
        condition = rule.get('condition', '')
        
        if rule_type == 'not_null':
            reject_condition = col(column).isNull()
            reject_reason = lit(f"NULL_CHECK_FAILED_{rule_name}")
        elif rule_type == 'range':
            min_val = rule.get('min_value', 0)
            max_val = rule.get('max_value', 999999)
            reject_condition = ~((col(column) >= min_val) & (col(column) <= max_val))
            reject_reason = lit(f"RANGE_CHECK_FAILED_{rule_name}")
        elif rule_type == 'length':
            max_length = rule.get('max_length', 255)
            reject_condition = length(col(column)) > max_length
            reject_reason = lit(f"LENGTH_CHECK_FAILED_{rule_name}")
        elif rule_type == 'regex':
            pattern = rule.get('pattern', '')
            reject_condition = ~col(column).rlike(pattern)
            reject_reason = lit(f"REGEX_CHECK_FAILED_{rule_name}")
        elif rule_type == 'custom':
            reject_condition = expr(f"NOT ({condition})")
            reject_reason = lit(f"CUSTOM_CHECK_FAILED_{rule_name}")
        else:
            continue
            
        reject_conditions.append(reject_condition)
        reject_reasons.append(reject_reason)
    
    if reject_conditions:
        # Combine all reject conditions
        combined_reject_condition = reject_conditions[0]
        combined_reject_reason = reject_reasons[0]
        
        for i in range(1, len(reject_conditions)):
            combined_reject_condition = combined_reject_condition | reject_conditions[i]
            combined_reject_reason = when(reject_conditions[i], reject_reasons[i]).otherwise(combined_reject_reason)
        
        # Add reject reason column
        df_with_reason = df.withColumn("reject_reason", 
                                     when(combined_reject_condition, combined_reject_reason))
        
        # Split clean and reject records
        clean_df = df_with_reason.filter(~combined_reject_condition).drop("reject_reason")
        reject_df = df_with_reason.filter(combined_reject_condition)
    else:
        reject_df = spark.createDataFrame([], df.schema)
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_df, table_name):
    """Apply business mappings from Excel file"""
    if mapping_df is None:
        return df
    
    # Filter mappings for current table
    table_mappings = mapping_df[mapping_df['source_table'].str.lower() == table_name.lower()]
    
    if table_mappings.empty:
        return df
    
    mapped_df = df
    
    for _, mapping in table_mappings.iterrows():
        source_col = mapping.get('source_column', '')
        target_col = mapping.get('target_column', '')
        transformation = mapping.get('transformation', '')
        
        if source_col and target_col:
            if transformation and transformation.strip():
                # Apply transformation
                mapped_df = mapped_df.withColumn(target_col, expr(transformation.replace('{col}', source_col)))
            elif source_col != target_col:
                # Simple rename
                mapped_df = mapped_df.withColumnRenamed(source_col, target_col)
    
    return mapped_df

def create_glue_table(glue_client, database_name, table_name, s3_location, columns, aws_region):
    """Create or update Glue external table"""
    try:
        # Convert Spark columns to Glue format
        glue_columns = []
        for col_name, col_type in columns:
            glue_type = "string"  # Default
            if "int" in col_type.lower():
                glue_type = "bigint"
            elif "double" in col_type.lower() or "float" in col_type.lower():
                glue_type = "double"
            elif "boolean" in col_type.lower():
                glue_type = "boolean"
            elif "timestamp" in col_type.lower():
                glue_type = "timestamp"
            elif "date" in col_type.lower():
                glue_type = "date"
            
            glue_columns.append({
                'Name': col_name,
                'Type': glue_type
            })
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': glue_columns,
                'Location': s3_location,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                },
                'Parameters': {
                    'classification': 'parquet'
                }
            },
            'PartitionKeys': [
                {
                    'Name': 'ingest_date',
                    'Type': 'string'
                }
            ],
            'Parameters': {
                'classification': 'parquet',
                