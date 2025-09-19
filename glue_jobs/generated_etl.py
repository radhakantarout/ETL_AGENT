```python
import sys
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL-DataQuality-Job") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        s3_client = boto3.client('s3')
        bucket = rules_s3_path.replace('s3://', '').split('/')[0]
        key = '/'.join(rules_s3_path.replace('s3://', '').split('/')[1:])
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        rules_content = response['Body'].read().decode('utf-8')
        return json.loads(rules_content)
    except Exception as e:
        print(f"Error reading DQ rules: {str(e)}")
        return {}

def read_business_mapping(s3_bucket):
    """Read business mapping from Excel file in S3"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=s3_bucket, Key='business_mapping.xlsx')
        excel_data = response['Body'].read()
        
        df = pd.read_excel(BytesIO(excel_data))
        return df.to_dict('records')
    except Exception as e:
        print(f"Error reading business mapping: {str(e)}")
        return []

def apply_data_quality_rules(df, rules, table_name):
    """Apply data quality rules and separate clean vs reject records"""
    if table_name not in rules:
        print(f"No DQ rules found for table: {table_name}")
        return df, df.limit(0)
    
    table_rules = rules[table_name]
    clean_df = df
    reject_conditions = []
    
    # Apply null checks
    if 'null_checks' in table_rules:
        for column in table_rules['null_checks']:
            if column in df.columns:
                reject_conditions.append(col(column).isNull())
    
    # Apply range checks
    if 'range_checks' in table_rules:
        for rule in table_rules['range_checks']:
            column = rule.get('column')
            min_val = rule.get('min')
            max_val = rule.get('max')
            
            if column in df.columns:
                if min_val is not None:
                    reject_conditions.append(col(column) < min_val)
                if max_val is not None:
                    reject_conditions.append(col(column) > max_val)
    
    # Apply regex checks
    if 'regex_checks' in table_rules:
        for rule in table_rules['regex_checks']:
            column = rule.get('column')
            pattern = rule.get('pattern')
            
            if column in df.columns and pattern:
                reject_conditions.append(~col(column).rlike(pattern))
    
    # Apply uniqueness checks
    if 'unique_checks' in table_rules:
        for column in table_rules['unique_checks']:
            if column in df.columns:
                window_spec = Window.partitionBy(column)
                df_with_count = df.withColumn(f"{column}_count", count("*").over(window_spec))
                reject_conditions.append(col(f"{column}_count") > 1)
                clean_df = df_with_count.drop(f"{column}_count")
    
    # Combine all reject conditions
    if reject_conditions:
        combined_reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            combined_reject_condition = combined_reject_condition | condition
        
        # Add rejection reason
        reject_df = clean_df.filter(combined_reject_condition) \
            .withColumn("rejection_reason", lit("DQ_RULE_VIOLATION")) \
            .withColumn("processed_timestamp", current_timestamp())
        
        # Clean data
        clean_df = clean_df.filter(~combined_reject_condition)
    else:
        reject_df = clean_df.limit(0)
    
    return clean_df, reject_df

def create_glue_table(database, table_name, s3_path, columns, aws_region, partition_keys=None):
    """Create or update Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Prepare column definitions
        column_list = []
        for col_name, col_type in columns.items():
            glue_type = map_spark_to_glue_type(col_type)
            column_list.append({
                'Name': col_name,
                'Type': glue_type
            })
        
        # Prepare partition keys
        partition_key_list = []
        if partition_keys:
            for key, key_type in partition_keys.items():
                glue_type = map_spark_to_glue_type(key_type)
                partition_key_list.append({
                    'Name': key,
                    'Type': glue_type
                })
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': column_list,
                'Location': s3_path,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            },
            'PartitionKeys': partition_key_list,
            'TableType': 'EXTERNAL_TABLE'
        }
        
        try:
            # Try to update existing table
            glue_client.update_table(DatabaseName=database, TableInput=table_input)
            print(f"Updated Glue table: {database}.{table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                # Create new table
                glue_client.create_table(DatabaseName=database, TableInput=table_input)
                print(f"Created Glue table: {database}.{table_name}")
            else:
                raise e
                
    except Exception as e:
        print(f"Error creating/updating Glue table {table_name}: {str(e)}")

def map_spark_to_glue_type(spark_type):
    """Map Spark data types to Glue/Hive types"""
    type_mapping = {
        'string': 'string',
        'int': 'int',
        'integer': 'int',
        'long': 'bigint',
        'double': 'double',
        'float': 'float',
        'boolean': 'boolean',
        'date': 'date',
        'timestamp': 'timestamp',
        'decimal': 'decimal'
    }
    return type_mapping.get(spark_type.lower(), 'string')

def apply_business_mapping(df, mapping_rules):
    """Apply business transformations based on mapping rules"""
    if not mapping_rules:
        return df
    
    transformed_df = df
    
    