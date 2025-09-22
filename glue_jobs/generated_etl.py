```python
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
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    try:
        df = spark.read.text(rules_s3_path)
        rules_json = df.collect()[0][0]
        return json.loads(rules_json)
    except Exception as e:
        print(f"Error reading DQ rules: {e}")
        return {}

def read_business_mapping(spark, bucket, aws_region):
    try:
        mapping_path = f"s3a://{bucket}/raw/business_mapping.xlsx"
        s3_client = boto3.client('s3', region_name=aws_region)
        obj = s3_client.get_object(Bucket=bucket, Key='raw/business_mapping.xlsx')
        mapping_df = pd.read_excel(obj['Body'].read())
        return mapping_df.to_dict('records')
    except Exception as e:
        print(f"Error reading business mapping: {e}")
        return []

def apply_data_quality_rules(df, dq_rules):
    clean_df = df
    rejects_df = None
    
    for rule_name, rule_config in dq_rules.items():
        rule_type = rule_config.get('type', '')
        column = rule_config.get('column', '')
        
        if rule_type == 'not_null' and column:
            rejects = clean_df.filter(col(column).isNull())
            clean_df = clean_df.filter(col(column).isNotNull())
            
        elif rule_type == 'data_type' and column:
            target_type = rule_config.get('target_type', 'string')
            try:
                if target_type == 'integer':
                    clean_df = clean_df.withColumn(column, col(column).cast(IntegerType()))
                elif target_type == 'double':
                    clean_df = clean_df.withColumn(column, col(column).cast(DoubleType()))
                elif target_type == 'date':
                    clean_df = clean_df.withColumn(column, to_date(col(column)))
            except:
                rejects = clean_df.filter(col(column).isNull())
                clean_df = clean_df.filter(col(column).isNotNull())
                
        elif rule_type == 'range' and column:
            min_val = rule_config.get('min_value')
            max_val = rule_config.get('max_value')
            if min_val is not None and max_val is not None:
                rejects = clean_df.filter((col(column) < min_val) | (col(column) > max_val))
                clean_df = clean_df.filter((col(column) >= min_val) & (col(column) <= max_val))
                
        elif rule_type == 'regex' and column:
            pattern = rule_config.get('pattern', '')
            if pattern:
                rejects = clean_df.filter(~col(column).rlike(pattern))
                clean_df = clean_df.filter(col(column).rlike(pattern))
        
        if 'rejects' in locals() and rejects is not None:
            rejects = rejects.withColumn('rejection_reason', lit(rule_name))
            if rejects_df is None:
                rejects_df = rejects
            else:
                rejects_df = rejects_df.union(rejects)
    
    return clean_df, rejects_df

def apply_business_mapping(df, mapping_rules):
    mapped_df = df
    
    for rule in mapping_rules:
        source_col = rule.get('source_column', '')
        target_col = rule.get('target_column', '')
        transformation = rule.get('transformation', '')
        
        if source_col and target_col:
            if transformation == 'upper':
                mapped_df = mapped_df.withColumn(target_col, upper(col(source_col)))
            elif transformation == 'lower':
                mapped_df = mapped_df.withColumn(target_col, lower(col(source_col)))
            elif transformation == 'trim':
                mapped_df = mapped_df.withColumn(target_col, trim(col(source_col)))
            elif transformation == 'date_format':
                date_format = rule.get('format', 'yyyy-MM-dd')
                mapped_df = mapped_df.withColumn(target_col, date_format(col(source_col), date_format))
            else:
                mapped_df = mapped_df.withColumn(target_col, col(source_col))
    
    return mapped_df

def create_glue_table(database_name, table_name, s3_location, columns, aws_region, partition_keys=None):
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        storage_descriptor = {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            }
        }
        
        if partition_keys:
            storage_descriptor['PartitionKeys'] = partition_keys
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': storage_descriptor,
            'TableType': 'EXTERNAL_TABLE'
        }
        
        try:
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Created Glue table: {database_name}.{table_name}")
        except glue_client.exceptions.AlreadyExistsException:
            glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Updated Glue table: {database_name}.{table_name}")
            
    except Exception as e:
        print(f"Error creating/updating Glue table: {e}")

def get_table_schema(df):
    columns = []
    for field in df.schema.fields:
        glue_type = 'string'
        if field.dataType == IntegerType():
            glue_type = 'int'
        elif field.dataType == DoubleType():
            glue_type = 'double'
        elif field.dataType == DateType():
            glue_type = 'date'
        
        columns.append({
            'Name': field.name,
            'Type': glue_type
        })
    return columns

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    