```python
import sys
import json
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd
from io import StringIO

def create_spark_session(app_name="ETL_Job"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_s3_file_content(s3_path, aws_region):
    s3_client = boto3.client('s3', region_name=aws_region)
    bucket = s3_path.replace('s3://', '').split('/')[0]
    key = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def load_dq_rules(rules_s3_path, aws_region):
    content = read_s3_file_content(rules_s3_path, aws_region)
    return json.loads(content)

def load_business_mapping(mapping_s3_path, aws_region):
    content = read_s3_file_content(mapping_s3_path, aws_region)
    df = pd.read_excel(StringIO(content))
    return df

def discover_csv_files(spark, s3_bucket, source_prefix="raw/"):
    s3_path = f"s3://{s3_bucket}/{source_prefix}*.csv"
    try:
        df_sample = spark.read.option("header", "true").csv(s3_path)
        return s3_path
    except:
        s3_path = f"s3://{s3_bucket}/{source_prefix}**/*.csv"
        return s3_path

def apply_data_quality_rules(df, dq_rules):
    clean_df = df
    reject_df = None
    
    for table_name, rules in dq_rules.items():
        if 'rules' not in rules:
            continue
            
        for rule in rules['rules']:
            rule_type = rule.get('type')
            column = rule.get('column')
            
            if rule_type == 'not_null' and column in df.columns:
                rejects = clean_df.filter(col(column).isNull())
                if reject_df is None:
                    reject_df = rejects.withColumn("reject_reason", lit(f"NULL value in {column}"))
                else:
                    reject_df = reject_df.union(rejects.withColumn("reject_reason", lit(f"NULL value in {column}")))
                clean_df = clean_df.filter(col(column).isNotNull())
                
            elif rule_type == 'unique' and column in df.columns:
                window_spec = Window.partitionBy(column)
                df_with_count = clean_df.withColumn("row_count", count("*").over(window_spec))
                rejects = df_with_count.filter(col("row_count") > 1).drop("row_count")
                if reject_df is None:
                    reject_df = rejects.withColumn("reject_reason", lit(f"Duplicate value in {column}"))
                else:
                    reject_df = reject_df.union(rejects.withColumn("reject_reason", lit(f"Duplicate value in {column}")))
                clean_df = df_with_count.filter(col("row_count") == 1).drop("row_count")
                
            elif rule_type == 'range' and column in df.columns:
                min_val = rule.get('min')
                max_val = rule.get('max')
                if min_val is not None and max_val is not None:
                    rejects = clean_df.filter((col(column) < min_val) | (col(column) > max_val))
                    if reject_df is None:
                        reject_df = rejects.withColumn("reject_reason", lit(f"Value out of range in {column}"))
                    else:
                        reject_df = reject_df.union(rejects.withColumn("reject_reason", lit(f"Value out of range in {column}")))
                    clean_df = clean_df.filter((col(column) >= min_val) & (col(column) <= max_val))
                    
            elif rule_type == 'regex' and column in df.columns:
                pattern = rule.get('pattern')
                if pattern:
                    rejects = clean_df.filter(~col(column).rlike(pattern))
                    if reject_df is None:
                        reject_df = rejects.withColumn("reject_reason", lit(f"Invalid format in {column}"))
                    else:
                        reject_df = reject_df.union(rejects.withColumn("reject_reason", lit(f"Invalid format in {column}")))
                    clean_df = clean_df.filter(col(column).rlike(pattern))
    
    if reject_df is None:
        reject_df = df.limit(0).withColumn("reject_reason", lit(""))
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_df):
    if mapping_df is None or mapping_df.empty:
        return df
    
    transformed_df = df
    
    for _, row in mapping_df.iterrows():
        source_col = row.get('source_column')
        target_col = row.get('target_column')
        transformation = row.get('transformation')
        
        if pd.notna(source_col) and source_col in df.columns:
            if pd.notna(target_col):
                if pd.notna(transformation):
                    if transformation.lower() == 'upper':
                        transformed_df = transformed_df.withColumn(target_col, upper(col(source_col)))
                    elif transformation.lower() == 'lower':
                        transformed_df = transformed_df.withColumn(target_col, lower(col(source_col)))
                    elif transformation.lower() == 'trim':
                        transformed_df = transformed_df.withColumn(target_col, trim(col(source_col)))
                    elif transformation.startswith('cast_'):
                        cast_type = transformation.replace('cast_', '')
                        if cast_type == 'int':
                            transformed_df = transformed_df.withColumn(target_col, col(source_col).cast(IntegerType()))
                        elif cast_type == 'double':
                            transformed_df = transformed_df.withColumn(target_col, col(source_col).cast(DoubleType()))
                        elif cast_type == 'string':
                            transformed_df = transformed_df.withColumn(target_col, col(source_col).cast(StringType()))
                    else:
                        transformed_df = transformed_df.withColumnRenamed(source_col, target_col)
                else:
                    transformed_df = transformed_df.withColumnRenamed(source_col, target_col)
    
    return transformed_df

def create_glue_table(database_name, table_name, s3_location, columns, aws_region, partition_keys=None):
    glue_client = boto3.client('glue', region_name=aws_region)
    
    storage_descriptor = {
        'Columns': columns,
        'Location': s3_location,
        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.Lazy