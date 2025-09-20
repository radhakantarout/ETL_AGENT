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
from botocore.exceptions import ClientError

def create_spark_session(app_name="ETL_Pipeline"):
    """Create Spark session with necessary configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def read_dq_rules_from_s3(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        rules_df = spark.read.option("multiline", "true").text(rules_s3_path)
        rules_content = rules_df.collect()[0][0]
        return json.loads(rules_content)
    except Exception as e:
        print(f"Error reading DQ rules from {rules_s3_path}: {str(e)}")
        return {}

def read_business_mapping_from_s3(s3_bucket, aws_region):
    """Read business mapping from Excel file in S3"""
    try:
        s3_client = boto3.client('s3', region_name=aws_region)
        obj = s3_client.get_object(Bucket=s3_bucket, Key='raw/business_mapping.xlsx')
        mapping_df = pd.read_excel(obj['Body'].read())
        return mapping_df.to_dict('records')
    except Exception as e:
        print(f"Error reading business mapping: {str(e)}")
        return []

def discover_csv_files(spark, s3_path):
    """Discover all CSV files in S3 path"""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
        
        file_list = []
        if fs.exists(path):
            files = fs.listStatus(path)
            for file in files:
                if file.getPath().getName().endswith('.csv'):
                    file_list.append(str(file.getPath()))
        
        return file_list
    except Exception as e:
        print(f"Error discovering CSV files: {str(e)}")
        return []

def apply_data_quality_rules(df, dq_rules):
    """Apply data quality rules and separate clean and reject records"""
    if not dq_rules:
        return df, df.limit(0)
    
    # Add a flag column to track quality issues
    df_with_flag = df.withColumn("dq_flag", lit(True))
    df_with_flag = df_with_flag.withColumn("dq_issues", lit(""))
    
    # Apply rules based on the structure of dq_rules
    for table_name, rules in dq_rules.items():
        if 'rules' in rules:
            for rule in rules['rules']:
                rule_type = rule.get('type', '')
                column = rule.get('column', '')
                
                if rule_type == 'not_null' and column:
                    df_with_flag = df_with_flag.withColumn(
                        "dq_flag",
                        when(col(column).isNull(), False).otherwise(col("dq_flag"))
                    )
                    df_with_flag = df_with_flag.withColumn(
                        "dq_issues",
                        when(col(column).isNull(), 
                             concat(col("dq_issues"), lit(f"{column}_null;"))).otherwise(col("dq_issues"))
                    )
                
                elif rule_type == 'regex' and column:
                    pattern = rule.get('pattern', '')
                    if pattern:
                        df_with_flag = df_with_flag.withColumn(
                            "dq_flag",
                            when(~col(column).rlike(pattern), False).otherwise(col("dq_flag"))
                        )
                        df_with_flag = df_with_flag.withColumn(
                            "dq_issues",
                            when(~col(column).rlike(pattern), 
                                 concat(col("dq_issues"), lit(f"{column}_regex;"))).otherwise(col("dq_issues"))
                        )
                
                elif rule_type == 'range' and column:
                    min_val = rule.get('min')
                    max_val = rule.get('max')
                    if min_val is not None and max_val is not None:
                        df_with_flag = df_with_flag.withColumn(
                            "dq_flag",
                            when((col(column) < min_val) | (col(column) > max_val), False).otherwise(col("dq_flag"))
                        )
                        df_with_flag = df_with_flag.withColumn(
                            "dq_issues",
                            when((col(column) < min_val) | (col(column) > max_val), 
                                 concat(col("dq_issues"), lit(f"{column}_range;"))).otherwise(col("dq_issues"))
                        )
    
    # Separate clean and reject records
    clean_df = df_with_flag.filter(col("dq_flag") == True).drop("dq_flag", "dq_issues")
    reject_df = df_with_flag.filter(col("dq_flag") == False).drop("dq_flag")
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_rules):
    """Apply business mapping transformations"""
    if not mapping_rules:
        return df
    
    mapped_df = df
    
    for rule in mapping_rules:
        source_col = rule.get('source_column')
        target_col = rule.get('target_column')
        transformation = rule.get('transformation')
        
        if source_col and target_col:
            if transformation == 'uppercase':
                mapped_df = mapped_df.withColumn(target_col, upper(col(source_col)))
            elif transformation == 'lowercase':
                mapped_df = mapped_df.withColumn(target_col, lower(col(source_col)))
            elif transformation == 'trim':
                mapped_df = mapped_df.withColumn(target_col, trim(col(source_col)))
            elif transformation == 'date_format':
                date_format = rule.get('format', 'yyyy-MM-dd')
                mapped_df = mapped_df.withColumn(target_col, date_format(col(source_col), date_format))
            else:
                mapped_df = mapped_df.withColumn(target_col, col(source_col))
    
    return mapped_df

def create_glue_table(table_name, s3_location, schema, aws_region, database_name="default"):
    """Create or update AWS Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Convert Spark schema to Glue schema
        columns = []
        for field in schema.fields:
            if field.name != 'ingest_date':  # Skip partition column
                glue_type = 'string'  # Default type
                if isinstance(field.dataType, IntegerType):
                    glue_type = 'int'
                elif