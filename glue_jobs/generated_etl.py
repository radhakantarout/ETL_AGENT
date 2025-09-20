```python
import sys
import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from io import StringIO
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLJob:
    def __init__(self, rules_s3, s3_bucket, aws_region, ingest_date):
        self.rules_s3 = rules_s3
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        self.ingest_date = ingest_date
        self.spark = None
        self.s3_client = None
        self.glue_client = None
        self.dq_rules = None
        self.business_mapping = None
        
    def initialize_clients(self):
        """Initialize Spark session and AWS clients"""
        self.spark = SparkSession.builder \
            .appName("ETL_Data_Pipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
            
        self.s3_client = boto3.client('s3', region_name=self.aws_region)
        self.glue_client = boto3.client('glue', region_name=self.aws_region)
        
    def load_dq_rules(self):
        """Load data quality rules from S3"""
        try:
            bucket, key = self.rules_s3.replace('s3://', '').split('/', 1)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            self.dq_rules = json.loads(response['Body'].read().decode('utf-8'))
            logger.info("Data quality rules loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load DQ rules: {str(e)}")
            raise
            
    def load_business_mapping(self):
        """Load business mapping from Excel file in S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key='business_mapping.xlsx')
            excel_data = response['Body'].read()
            self.business_mapping = pd.read_excel(excel_data)
            logger.info("Business mapping loaded successfully")
        except Exception as e:
            logger.warning(f"Business mapping file not found or failed to load: {str(e)}")
            self.business_mapping = None
            
    def discover_csv_files(self):
        """Discover all CSV files in the raw S3 bucket"""
        csv_files = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix='raw/')
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.csv'):
                            csv_files.append(f"s3://{self.s3_bucket}/{obj['Key']}")
                            
            logger.info(f"Found {len(csv_files)} CSV files")
            return csv_files
        except Exception as e:
            logger.error(f"Failed to discover CSV files: {str(e)}")
            raise
            
    def read_csv_files(self, csv_files):
        """Read all CSV files and combine them"""
        dataframes = []
        
        for file_path in csv_files:
            try:
                df = self.spark.read.option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(file_path)
                
                # Add source file column
                df = df.withColumn("source_file", lit(file_path))
                df = df.withColumn("load_date", lit(self.ingest_date))
                dataframes.append(df)
                logger.info(f"Successfully read: {file_path}")
                
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {str(e)}")
                continue
                
        if not dataframes:
            raise Exception("No CSV files could be read")
            
        # Union all dataframes
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            
        return combined_df
        
    def apply_dq_rules(self, df):
        """Apply data quality rules and separate clean and reject records"""
        if not self.dq_rules:
            logger.warning("No DQ rules found, returning original data as clean")
            return df, self.spark.createDataFrame([], df.schema)
            
        conditions = []
        
        for rule in self.dq_rules.get('rules', []):
            rule_type = rule.get('type')
            column = rule.get('column')
            
            if rule_type == 'not_null':
                conditions.append(col(column).isNotNull())
            elif rule_type == 'unique':
                # For unique constraints, we'll mark duplicates as rejects
                window_spec = Window.partitionBy(column)
                df = df.withColumn(f"{column}_count", count("*").over(window_spec))
                conditions.append(col(f"{column}_count") == 1)
            elif rule_type == 'min_length':
                min_len = rule.get('value', 0)
                conditions.append(length(col(column)) >= min_len)
            elif rule_type == 'max_length':
                max_len = rule.get('value', 1000)
                conditions.append(length(col(column)) <= max_len)
            elif rule_type == 'regex':
                pattern = rule.get('pattern', '.*')
                conditions.append(col(column).rlike(pattern))
            elif rule_type == 'range':
                min_val = rule.get('min_value')
                max_val = rule.get('max_value')
                if min_val is not None:
                    conditions.append(col(column) >= min_val)
                if max_val is not None:
                    conditions.append(col(column) <= max_val)
                    
        # Combine all conditions
        if conditions:
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition & condition
                
            clean_df = df.filter(combined_condition)
            reject_df = df.filter(~combined_condition)
        else:
            clean_df = df
            reject_df = self.spark.createDataFrame([], df.schema)
            
        # Add DQ validation timestamp
        clean_df = clean_df.withColumn("dq_validation_timestamp", current_timestamp())
        reject_df = reject_df.withColumn("dq_validation_timestamp", current_timestamp())
        reject_df = reject_df.withColumn("reject_reason", lit("Failed DQ validation"))
        
        return clean_df, reject_df
        
    def write_to_s3(self, df, path, table_name):
        """Write DataFrame to S3 in Parquet format"""
        try:
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(path)
            logger.info(f"Successfully wrote {df.count()} records to {path}")
        except Exception as e:
            logger.error(f"Failed to write to {path}: {str(e)