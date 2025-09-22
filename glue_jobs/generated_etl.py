```python
import sys
import argparse
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from io import StringIO

def parse_args():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to dq_rules.json')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    return parser.parse_args()

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_Job") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_s3_file(bucket, key, aws_region):
    s3_client = boto3.client('s3', region_name=aws_region)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def get_dq_rules(rules_s3_path, aws_region):
    bucket = rules_s3_path.replace('s3://', '').split('/')[0]
    key = '/'.join(rules_s3_path.replace('s3://', '').split('/')[1:])
    rules_content = read_s3_file(bucket, key, aws_region)
    return json.loads(rules_content)

def get_business_mapping(bucket, aws_region):
    try:
        mapping_content = read_s3_file(bucket, 'business_mapping.xlsx', aws_region)
        df = pd.read_excel(StringIO(mapping_content))
        return df.to_dict('records')
    except:
        return []

def apply_dq_rules(df, rules, table_name):
    clean_df = df
    reject_df = None
    
    if table_name in rules:
        table_rules = rules[table_name]
        
        # Apply null checks
        if 'not_null_columns' in table_rules:
            for col in table_rules['not_null_columns']:
                if col in df.columns:
                    condition = df[col].isNull() | (df[col] == '')
                    if reject_df is None:
                        reject_df = df.filter(condition).withColumn('reject_reason', lit(f'null_check_{col}'))
                    else:
                        reject_df = reject_df.union(
                            df.filter(condition).withColumn('reject_reason', lit(f'null_check_{col}'))
                        )
                    clean_df = clean_df.filter(~condition)
        
        # Apply data type validations
        if 'data_types' in table_rules:
            for col, expected_type in table_rules['data_types'].items():
                if col in df.columns:
                    try:
                        if expected_type.lower() == 'integer':
                            clean_df = clean_df.withColumn(col, col(col).cast(IntegerType()))
                        elif expected_type.lower() == 'double':
                            clean_df = clean_df.withColumn(col, col(col).cast(DoubleType()))
                        elif expected_type.lower() == 'date':
                            clean_df = clean_df.withColumn(col, to_date(col(col)))
                    except:
                        pass
        
        # Apply custom validations
        if 'custom_validations' in table_rules:
            for validation in table_rules['custom_validations']:
                try:
                    condition = expr(validation['condition'])
                    invalid_records = clean_df.filter(~condition)
                    if invalid_records.count() > 0:
                        if reject_df is None:
                            reject_df = invalid_records.withColumn('reject_reason', lit(validation.get('name', 'custom_validation')))
                        else:
                            reject_df = reject_df.union(
                                invalid_records.withColumn('reject_reason', lit(validation.get('name', 'custom_validation')))
                            )
                        clean_df = clean_df.filter(condition)
                except:
                    pass
    
    return clean_df, reject_df

def create_glue_table(database, table_name, s3_location, columns, aws_region, is_partitioned=True):
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
    
    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'TableType': 'EXTERNAL_TABLE'
    }
    
    if is_partitioned:
        table_input['PartitionKeys'] = [{'Name': 'ingest_date', 'Type': 'string'}]
    
    try:
        glue_client.create_table(DatabaseName=database, TableInput=table_input)
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(DatabaseName=database, TableInput=table_input)

def get_csv_files(spark, bucket):
    s3_path = f"s3a://{bucket}/raw/*.csv"
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
        return {"combined": df}
    except:
        # Try reading individual files
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix='raw/')
        
        dataframes = {}
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                file_name = obj['Key'].split('/')[-1].replace('.csv', '')
                file_path = f"s3a://{bucket}/{obj['Key']}"
                try:
                    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                    dataframes[file_name] = df
                except:
                    continue
        
        return dataframes

def apply_business_mapping(df, mappings):
    if not mappings:
        return df
    
    mapped_df = df
    for mapping in mappings:
        try:
            source_col = mapping.get('source_column')
            target_col = mapping.get('target_column')
            transformation = mapping.get('transformation')
            
            if source_col and target_col:
                if transformation:
                    mapped_df = mapped_df.withColumn(target_col, expr(transformation.replace('{col}', source_col)))
                else:
                    mapped_df = mapped_df.withColumnRenamed(source_col, target_col)
        except:
            continue
    
    return mapped_df

def main():
    args = parse_args()
    spark = create_spark_session()
    
    try:
        # Read DQ rules
        dq_rules = get_dq_rules(