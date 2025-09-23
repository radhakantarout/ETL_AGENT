from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import json
import argparse
import sys
import pandas as pd
from io import StringIO
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="ETL-Pipeline"):
    """Create Spark session with necessary configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def read_s3_json(s3_path, aws_region):
    """Read JSON file from S3"""
    s3 = boto3.client('s3', region_name=aws_region)
    bucket = s3_path.replace('s3://', '').split('/')[0]
    key = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error reading {s3_path}: {str(e)}")
        raise

def read_s3_excel(s3_path, aws_region):
    """Read Excel file from S3"""
    s3 = boto3.client('s3', region_name=aws_region)
    bucket = s3_path.replace('s3://', '').split('/')[0]
    key = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        return pd.read_excel(content)
    except Exception as e:
        logger.error(f"Error reading {s3_path}: {str(e)}")
        raise

def list_s3_csv_files(s3_path, aws_region):
    """List all CSV files from S3 path"""
    s3 = boto3.client('s3', region_name=aws_region)
    bucket = s3_path.replace('s3://', '').split('/')[0]
    prefix = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    csv_files = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.csv'):
                        csv_files.append(f"s3://{bucket}/{obj['Key']}")
        
        return csv_files
    except Exception as e:
        logger.error(f"Error listing files from {s3_path}: {str(e)}")
        raise

def apply_data_quality_rules(df, rules):
    """Apply data quality rules and separate clean/reject records"""
    clean_df = df
    reject_conditions = []
    
    for rule in rules.get('rules', []):
        rule_name = rule.get('rule_name', 'unknown')
        rule_type = rule.get('type', '')
        column = rule.get('column', '')
        
        if rule_type == 'not_null':
            condition = col(column).isNull()
            reject_conditions.append(condition)
            clean_df = clean_df.filter(col(column).isNotNull())
            
        elif rule_type == 'unique':
            # Mark duplicates as rejects
            window_spec = Window.partitionBy(column)
            clean_df = clean_df.withColumn("row_count", count("*").over(window_spec))
            reject_condition = col("row_count") > 1
            reject_conditions.append(reject_condition)
            clean_df = clean_df.filter(col("row_count") == 1).drop("row_count")
            
        elif rule_type == 'data_type':
            expected_type = rule.get('expected_type', 'string')
            if expected_type == 'integer':
                clean_df = clean_df.filter(col(column).rlike("^[0-9]+$"))
            elif expected_type == 'decimal':
                clean_df = clean_df.filter(col(column).rlike("^[0-9]+\\.?[0-9]*$"))
            elif expected_type == 'date':
                clean_df = clean_df.filter(col(column).rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"))
                
        elif rule_type == 'range':
            min_val = rule.get('min_value')
            max_val = rule.get('max_value')
            if min_val is not None:
                clean_df = clean_df.filter(col(column) >= min_val)
            if max_val is not None:
                clean_df = clean_df.filter(col(column) <= max_val)
                
        elif rule_type == 'regex':
            pattern = rule.get('pattern', '')
            clean_df = clean_df.filter(col(column).rlike(pattern))
    
    # Create reject dataframe
    if reject_conditions:
        reject_condition = reduce(lambda x, y: x | y, reject_conditions)
        reject_df = df.filter(reject_condition)
    else:
        reject_df = spark.createDataFrame([], df.schema)
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_df):
    """Apply business mapping transformations"""
    transformed_df = df
    
    for _, row in mapping_df.iterrows():
        source_col = row.get('source_column', '')
        target_col = row.get('target_column', source_col)
        transformation = row.get('transformation', '')
        
        if source_col in df.columns:
            if transformation == 'upper':
                transformed_df = transformed_df.withColumn(target_col, upper(col(source_col)))
            elif transformation == 'lower':
                transformed_df = transformed_df.withColumn(target_col, lower(col(source_col)))
            elif transformation == 'trim':
                transformed_df = transformed_df.withColumn(target_col, trim(col(source_col)))
            elif transformation.startswith('cast_'):
                cast_type = transformation.replace('cast_', '')
                if cast_type == 'int':
                    transformed_df = transformed_df.withColumn(target_col, col(source_col).cast(IntegerType()))
                elif cast_type == 'double':
                    transformed_df = transformed_df.withColumn(target_col, col(source_col).cast(DoubleType()))
                elif cast_type == 'date':
                    transformed_df = transformed_df.withColumn(target_col, to_date(col(source_col)))
            elif target_col != source_col:
                transformed_df = transformed_df.withColumn(target_col, col(source_col))
    
    return transformed_df

def create_glue_table(database, table_name, s3_location, columns, aws_region, partition_keys=None):
    """Create or update Glue external table"""
    glue = boto3.client('glue', region_name=aws_region)
    
    storage_descriptor = {
        'Columns': [{'Name': col[0], 'Type': col[1]} for col in columns],
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
    
    if partition_keys:
        table_input['PartitionKeys'] = [{'Name': key, 'Type': 'string'} for key in partition_keys]
    
    try:
        # Try to update existing table
        glue.update_table(DatabaseName=database, TableInput=table_input)
        logger.info(f"Updated Glue table {database}.{table_name}")
    except glue.exceptions.EntityNotFoundException:
        # Create new table if it doesn't exist
        glue.create_table(DatabaseName=database, TableInput=table_input)
        logger.info(f"Created Glue table {database}.{table_name}")
    except Exception as e:
        logger.error(f"Error creating/updating Glue table {database}.{table_name}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Pipeline')
    parser.add_argument('--rules_s3', required=True, help='S3 path to dq_rules.json')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Define paths
        raw_path = f"s3://{args.s3_bucket}/raw/"
        access_path = f"s3://{args.s3_bucket}/access/"
        mart_path = f"s3://{args.s3_bucket}/mart/"
        mapping_path = f"s3://{args.s3_bucket}/raw/business_mapping.xlsx"
        
        # Read DQ rules
        logger.info("Reading data quality rules...")
        dq_rules = read_s3_json(args.rules_s3, args.aws_region)
        
        # Read business mapping
        logger.info("Reading business mapping...")
        mapping_df = read_s3_excel(mapping_path, args.aws_region)
        
        # List CSV files
        logger.info("Listing CSV files...")
        csv_files = list_s3_csv_files(raw_path, args.aws_region)
        
        if not csv_files:
            logger.warning("No CSV files found in source path")
            return
        
        # Process each category (assuming filename contains category)
        categories = {}
        for file_path in csv_files:
            filename = file_path.split('/')[-1]
            category = filename.split('_')[0] if '_' in filename else 'default'
            if category not in categories:
                categories[category] = []
            categories[category].append(file_path)
        
        for category, files in categories.items():
            logger.info(f"Processing category: {category}")
            
            # Read all CSV files for this category
            dfs = []
            for file_path in files:
                logger.info(f"Reading file: {file_path}")
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                dfs.append(df)
            
            # Union all dataframes
            if len(dfs) == 1:
                combined_df = dfs[0]
            else:
                combined_df = dfs[0]
                for df in dfs[1:]:
                    combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            
            # Add load_date column
            combined_df = combined_df.withColumn("load_date", lit(args.ingest_date))
            
            # Apply data quality rules
            logger.info("Applying data quality rules...")
            clean_df, reject_df = apply_data_quality_rules(combined_df, dq_rules)
            
            # Write clean data to access layer
            clean_output_path = f"{access_path}{category}/ingest_date={args.ingest_date}/"
            logger.info(f"Writing clean data to: {clean_output_path}")
            clean_df.coalesce(1).write.mode("overwrite").parquet(clean_output_path)
            
            # Write reject data
            reject_output_path = f"{access_path}{category}/rejects/ingest_date={args.ingest_date}/"
            logger.info(f"Writing reject data to: {reject_output_path}")
            reject_df.coalesce(1).write.mode("overwrite").parquet(reject_output_path)
            
            # Apply business mapping for mart layer
            logger.info("Applying business mapping...")
            mart_df = apply_business_mapping(clean_df, mapping_df)
            
            # Write to mart layer
            mart_output_path = f"{mart_path}{category}/ingest_date={args.ingest_date}/"
            logger.info(f"Writing mart data to: {mart_output_path}")
            mart_df.coalesce(1).write.mode("overwrite").parquet(mart_output_path)
            
            # Create Glue tables
            logger.info("Creating/updating Glue tables...")
            
            # Get column information
            clean_columns = [(field.name, field.dataType.simpleString()) for field in clean_df.schema.fields]
            mart_columns = [(field.name, field.dataType.simpleString()) for field in mart_df.schema.fields]
            
            # Create access layer table
            create_glue_table(
                database='default',
                table_name=f'access_{category}',
                s3_location=f"{access_path}{category}/",
                columns=clean_columns,
                aws_region=args.aws_region,
                partition_keys=['ingest_date']
            )
            
            # Create mart layer table
            create_glue_table(
                database='default',
                table_name=f'mart_{category}',
                s3_location=f"{mart_path}{category}/",
                columns=mart_columns,
                aws_region=args.aws_region,
                partition_keys=['ingest_date']
            )
            
            logger.info(f"Successfully processed category: {category}")
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()