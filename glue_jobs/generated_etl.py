import argparse
import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="ETL_Job"):
    """Create Spark session with optimized configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def read_s3_json(s3_path):
    """Read JSON file from S3"""
    s3 = boto3.client('s3')
    bucket = s3_path.split('/')[2]
    key = '/'.join(s3_path.split('/')[3:])
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error reading S3 JSON file {s3_path}: {str(e)}")
        return {}

def read_excel_mapping(s3_path):
    """Read Excel mapping file from S3"""
    s3 = boto3.client('s3')
    bucket = s3_path.split('/')[2]
    key = '/'.join(s3_path.split('/')[3:])
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        df = pd.read_excel(content)
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error reading Excel mapping file {s3_path}: {str(e)}")
        return []

def apply_data_quality_rules(df, rules, category):
    """Apply data quality rules and return clean and reject dataframes"""
    if not rules or category not in rules:
        logger.warning(f"No DQ rules found for category: {category}")
        return df, df.filter(lit(False))
    
    category_rules = rules[category]
    reject_conditions = []
    
    # Apply null checks
    if 'null_checks' in category_rules:
        for col_name in category_rules['null_checks']:
            if col_name in df.columns:
                reject_conditions.append(col(col_name).isNull())
    
    # Apply data type checks
    if 'type_checks' in category_rules:
        for col_name, expected_type in category_rules['type_checks'].items():
            if col_name in df.columns:
                if expected_type.lower() == 'integer':
                    reject_conditions.append(~col(col_name).rlike(r'^\d+$'))
                elif expected_type.lower() == 'decimal':
                    reject_conditions.append(~col(col_name).rlike(r'^\d*\.?\d+$'))
                elif expected_type.lower() == 'date':
                    reject_conditions.append(to_date(col(col_name), 'yyyy-MM-dd').isNull())
    
    # Apply value range checks
    if 'range_checks' in category_rules:
        for col_name, ranges in category_rules['range_checks'].items():
            if col_name in df.columns:
                min_val = ranges.get('min')
                max_val = ranges.get('max')
                if min_val is not None:
                    reject_conditions.append(col(col_name).cast('double') < min_val)
                if max_val is not None:
                    reject_conditions.append(col(col_name).cast('double') > max_val)
    
    # Apply regex pattern checks
    if 'pattern_checks' in category_rules:
        for col_name, pattern in category_rules['pattern_checks'].items():
            if col_name in df.columns:
                reject_conditions.append(~col(col_name).rlike(pattern))
    
    # Apply uniqueness checks
    if 'unique_checks' in category_rules:
        for col_name in category_rules['unique_checks']:
            if col_name in df.columns:
                window_spec = Window.partitionBy(col_name)
                df_with_count = df.withColumn(f"{col_name}_count", count('*').over(window_spec))
                reject_conditions.append(col(f"{col_name}_count") > 1)
    
    # Combine all reject conditions
    if reject_conditions:
        combined_reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            combined_reject_condition = combined_reject_condition | condition
        
        # Add rejection reason
        df_with_reason = df.withColumn('rejection_reason', 
            when(combined_reject_condition, 'Data Quality Violation').otherwise(None))
        
        clean_df = df_with_reason.filter(col('rejection_reason').isNull()).drop('rejection_reason')
        reject_df = df_with_reason.filter(col('rejection_reason').isNotNull())
        
        return clean_df, reject_df
    else:
        return df, df.filter(lit(False))

def apply_business_mapping(df, mappings, category):
    """Apply business mappings to transform data"""
    if not mappings:
        logger.warning("No business mappings found")
        return df
    
    # Filter mappings for current category
    category_mappings = [m for m in mappings if m.get('category', '').lower() == category.lower()]
    
    if not category_mappings:
        logger.warning(f"No business mappings found for category: {category}")
        return df
    
    transformed_df = df
    
    for mapping in category_mappings:
        source_col = mapping.get('source_column')
        target_col = mapping.get('target_column')
        transformation = mapping.get('transformation', 'direct')
        
        if source_col and target_col and source_col in df.columns:
            if transformation == 'direct':
                transformed_df = transformed_df.withColumnRenamed(source_col, target_col)
            elif transformation == 'upper':
                transformed_df = transformed_df.withColumn(target_col, upper(col(source_col)))
            elif transformation == 'lower':
                transformed_df = transformed_df.withColumn(target_col, lower(col(source_col)))
            elif transformation == 'trim':
                transformed_df = transformed_df.withColumn(target_col, trim(col(source_col)))
            elif transformation.startswith('substring'):
                # Format: substring(start,length)
                params = transformation.split('(')[1].split(')')[0].split(',')
                start_pos = int(params[0])
                length = int(params[1]) if len(params) > 1 else None
                if length:
                    transformed_df = transformed_df.withColumn(target_col, 
                        substring(col(source_col), start_pos, length))
                else:
                    transformed_df = transformed_df.withColumn(target_col, 
                        substring(col(source_col), start_pos, 999))
    
    return transformed_df

def create_glue_table(database_name, table_name, s3_location, schema, aws_region):
    """Create or update Glue external table"""
    glue_client = boto3.client('glue', region_name=aws_region)
    
    # Convert Spark schema to Glue schema
    columns = []
    for field in schema.fields:
        if field.name != 'ingest_date':  # Exclude partition column
            glue_type = 'string'  # Default
            if isinstance(field.dataType, IntegerType):
                glue_type = 'int'
            elif isinstance(field.dataType, LongType):
                glue_type = 'bigint'
            elif isinstance(field.dataType, DoubleType):
                glue_type = 'double'
            elif isinstance(field.dataType, FloatType):
                glue_type = 'float'
            elif isinstance(field.dataType, BooleanType):
                glue_type = 'boolean'
            elif isinstance(field.dataType, DateType):
                glue_type = 'date'
            elif isinstance(field.dataType, TimestampType):
                glue_type = 'timestamp'
            
            columns.append({
                'Name': field.name,
                'Type': glue_type
            })
    
    partition_keys = [{'Name': 'ingest_date', 'Type': 'string'}]
    
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            }
        },
        'PartitionKeys': partition_keys,
        'TableType': 'EXTERNAL_TABLE'
    }
    
    try:
        # Try to create database first
        try:
            glue_client.create_database(DatabaseInput={'Name': database_name})
        except glue_client.exceptions.AlreadyExistsException:
            pass
        
        # Try to update table, if not exists then create
        try:
            glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            logger.info(f"Updated Glue table: {database_name}.{table_name}")
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            logger.info(f"Created Glue table: {database_name}.{table_name}")
            
    except Exception as e:
        logger.error(f"Error creating/updating Glue table {database_name}.{table_name}: {str(e)}")

def process_category(spark, category_path, rules_s3, s3_bucket, aws_region, ingest_date):
    """Process a single category of CSV files"""
    logger.info(f"Processing category: {category_path}")
    
    # Extract category name from path
    category = category_path.split('/')[-1] if category_path.endswith('/') else category_path.split('/')[-1]
    
    # Read CSV files with wildcard
    try:
        csv_path = f"{category_path}*.csv" if not category_path.endswith('.csv') else category_path
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        
        if df.count() == 0:
            logger.warning(f"No data found in {csv_path}")
            return
            
        logger.info(f"Read {df.count()} records from {csv_path}")
        
    except Exception as e:
        logger.error(f"Error reading CSV files from {csv_path}: {str(e)}")
        return
    
    # Read data quality rules
    dq_rules = read_s3_json(rules_s3)
    
    # Apply data quality rules
    clean_df, reject_df = apply_data_quality_rules(df, dq_rules, category)
    
    logger.info(f"Clean records: {clean_df.count()}, Rejected records: {reject_df.count()}")
    
    # Write clean data to access layer
    access_path = f"s3://{s3_bucket}/access/{category}/ingest_date={ingest_date}/"
    clean_df.write.mode("overwrite").parquet(access_path)
    logger.info(f"Written clean data to: {access_path}")
    
    # Write reject data
    if reject_df.count() > 0:
        reject_path = f"s3://{s3_bucket}/access/{category}/rejects/ingest_date={ingest_date}/"
        reject_df.write.mode("overwrite").parquet(reject_path)
        logger.info(f"Written reject data to: {reject_path}")
    
    # Create Glue table for access layer
    create_glue_table(
        database_name="access_db",
        table_name=f"{category}_clean",
        s3_location=f"s3://{s3_bucket}/access/{category}/",
        schema=clean_df.schema,
        aws_region=aws_region
    )
    
    # Read business mapping and create mart table
    try:
        mapping_path = f"s3://{s3_bucket}/business_mapping.xlsx"
        mappings = read_excel_mapping(mapping_path)
        
        # Apply business transformations
        mart_df = apply_business_mapping(clean_df, mappings, category)
        
        # Write to mart layer
        mart_path = f"s3://{s3_bucket}/mart/{category}/ingest_date={ingest_date}/"
        mart_df.write.mode("overwrite").parquet(mart_path)
        logger.info(f"Written mart data to: {mart_path}")
        
        # Create Glue table for mart layer
        create_glue_table(
            database_name="mart_db",
            table_name=f"{category}_mart",
            s3_location=f"s3://{s3_bucket}/mart/{category}/",
            schema=mart_df.schema,
            aws_region=aws_region
        )
        
    except Exception as e:
        logger.warning(f"Error processing business mapping for {category}: {str(e)}")
        logger.info("Continuing without mart layer creation")

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to dq_rules.json')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("ETL_Job")
    spark.sparkContext.setLogLevel("INFO")
    
    # Source path
    source_path = f"s3://{args.s3_bucket}/raw/"
    
    # Get list of categories (subdirectories) from S3
    s3 = boto3.client('s3')
    
    try:
        # List objects in raw/ directory to identify categories
        response = s3.list_objects_v2(Bucket=args.s3_bucket, Prefix='raw/', Delimiter='/')
        
        categories = []
        
        # Get subdirectories (categories)
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                category_path = prefix['Prefix']
                categories.append(category_path)
        
        # Also check for CSV files directly in raw/
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    categories.append('raw/')
                    break
        
        if not categories:
            logger.error("No categories or CSV files found in raw/ directory")
            sys.exit(1)
        
        logger.info(f"Found categories: {categories}")
        
        # Process each category
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                for category_path in categories:
                    full_path = f"s3://{args.s3_bucket}/{category_path}"
                    process_category(spark, full_path, args.rules_s3, args.s3_bucket, 
                                   args.aws_region, args.ingest_date)
                
                logger.info("ETL job completed successfully")
                break
                
            except Exception as e:
                retry_count += 1
                logger.error(f"ETL job failed (attempt {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count >= max_retries:
                    logger.error("Max retries reached. ETL job failed.")
                    sys.exit(1)
                else:
                    logger.info(f"Retrying... (attempt {retry_count + 1}/{max_retries})")
    
    except Exception as e:
        logger.error(f"Error listing S3 objects: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()