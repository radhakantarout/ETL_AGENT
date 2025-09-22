import sys
import argparse
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="ETL-Job"):
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        # Read JSON file from S3
        df = spark.read.text(rules_s3_path)
        json_content = df.collect()[0][0]
        return json.loads(json_content)
    except Exception as e:
        logger.error(f"Error reading DQ rules from {rules_s3_path}: {str(e)}")
        return {}

def read_business_mapping(spark, s3_bucket, aws_region):
    """Read business mapping from Excel file on S3"""
    try:
        # Use boto3 to read Excel file
        s3_client = boto3.client('s3', region_name=aws_region)
        excel_path = f"s3://{s3_bucket}/business_mapping.xlsx"
        
        # Download Excel file temporarily
        s3_client.download_file(s3_bucket, 'business_mapping.xlsx', '/tmp/business_mapping.xlsx')
        
        # Read Excel file
        mapping_df = pd.read_excel('/tmp/business_mapping.xlsx')
        return mapping_df.to_dict('records')
    except Exception as e:
        logger.warning(f"Error reading business mapping: {str(e)}")
        return []

def apply_dq_rules(df, rules):
    """Apply data quality rules and separate clean/reject records"""
    if not rules:
        return df, df.filter("1=0")  # Return all as clean, empty rejects
    
    clean_df = df
    reject_conditions = []
    
    for rule in rules.get('rules', []):
        rule_type = rule.get('type')
        column = rule.get('column')
        
        if rule_type == 'not_null' and column:
            reject_condition = col(column).isNull()
            reject_conditions.append(reject_condition)
            clean_df = clean_df.filter(col(column).isNotNull())
            
        elif rule_type == 'numeric' and column:
            # Check if column can be cast to numeric
            reject_condition = col(column).cast("double").isNull() & col(column).isNotNull()
            reject_conditions.append(reject_condition)
            clean_df = clean_df.filter(
                col(column).cast("double").isNotNull() | col(column).isNull()
            )
            
        elif rule_type == 'length' and column:
            min_len = rule.get('min_length', 0)
            max_len = rule.get('max_length', 1000)
            reject_condition = (length(col(column)) < min_len) | (length(col(column)) > max_len)
            reject_conditions.append(reject_condition)
            clean_df = clean_df.filter(
                (length(col(column)) >= min_len) & (length(col(column)) <= max_len)
            )
    
    # Create reject dataframe
    if reject_conditions:
        combined_reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            combined_reject_condition = combined_reject_condition | condition
        reject_df = df.filter(combined_reject_condition)
    else:
        reject_df = df.filter("1=0")  # Empty dataframe
    
    return clean_df, reject_df

def apply_business_mapping(df, mappings):
    """Apply business mappings to create mart data"""
    if not mappings:
        return df
    
    mart_df = df
    
    for mapping in mappings:
        source_col = mapping.get('source_column')
        target_col = mapping.get('target_column')
        transformation = mapping.get('transformation', 'direct')
        
        if source_col and target_col:
            if transformation == 'direct':
                mart_df = mart_df.withColumnRenamed(source_col, target_col)
            elif transformation == 'upper':
                mart_df = mart_df.withColumn(target_col, upper(col(source_col)))
            elif transformation == 'lower':
                mart_df = mart_df.withColumn(target_col, lower(col(source_col)))
            elif transformation == 'trim':
                mart_df = mart_df.withColumn(target_col, trim(col(source_col)))
    
    return mart_df

def create_glue_table(database_name, table_name, s3_path, columns, aws_region, partition_keys=None):
    """Create or update Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Create database if not exists
        try:
            glue_client.create_database(DatabaseInput={'Name': database_name})
        except ClientError as e:
            if e.response['Error']['Code'] != 'AlreadyExistsException':
                raise
        
        # Prepare column definitions
        column_list = []
        for col_name, col_type in columns:
            column_list.append({
                'Name': col_name,
                'Type': col_type
            })
        
        # Prepare partition keys
        partition_key_list = []
        if partition_keys:
            for key_name, key_type in partition_keys:
                partition_key_list.append({
                    'Name': key_name,
                    'Type': key_type
                })
        
        # Table input
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
            'PartitionKeys': partition_key_list
        }
        
        # Try to create or update table
        try:
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            logger.info(f"Created Glue table: {database_name}.{table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
                logger.info(f"Updated Glue table: {database_name}.{table_name}")
            else:
                raise
                
    except Exception as e:
        logger.error(f"Error creating/updating Glue table {database_name}.{table_name}: {str(e)}")

def get_spark_columns_info(df):
    """Extract column information from Spark DataFrame for Glue table creation"""
    columns = []
    for field in df.schema.fields:
        spark_type = str(field.dataType)
        if 'string' in spark_type.lower():
            glue_type = 'string'
        elif 'int' in spark_type.lower():
            glue_type = 'int'
        elif 'double' in spark_type.lower() or 'float' in spark_type.lower():
            glue_type = 'double'
        elif 'boolean' in spark_type.lower():
            glue_type = 'boolean'
        elif 'timestamp' in spark_type.lower():
            glue_type = 'timestamp'
        elif 'date' in spark_type.lower():
            glue_type = 'date'
        else:
            glue_type = 'string'
        
        columns.append((field.name, glue_type))
    
    return columns

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read DQ rules
        logger.info(f"Reading DQ rules from {args.rules_s3}")
        dq_rules = read_dq_rules(spark, args.rules_s3)
        
        # Read business mapping
        logger.info("Reading business mapping")
        business_mappings = read_business_mapping(spark, args.s3_bucket, args.aws_region)
        
        # Read CSV files from source
        source_path = f"s3://{args.s3_bucket}/raw/*.csv"
        logger.info(f"Reading CSV files from {source_path}")
        
        # Read all CSV files with header
        raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
        
        # Add load_date column
        raw_df = raw_df.withColumn("load_date", lit(args.ingest_date))
        
        logger.info(f"Read {raw_df.count()} records from source")
        
        # Apply DQ rules
        logger.info("Applying data quality rules")
        clean_df, reject_df = apply_dq_rules(raw_df, dq_rules)
        
        logger.info(f"Clean records: {clean_df.count()}, Reject records: {reject_df.count()}")
        
        # Determine category (assuming single category for now)
        category = "general"  # This could be derived from file names or metadata
        
        # Write clean data to access layer
        access_path = f"s3://{args.s3_bucket}/access/{category}"
        logger.info(f"Writing clean data to {access_path}")
        
        clean_df.write.mode("overwrite") \
            .partitionBy("load_date") \
            .parquet(access_path)
        
        # Write rejects
        reject_path = f"s3://{args.s3_bucket}/access/{category}/rejects"
        logger.info(f"Writing reject data to {reject_path}")
        
        if reject_df.count() > 0:
            reject_df.write.mode("overwrite") \
                .partitionBy("load_date") \
                .parquet(reject_path)
        
        # Apply business mapping and create mart data
        if business_mappings:
            logger.info("Applying business mappings")
            mart_df = apply_business_mapping(clean_df, business_mappings)
            
            # Write to mart layer
            mart_path = f"s3://{args.s3_bucket}/mart/{category}"
            logger.info(f"Writing mart data to {mart_path}")
            
            mart_df.write.mode("overwrite") \
                .partitionBy("load_date") \
                .parquet(mart_path)
        else:
            mart_df = clean_df
            mart_path = f"s3://{args.s3_bucket}/mart/{category}"
            mart_df.write.mode("overwrite") \
                .partitionBy("load_date") \
                .parquet(mart_path)
        
        # Create Glue external tables
        logger.info("Creating/updating Glue tables")
        
        # Access layer table
        access_columns = get_spark_columns_info(clean_df.drop("load_date"))
        create_glue_table(
            database_name="access_db",
            table_name=f"{category}_access",
            s3_path=access_path,
            columns=access_columns,
            aws_region=args.aws_region,
            partition_keys=[("load_date", "string")]
        )
        
        # Mart layer table
        mart_columns = get_spark_columns_info(mart_df.drop("load_date"))
        create_glue_table(
            database_name="mart_db",
            table_name=f"{category}_mart",
            s3_path=mart_path,
            columns=mart_columns,
            aws_region=args.aws_region,
            partition_keys=[("load_date", "string")]
        )
        
        # Reject table (if rejects exist)
        if reject_df.count() > 0:
            reject_columns = get_spark_columns_info(reject_df.drop("load_date"))
            create_glue_table(
                database_name="access_db",
                table_name=f"{category}_rejects",
                s3_path=reject_path,
                columns=reject_columns,
                aws_region=args.aws_region,
                partition_keys=[("load_date", "string")]
            )
        
        logger.info("ETL job completed successfully")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()