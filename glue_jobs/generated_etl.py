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

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("ETL_Data_Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def read_json_from_s3(s3_path, aws_region):
    """Read JSON file from S3"""
    s3_client = boto3.client('s3', region_name=aws_region)
    bucket = s3_path.replace('s3://', '').split('/')[0]
    key = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error reading JSON from S3: {e}")
        raise

def read_excel_from_s3(s3_path, aws_region):
    """Read Excel file from S3"""
    s3_client = boto3.client('s3', region_name=aws_region)
    bucket = s3_path.replace('s3://', '').split('/')[0]
    key = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        return pd.read_excel(content)
    except Exception as e:
        logger.error(f"Error reading Excel from S3: {e}")
        raise

def discover_csv_files(spark, source_path):
    """Discover CSV files from S3 source path"""
    try:
        # Read all CSV files with wildcard
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{source_path}*.csv")
        
        # Add source file name column
        df = df.withColumn("source_file", input_file_name())
        
        return df
    except Exception as e:
        logger.error(f"Error reading CSV files: {e}")
        raise

def apply_data_quality_rules(spark, df, dq_rules):
    """Apply data quality rules and separate clean vs rejected records"""
    try:
        clean_df = df
        reject_conditions = []
        
        for rule in dq_rules.get('rules', []):
            rule_name = rule.get('rule_name')
            rule_type = rule.get('rule_type')
            column_name = rule.get('column')
            rule_condition = rule.get('condition')
            
            if rule_type == 'not_null':
                condition = col(column_name).isNotNull()
                reject_condition = col(column_name).isNull()
                reject_conditions.append(when(reject_condition, lit(f"Failed_{rule_name}")).otherwise(lit(None)))
                clean_df = clean_df.filter(condition)
                
            elif rule_type == 'data_type':
                expected_type = rule_condition.get('type')
                if expected_type == 'numeric':
                    condition = col(column_name).cast('double').isNotNull()
                    reject_condition = col(column_name).cast('double').isNull()
                    reject_conditions.append(when(reject_condition, lit(f"Failed_{rule_name}")).otherwise(lit(None)))
                    clean_df = clean_df.filter(condition)
                    
            elif rule_type == 'range':
                min_val = rule_condition.get('min')
                max_val = rule_condition.get('max')
                condition = (col(column_name) >= min_val) & (col(column_name) <= max_val)
                reject_condition = ~condition
                reject_conditions.append(when(reject_condition, lit(f"Failed_{rule_name}")).otherwise(lit(None)))
                clean_df = clean_df.filter(condition)
                
            elif rule_type == 'regex':
                pattern = rule_condition.get('pattern')
                condition = col(column_name).rlike(pattern)
                reject_condition = ~condition
                reject_conditions.append(when(reject_condition, lit(f"Failed_{rule_name}")).otherwise(lit(None)))
                clean_df = clean_df.filter(condition)
        
        # Create reject dataframe
        reject_condition_combined = None
        for condition in reject_conditions:
            if reject_condition_combined is None:
                reject_condition_combined = condition.isNotNull()
            else:
                reject_condition_combined = reject_condition_combined | condition.isNotNull()
        
        if reject_condition_combined is not None:
            reject_df = df.filter(reject_condition_combined)
            reject_df = reject_df.withColumn("reject_reason", 
                                           coalesce(*[cond for cond in reject_conditions if cond is not None]))
        else:
            reject_df = spark.createDataFrame([], df.schema.add("reject_reason", StringType()))
        
        return clean_df, reject_df
        
    except Exception as e:
        logger.error(f"Error applying data quality rules: {e}")
        raise

def apply_business_mapping(clean_df, mapping_df):
    """Apply business mapping transformations"""
    try:
        mart_df = clean_df
        
        # Apply column mappings if available
        if 'source_column' in mapping_df.columns and 'target_column' in mapping_df.columns:
            column_mapping = dict(zip(mapping_df['source_column'], mapping_df['target_column']))
            
            for old_col, new_col in column_mapping.items():
                if old_col in mart_df.columns:
                    mart_df = mart_df.withColumnRenamed(old_col, new_col)
        
        # Apply transformations if available
        if 'transformation' in mapping_df.columns:
            for _, row in mapping_df.iterrows():
                if pd.notna(row.get('transformation')):
                    transformation = row['transformation']
                    column = row.get('target_column', row.get('source_column'))
                    
                    if transformation == 'upper':
                        mart_df = mart_df.withColumn(column, upper(col(column)))
                    elif transformation == 'lower':
                        mart_df = mart_df.withColumn(column, lower(col(column)))
                    elif transformation == 'trim':
                        mart_df = mart_df.withColumn(column, trim(col(column)))
        
        return mart_df
        
    except Exception as e:
        logger.error(f"Error applying business mapping: {e}")
        return clean_df

def create_or_update_glue_table(table_name, s3_location, df, database_name, aws_region):
    """Create or update AWS Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Convert Spark schema to Glue columns
        columns = []
        for field in df.schema.fields:
            if field.name != 'ingest_date':  # Exclude partition column
                glue_type = spark_to_glue_type(field.dataType)
                columns.append({
                    'Name': field.name,
                    'Type': glue_type
                })
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': s3_location,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            },
            'PartitionKeys': [
                {
                    'Name': 'ingest_date',
                    'Type': 'string'
                }
            ]
        }
        
        try:
            # Try to update existing table
            glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            logger.info(f"Updated Glue table: {table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                # Create new table
                glue_client.create_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                logger.info(f"Created Glue table: {table_name}")
            else:
                raise
                
    except Exception as e:
        logger.error(f"Error creating/updating Glue table {table_name}: {e}")
        raise

def spark_to_glue_type(spark_type):
    """Convert Spark data type to Glue data type"""
    type_mapping = {
        'StringType': 'string',
        'IntegerType': 'int',
        'LongType': 'bigint',
        'DoubleType': 'double',
        'FloatType': 'float',
        'BooleanType': 'boolean',
        'TimestampType': 'timestamp',
        'DateType': 'date',
        'DecimalType': 'decimal'
    }
    
    spark_type_name = type(spark_type).__name__
    return type_mapping.get(spark_type_name, 'string')

def write_data_with_partition(df, output_path, ingest_date, mode='overwrite'):
    """Write dataframe to S3 with partition"""
    try:
        df_with_partition = df.withColumn("ingest_date", lit(ingest_date))
        
        df_with_partition.write \
            .mode(mode) \
            .partitionBy("ingest_date") \
            .parquet(output_path)
            
        logger.info(f"Successfully wrote data to {output_path}")
        
    except Exception as e:
        logger.error(f"Error writing data to {output_path}: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='ETL Data Pipeline')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = create_spark_session()
    
    try:
        # Define paths
        source_path = f"s3://{args.s3_bucket}/raw/"
        access_path = f"s3://{args.s3_bucket}/access/"
        mart_path = f"s3://{args.s3_bucket}/mart/"
        
        # Read configuration files
        logger.info("Reading DQ rules...")
        dq_rules = read_json_from_s3(args.rules_s3, args.aws_region)
        
        logger.info("Reading business mapping...")
        try:
            mapping_df = read_excel_from_s3(f"s3://{args.s3_bucket}/business_mapping.xlsx", args.aws_region)
        except:
            logger.warning("Business mapping file not found, proceeding without mapping")
            mapping_df = pd.DataFrame()
        
        # Discover and read CSV files
        logger.info("Reading CSV files from source...")
        raw_df = discover_csv_files(spark, source_path)
        
        # Determine category from source file path
        category = "general"  # Default category
        
        # Apply data quality rules
        logger.info("Applying data quality rules...")
        clean_df, reject_df = apply_data_quality_rules(spark, raw_df, dq_rules)
        
        # Write clean data to access layer
        access_clean_path = f"{access_path}{category}/"
        logger.info(f"Writing clean data to access layer: {access_clean_path}")
        write_data_with_partition(clean_df, access_clean_path, args.ingest_date)
        
        # Write reject data
        access_reject_path = f"{access_path}{category}/rejects/"
        logger.info(f"Writing reject data: {access_reject_path}")
        if reject_df.count() > 0:
            write_data_with_partition(reject_df, access_reject_path, args.ingest_date)
        
        # Apply business mapping and create mart
        if not mapping_df.empty:
            logger.info("Applying business mapping...")
            mart_df = apply_business_mapping(clean_df, mapping_df)
            
            mart_output_path = f"{mart_path}{category}/"
            logger.info(f"Writing mart data: {mart_output_path}")
            write_data_with_partition(mart_df, mart_output_path, args.ingest_date)
            
            # Create Glue table for mart
            create_or_update_glue_table(
                table_name=f"{category}_mart",
                s3_location=mart_output_path,
                df=mart_df,
                database_name="default",
                aws_region=args.aws_region
            )
        
        # Create Glue table for access layer
        create_or_update_glue_table(
            table_name=f"{category}_access",
            s3_location=access_clean_path,
            df=clean_df,
            database_name="default",
            aws_region=args.aws_region
        )
        
        logger.info("ETL pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()