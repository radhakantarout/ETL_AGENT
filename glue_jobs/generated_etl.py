import argparse
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import pandas as pd

def create_spark_session(app_name="ETL_Job"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        # Download rules file from S3
        s3_client = boto3.client('s3')
        bucket, key = rules_s3_path.replace('s3://', '').split('/', 1)
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        rules_content = response['Body'].read().decode('utf-8')
        return json.loads(rules_content)
    except Exception as e:
        print(f"Error reading DQ rules: {e}")
        return {}

def read_business_mapping(spark, s3_bucket):
    """Read business mapping Excel file from S3"""
    try:
        s3_client = boto3.client('s3')
        mapping_key = 'business_mapping.xlsx'
        
        # Download Excel file locally
        s3_client.download_file(s3_bucket, mapping_key, '/tmp/business_mapping.xlsx')
        
        # Read Excel file using pandas
        mapping_df = pd.read_excel('/tmp/business_mapping.xlsx')
        return spark.createDataFrame(mapping_df)
    except Exception as e:
        print(f"Error reading business mapping: {e}")
        return None

def apply_dq_rules(df, rules):
    """Apply data quality rules and separate clean and reject records"""
    if not rules:
        return df, spark.createDataFrame([], df.schema.add("rejection_reason", StringType()))
    
    clean_df = df
    reject_conditions = []
    
    for table_name, table_rules in rules.items():
        if 'columns' in table_rules:
            for column_name, column_rules in table_rules['columns'].items():
                if column_name in df.columns:
                    # Apply not null rule
                    if column_rules.get('not_null', False):
                        reject_condition = col(column_name).isNull()
                        reject_conditions.append((reject_condition, f"{column_name}_null"))
                    
                    # Apply data type validation
                    if 'data_type' in column_rules:
                        data_type = column_rules['data_type']
                        if data_type == 'integer':
                            reject_condition = ~col(column_name).rlike(r'^\d+$')
                            reject_conditions.append((reject_condition, f"{column_name}_invalid_integer"))
                        elif data_type == 'decimal':
                            reject_condition = ~col(column_name).rlike(r'^\d*\.?\d+$')
                            reject_conditions.append((reject_condition, f"{column_name}_invalid_decimal"))
                    
                    # Apply min/max length validation
                    if 'min_length' in column_rules:
                        min_len = column_rules['min_length']
                        reject_condition = length(col(column_name)) < min_len
                        reject_conditions.append((reject_condition, f"{column_name}_min_length"))
                    
                    if 'max_length' in column_rules:
                        max_len = column_rules['max_length']
                        reject_condition = length(col(column_name)) > max_len
                        reject_conditions.append((reject_condition, f"{column_name}_max_length"))
    
    # Create reject dataframe
    if reject_conditions:
        combined_reject_condition = reject_conditions[0][0]
        rejection_reason = when(reject_conditions[0][0], reject_conditions[0][1])
        
        for condition, reason in reject_conditions[1:]:
            combined_reject_condition = combined_reject_condition | condition
            rejection_reason = rejection_reason.when(condition, reason)
        
        reject_df = df.filter(combined_reject_condition).withColumn("rejection_reason", rejection_reason)
        clean_df = df.filter(~combined_reject_condition)
    else:
        reject_df = spark.createDataFrame([], df.schema.add("rejection_reason", StringType()))
    
    return clean_df, reject_df

def create_glue_table(database_name, table_name, s3_path, schema, aws_region, partition_keys=None):
    """Create or update Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Convert Spark schema to Glue columns
        columns = []
        for field in schema.fields:
            if partition_keys and field.name in partition_keys:
                continue
            
            glue_type = "string"  # default
            if field.dataType == IntegerType():
                glue_type = "int"
            elif field.dataType == LongType():
                glue_type = "bigint"
            elif field.dataType == DoubleType():
                glue_type = "double"
            elif field.dataType == FloatType():
                glue_type = "float"
            elif field.dataType == BooleanType():
                glue_type = "boolean"
            elif field.dataType == DateType():
                glue_type = "date"
            elif field.dataType == TimestampType():
                glue_type = "timestamp"
            
            columns.append({
                'Name': field.name,
                'Type': glue_type
            })
        
        # Partition keys
        partition_columns = []
        if partition_keys:
            for pk in partition_keys:
                partition_columns.append({
                    'Name': pk,
                    'Type': 'string'
                })
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': s3_path,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                }
            },
            'PartitionKeys': partition_columns
        }
        
        # Try to update existing table, create if doesn't exist
        try:
            glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            print(f"Updated Glue table: {database_name}.{table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                glue_client.create_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                print(f"Created Glue table: {database_name}.{table_name}")
            else:
                raise e
                
    except Exception as e:
        print(f"Error creating/updating Glue table {table_name}: {e}")

def process_csv_files(spark, source_path, dq_rules, s3_bucket, ingest_date, aws_region):
    """Process all CSV files from source path"""
    try:
        # Read all CSV files with wildcard
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{source_path}*.csv")
        
        if df.count() == 0:
            print("No data found in source CSV files")
            return None, None
        
        # Apply data quality rules
        clean_df, reject_df = apply_dq_rules(df, dq_rules)
        
        print(f"Total records: {df.count()}")
        print(f"Clean records: {clean_df.count()}")
        print(f"Rejected records: {reject_df.count()}")
        
        return clean_df, reject_df
        
    except Exception as e:
        print(f"Error processing CSV files: {e}")
        return None, None

def write_to_s3_and_catalog(df, s3_path, table_name, aws_region, ingest_date, database_name="default"):
    """Write dataframe to S3 and create Glue table"""
    try:
        # Write to S3 with partitioning
        df.write.mode("overwrite").partitionBy("ingest_date").parquet(s3_path)
        
        # Create/update Glue table
        create_glue_table(
            database_name=database_name,
            table_name=table_name,
            s3_path=s3_path,
            schema=df.schema,
            aws_region=aws_region,
            partition_keys=["ingest_date"]
        )
        
        print(f"Successfully wrote data to {s3_path}")
        
    except Exception as e:
        print(f"Error writing to S3 and catalog: {e}")

def create_mart_table(spark, clean_df, business_mapping, s3_bucket, ingest_date, aws_region):
    """Create mart table using business mapping"""
    try:
        if business_mapping is None:
            print("No business mapping found, skipping mart creation")
            return
        
        # Apply business transformations based on mapping
        # This is a simplified version - adjust based on your mapping structure
        mart_df = clean_df
        
        # Add ingest_date column
        mart_df = mart_df.withColumn("ingest_date", lit(ingest_date))
        
        # Write mart data
        mart_path = f"s3://{s3_bucket}/mart/business_mart/ingest_date={ingest_date}/"
        write_to_s3_and_catalog(
            mart_df, 
            f"s3://{s3_bucket}/mart/business_mart/",
            "business_mart",
            aws_region,
            ingest_date
        )
        
    except Exception as e:
        print(f"Error creating mart table: {e}")

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("ETL_Job")
    
    try:
        # Read DQ rules
        dq_rules = read_dq_rules(spark, args.rules_s3)
        
        # Read business mapping
        business_mapping = read_business_mapping(spark, args.s3_bucket)
        
        # Source path
        source_path = f"s3://{args.s3_bucket}/raw/"
        
        # Process CSV files
        clean_df, reject_df = process_csv_files(
            spark, source_path, dq_rules, args.s3_bucket, args.ingest_date, args.aws_region
        )
        
        if clean_df is None:
            print("No data to process")
            return
        
        # Add ingest_date column to both dataframes
        clean_df = clean_df.withColumn("ingest_date", lit(args.ingest_date))
        reject_df = reject_df.withColumn("ingest_date", lit(args.ingest_date))
        
        # Write clean data to access layer
        access_path = f"s3://{args.s3_bucket}/access/clean_data/"
        write_to_s3_and_catalog(
            clean_df, access_path, "clean_data", args.aws_region, args.ingest_date
        )
        
        # Write reject data
        reject_path = f"s3://{args.s3_bucket}/access/clean_data/rejects/"
        write_to_s3_and_catalog(
            reject_df, reject_path, "reject_data", args.aws_region, args.ingest_date
        )
        
        # Create mart table
        create_mart_table(
            spark, clean_df, business_mapping, args.s3_bucket, args.ingest_date, args.aws_region
        )
        
        print("ETL job completed successfully!")
        
    except Exception as e:
        print(f"ETL job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()