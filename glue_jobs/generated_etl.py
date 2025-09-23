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
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def read_s3_file_content(s3_path, aws_region):
    s3_client = boto3.client('s3', region_name=aws_region)
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def load_dq_rules(rules_s3_path, aws_region):
    content = read_s3_file_content(rules_s3_path, aws_region)
    return json.loads(content)

def load_business_mapping(mapping_s3_path, aws_region):
    s3_client = boto3.client('s3', region_name=aws_region)
    bucket, key = mapping_s3_path.replace('s3://', '').split('/', 1)
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        excel_data = response['Body'].read()
        return pd.read_excel(excel_data)
    except:
        return None

def discover_csv_files(spark, source_path):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(source_path)
    
    files = []
    if fs.exists(path):
        file_statuses = fs.listStatus(path)
        for file_status in file_statuses:
            file_path = str(file_status.getPath())
            if file_path.endswith('.csv'):
                files.append(file_path)
    
    return files

def infer_schema_and_read_csv(spark, file_paths):
    if not file_paths:
        return None
    
    # Read first file to infer schema
    sample_df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_paths[0])
    
    # Read all files with inferred schema
    all_files_pattern = file_paths[0].rsplit('/', 1)[0] + "/*.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(all_files_pattern)
    
    return df

def apply_dq_rules(df, dq_rules):
    clean_df = df
    reject_df = None
    
    for table_name, rules in dq_rules.items():
        if 'columns' in rules:
            for column_name, column_rules in rules['columns'].items():
                if column_name in df.columns:
                    # Apply null checks
                    if column_rules.get('nullable') == False:
                        reject_condition = col(column_name).isNull()
                        if reject_df is None:
                            reject_df = clean_df.filter(reject_condition).withColumn("reject_reason", lit(f"{column_name}_null"))
                        else:
                            reject_df = reject_df.union(
                                clean_df.filter(reject_condition).withColumn("reject_reason", lit(f"{column_name}_null"))
                            )
                        clean_df = clean_df.filter(~reject_condition)
                    
                    # Apply data type validation
                    if 'data_type' in column_rules:
                        expected_type = column_rules['data_type']
                        if expected_type == 'integer':
                            reject_condition = ~col(column_name).rlike("^[+-]?[0-9]+$")
                        elif expected_type == 'decimal':
                            reject_condition = ~col(column_name).rlike("^[+-]?[0-9]*\\.?[0-9]+$")
                        elif expected_type == 'date':
                            reject_condition = col(column_name).isNull() | (to_date(col(column_name)).isNull())
                        else:
                            continue
                        
                        if reject_df is None:
                            reject_df = clean_df.filter(reject_condition).withColumn("reject_reason", lit(f"{column_name}_type_mismatch"))
                        else:
                            reject_df = reject_df.union(
                                clean_df.filter(reject_condition).withColumn("reject_reason", lit(f"{column_name}_type_mismatch"))
                            )
                        clean_df = clean_df.filter(~reject_condition)
                    
                    # Apply length validation
                    if 'max_length' in column_rules:
                        max_len = column_rules['max_length']
                        reject_condition = length(col(column_name)) > max_len
                        if reject_df is None:
                            reject_df = clean_df.filter(reject_condition).withColumn("reject_reason", lit(f"{column_name}_length_exceeded"))
                        else:
                            reject_df = reject_df.union(
                                clean_df.filter(reject_condition).withColumn("reject_reason", lit(f"{column_name}_length_exceeded"))
                            )
                        clean_df = clean_df.filter(~reject_condition)
    
    if reject_df is None:
        reject_df = spark.createDataFrame([], df.schema.add(StructField("reject_reason", StringType(), True)))
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_df):
    if mapping_df is None or mapping_df.empty:
        return df
    
    # Apply column mappings if mapping file exists
    for _, row in mapping_df.iterrows():
        if 'source_column' in mapping_df.columns and 'target_column' in mapping_df.columns:
            source_col = row['source_column']
            target_col = row['target_column']
            if source_col in df.columns and source_col != target_col:
                df = df.withColumnRenamed(source_col, target_col)
    
    return df

def create_glue_table(database_name, table_name, s3_location, columns, aws_region, partition_keys=None):
    glue_client = boto3.client('glue', region_name=aws_region)
    
    storage_descriptor = {
        'Columns': columns,
        'Location': s3_location,
        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        },
        'StoredAsSubDirectories': False
    }
    
    if partition_keys:
        storage_descriptor['PartitionKeys'] = partition_keys
    
    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'TableType': 'EXTERNAL_TABLE'
    }
    
    try:
        # Try to update existing table
        glue_client.update_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        print(f"Updated Glue table: {database_name}.{table_name}")
    except glue_client.exceptions.EntityNotFoundException:
        # Create new table if it doesn't exist
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        print(f"Created Glue table: {database_name}.{table_name}")
    except Exception as e:
        print(f"Error creating/updating Glue table {database_name}.{table_name}: {str(e)}")

def spark_to_glue_columns(df):
    columns = []
    for field in df.schema.fields:
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
    return columns

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = create_spark_session("CSV_ETL_Job")
    
    try:
        # Define paths
        source_path = f"s3://{args.s3_bucket}/raw/"
        access_path = f"s3://{args.s3_bucket}/access/"
        mart_path = f"s3://{args.s3_bucket}/mart/"
        mapping_path = f"s3://{args.s3_bucket}/raw/business_mapping.xlsx"
        
        # Load DQ rules
        print("Loading DQ rules...")
        dq_rules = load_dq_rules(args.rules_s3, args.aws_region)
        
        # Load business mapping
        print("Loading business mapping...")
        mapping_df = load_business_mapping(mapping_path, args.aws_region)
        
        # Discover and read CSV files
        print("Discovering CSV files...")
        csv_files = discover_csv_files(spark, source_path)
        
        if not csv_files:
            print("No CSV files found in source path")
            return
        
        print(f"Found {len(csv_files)} CSV files")
        
        # Read CSV data
        print("Reading CSV data...")
        df = infer_schema_and_read_csv(spark, csv_files)
        
        if df is None:
            print("No data to process")
            return
        
        # Add ingest_date column
        df = df.withColumn("ingest_date", lit(args.ingest_date))
        
        print(f"Total records: {df.count()}")
        
        # Apply DQ rules
        print("Applying DQ rules...")
        clean_df, reject_df = apply_dq_rules(df, dq_rules)
        
        print(f"Clean records: {clean_df.count()}")
        print(f"Rejected records: {reject_df.count()}")
        
        # Determine category from source data (use first table name from DQ rules or default)
        category = list(dq_rules.keys())[0] if dq_rules else "data"
        
        # Write clean data to access layer
        clean_output_path = f"{access_path}{category}/"
        print(f"Writing clean data to {clean_output_path}")
        clean_df.write.mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(clean_output_path)
        
        # Write rejects
        reject_output_path = f"{access_path}{category}/rejects/"
        print(f"Writing rejects to {reject_output_path}")
        reject_df.write.mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(reject_output_path)
        
        # Apply business mapping and create mart
        print("Creating mart data...")
        mart_df = apply_business_mapping(clean_df, mapping_df)
        mart_output_path = f"{mart_path}{category}/"
        print(f"Writing mart data to {mart_output_path}")
        mart_df.write.mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(mart_output_path)
        
        # Create Glue tables
        print("Creating/updating Glue tables...")
        
        # Access layer table
        access_columns = spark_to_glue_columns(clean_df)
        partition_keys = [{'Name': 'ingest_date', 'Type': 'string'}]
        
        create_glue_table(
            database_name="access_db",
            table_name=f"{category}_clean",
            s3_location=clean_output_path,
            columns=access_columns,
            aws_region=args.aws_region,
            partition_keys=partition_keys
        )
        
        # Rejects table
        reject_columns = spark_to_glue_columns(reject_df)
        create_glue_table(
            database_name="access_db",
            table_name=f"{category}_rejects",
            s3_location=reject_output_path,
            columns=reject_columns,
            aws_region=args.aws_region,
            partition_keys=partition_keys
        )
        
        # Mart layer table
        mart_columns = spark_to_glue_columns(mart_df)
        create_glue_table(
            database_name="mart_db",
            table_name=f"{category}_mart",
            s3_location=mart_output_path,
            columns=mart_columns,
            aws_region=args.aws_region,
            partition_keys=partition_keys
        )
        
        print("ETL job completed successfully!")
        
    except Exception as e:
        print(f"Error in ETL job: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()