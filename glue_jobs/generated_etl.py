import argparse
import json
import sys
from datetime import datetime
from typing import Dict, List, Any

import boto3
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session with required configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def read_s3_json(s3_path: str) -> Dict[str, Any]:
    """Read JSON file from S3"""
    s3_client = boto3.client('s3')
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))


def read_s3_excel(s3_path: str) -> pd.DataFrame:
    """Read Excel file from S3"""
    s3_client = boto3.client('s3')
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_excel(response['Body'].read())


def discover_csv_files(spark: SparkSession, s3_path: str) -> List[str]:
    """Discover all CSV files in S3 path"""
    s3_client = boto3.client('s3')
    bucket = s3_path.replace('s3://', '').split('/')[0]
    prefix = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    csv_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.csv'):
                    csv_files.append(f"s3://{bucket}/{obj['Key']}")
    
    return csv_files


def read_csv_files(spark: SparkSession, csv_files: List[str]) -> Dict[str, DataFrame]:
    """Read all CSV files and return as dictionary of DataFrames"""
    dataframes = {}
    
    for csv_file in csv_files:
        try:
            # Extract table name from file path
            table_name = csv_file.split('/')[-1].replace('.csv', '')
            
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(csv_file)
            
            # Add metadata columns
            df = df.withColumn("source_file", lit(csv_file)) \
                   .withColumn("load_timestamp", current_timestamp())
            
            dataframes[table_name] = df
            print(f"Successfully read {csv_file} with {df.count()} records")
            
        except Exception as e:
            print(f"Error reading {csv_file}: {str(e)}")
            continue
    
    return dataframes


def apply_data_quality_rules(df: DataFrame, rules: Dict[str, Any], table_name: str) -> tuple[DataFrame, DataFrame]:
    """Apply data quality rules and return clean and reject DataFrames"""
    if table_name not in rules:
        print(f"No DQ rules found for table {table_name}, returning original DataFrame")
        return df, spark.createDataFrame([], df.schema)
    
    table_rules = rules[table_name]
    clean_df = df
    reject_conditions = []
    
    # Apply null checks
    if "null_checks" in table_rules:
        for column in table_rules["null_checks"]:
            if column in df.columns:
                reject_conditions.append(col(column).isNull())
    
    # Apply data type validations
    if "data_types" in table_rules:
        for column, expected_type in table_rules["data_types"].items():
            if column in df.columns:
                if expected_type.lower() == "numeric":
                    reject_conditions.append(~col(column).rlike("^[0-9]+\.?[0-9]*$"))
                elif expected_type.lower() == "date":
                    reject_conditions.append(col(column).isNull() | (col(column) == ""))
    
    # Apply value validations
    if "value_checks" in table_rules:
        for column, conditions in table_rules["value_checks"].items():
            if column in df.columns:
                if "min_length" in conditions:
                    reject_conditions.append(length(col(column)) < conditions["min_length"])
                if "max_length" in conditions:
                    reject_conditions.append(length(col(column)) > conditions["max_length"])
                if "allowed_values" in conditions:
                    reject_conditions.append(~col(column).isin(conditions["allowed_values"]))
    
    # Apply range checks
    if "range_checks" in table_rules:
        for column, range_config in table_rules["range_checks"].items():
            if column in df.columns:
                if "min_value" in range_config:
                    reject_conditions.append(col(column) < range_config["min_value"])
                if "max_value" in range_config:
                    reject_conditions.append(col(column) > range_config["max_value"])
    
    # Create reject DataFrame
    if reject_conditions:
        reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            reject_condition = reject_condition | condition
        
        reject_df = df.filter(reject_condition) \
                     .withColumn("reject_reason", lit("DQ_VALIDATION_FAILED")) \
                     .withColumn("reject_timestamp", current_timestamp())
        
        clean_df = df.filter(~reject_condition)
    else:
        reject_df = spark.createDataFrame([], df.schema)
    
    return clean_df, reject_df


def apply_business_mappings(clean_dataframes: Dict[str, DataFrame], mapping_df: pd.DataFrame) -> Dict[str, DataFrame]:
    """Apply business mappings from Excel file to create mart tables"""
    mart_dataframes = {}
    
    # Group mappings by target table
    target_tables = mapping_df['target_table'].unique()
    
    for target_table in target_tables:
        table_mappings = mapping_df[mapping_df['target_table'] == target_table]
        
        # Find source table
        source_tables = table_mappings['source_table'].unique()
        
        for source_table in source_tables:
            if source_table in clean_dataframes:
                source_df = clean_dataframes[source_table]
                source_mappings = table_mappings[table_mappings['source_table'] == source_table]
                
                # Apply column mappings and transformations
                select_exprs = []
                
                for _, mapping in source_mappings.iterrows():
                    source_col = mapping['source_column']
                    target_col = mapping['target_column']
                    transformation = mapping.get('transformation', '')
                    
                    if source_col in source_df.columns:
                        if transformation and transformation.strip():
                            # Apply transformation logic
                            if transformation.lower() == 'upper':
                                select_exprs.append(upper(col(source_col)).alias(target_col))
                            elif transformation.lower() == 'lower':
                                select_exprs.append(lower(col(source_col)).alias(target_col))
                            elif transformation.lower() == 'trim':
                                select_exprs.append(trim(col(source_col)).alias(target_col))
                            elif 'cast' in transformation.lower():
                                # Extract cast type from transformation
                                cast_type = transformation.lower().replace('cast', '').strip('() ')
                                select_exprs.append(col(source_col).cast(cast_type).alias(target_col))
                            else:
                                select_exprs.append(col(source_col).alias(target_col))
                        else:
                            select_exprs.append(col(source_col).alias(target_col))
                
                if select_exprs:
                    mart_df = source_df.select(*select_exprs)
                    mart_dataframes[target_table] = mart_df
    
    return mart_dataframes


def write_to_s3(df: DataFrame, s3_path: str, format_type: str = "parquet"):
    """Write DataFrame to S3"""
    try:
        df.write \
          .mode("overwrite") \
          .format(format_type) \
          .save(s3_path)
        print(f"Successfully wrote data to {s3_path}")
    except Exception as e:
        print(f"Error writing to {s3_path}: {str(e)}")
        raise


def create_glue_table(table_name: str, s3_path: str, df: DataFrame, database_name: str, aws_region: str):
    """Create or update Glue external table"""
    glue_client = boto3.client('glue', region_name=aws_region)
    
    # Convert Spark schema to Glue schema
    columns = []
    for field in df.schema.fields:
        glue_type = "string"  # Default
        if isinstance(field.dataType, IntegerType):
            glue_type = "int"
        elif isinstance(field.dataType, LongType):
            glue_type = "bigint"
        elif isinstance(field.dataType, DoubleType):
            glue_type = "double"
        elif isinstance(field.dataType, FloatType):
            glue_type = "float"
        elif isinstance(field.dataType, BooleanType):
            glue_type = "boolean"
        elif isinstance(field.dataType, TimestampType):
            glue_type = "timestamp"
        elif isinstance(field.dataType, DateType):
            glue_type = "date"
        
        columns.append({
            'Name': field.name,
            'Type': glue_type
        })
    
    partition_keys = [
        {
            'Name': 'ingest_date',
            'Type': 'string'
        }
    ]
    
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_path,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        },
        'PartitionKeys': partition_keys,
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
        try:
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            print(f"Created Glue table: {database_name}.{table_name}")
        except Exception as e:
            print(f"Error creating Glue table {table_name}: {str(e)}")
    except Exception as e:
        print(f"Error updating Glue table {table_name}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="PySpark ETL Job")
    parser.add_argument("--rules_s3", required=True, help="S3 path to DQ rules JSON file")
    parser.add_argument("--s3_bucket", required=True, help="S3 bucket name")
    parser.add_argument("--aws_region", required=True, help="AWS region")
    parser.add_argument("--ingest_date", required=True, help="Ingest date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session("ETL_Job")
    
    try:
        # Read DQ rules
        print("Reading DQ rules from S3...")
        dq_rules = read_s3_json(args.rules_s3)
        
        # Read business mapping
        print("Reading business mapping from S3...")
        mapping_s3_path = f"s3://{args.s3_bucket}/business_mapping.xlsx"
        try:
            mapping_df = read_s3_excel(mapping_s3_path)
        except:
            print("Business mapping file not found, creating empty mapping")
            mapping_df = pd.DataFrame(columns=['source_table', 'source_column', 'target_table', 'target_column', 'transformation'])
        
        # Discover and read CSV files
        source_path = f"s3://{args.s3_bucket}/raw/"
        print(f"Discovering CSV files in {source_path}...")
        csv_files = discover_csv_files(spark, source_path)
        
        if not csv_files:
            print("No CSV files found in source path")
            return
        
        print(f"Found {len(csv_files)} CSV files")
        dataframes = read_csv_files(spark, csv_files)
        
        clean_dataframes = {}
        
        # Process each table
        for table_name, df in dataframes.items():
            print(f"Processing table: {table_name}")
            
            # Apply DQ rules
            clean_df, reject_df = apply_data_quality_rules(df, dq_rules, table_name)
            
            print(f"Table {table_name}: {clean_df.count()} clean records, {reject_df.count()} rejected records")
            
            # Write clean data to access layer
            access_path = f"s3://{args.s3_bucket}/access/{table_name}/ingest_date={args.ingest_date}/"
            write_to_s3(clean_df, access_path)
            
            # Write rejects if any
            if reject_df.count() > 0:
                reject_path = f"s3://{args.s3_bucket}/access/{table_name}/rejects/ingest_date={args.ingest_date}/"
                write_to_s3(reject_df, reject_path)
            
            # Create Glue table for access layer
            create_glue_table(
                f"access_{table_name}",
                f"s3://{args.s3_bucket}/access/{table_name}/",
                clean_df,
                "default",
                args.aws_region
            )
            
            clean_dataframes[table_name] = clean_df
        
        # Apply business mappings for mart layer
        if not mapping_df.empty:
            print("Applying business mappings...")
            mart_dataframes = apply_business_mappings(clean_dataframes, mapping_df)
            
            # Write mart tables
            for mart_table_name, mart_df in mart_dataframes.items():
                print(f"Writing mart table: {mart_table_name}")
                
                mart_path = f"s3://{args.s3_bucket}/mart/{mart_table_name}/ingest_date={args.ingest_date}/"
                write_to_s3(mart_df, mart_path)
                
                # Create Glue table for mart layer
                create_glue_table(
                    f"mart_{mart_table_name}",
                    f"s3://{args.s3_bucket}/mart/{mart_table_name}/",
                    mart_df,
                    "default",
                    args.aws_region
                )
        
        print("ETL job completed successfully!")
        
    except Exception as e:
        print(f"ETL job failed with error: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()