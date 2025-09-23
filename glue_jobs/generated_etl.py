import argparse
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import os

def create_spark_session(app_name="ETL_Job"):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        # Read the JSON file from S3
        df_rules = spark.read.option("multiline", "true").text(rules_s3_path)
        rules_content = df_rules.collect()[0][0]
        return json.loads(rules_content)
    except Exception as e:
        print(f"Error reading DQ rules: {str(e)}")
        return {}

def read_business_mapping(s3_bucket, aws_region):
    """Read business mapping from Excel file in S3"""
    try:
        s3_client = boto3.client('s3', region_name=aws_region)
        mapping_key = "business_mapping.xlsx"
        
        # Download Excel file temporarily
        temp_file = "/tmp/business_mapping.xlsx"
        s3_client.download_file(s3_bucket, mapping_key, temp_file)
        
        # Read Excel file
        mapping_df = pd.read_excel(temp_file)
        
        # Clean up temp file
        os.remove(temp_file)
        
        return mapping_df.to_dict('records')
    except Exception as e:
        print(f"Error reading business mapping: {str(e)}")
        return []

def apply_dq_rules(df, rules, spark):
    """Apply data quality rules and separate clean/reject records"""
    if not rules:
        return df, spark.createDataFrame([], df.schema)
    
    # Start with all records as potentially clean
    clean_df = df
    reject_conditions = []
    
    for table_name, table_rules in rules.items():
        if 'rules' in table_rules:
            for rule in table_rules['rules']:
                rule_name = rule.get('rule_name', 'unknown')
                rule_type = rule.get('rule_type', '')
                column = rule.get('column', '')
                
                if rule_type == 'not_null' and column in df.columns:
                    reject_condition = col(column).isNull()
                    reject_conditions.append(reject_condition)
                    
                elif rule_type == 'data_type' and column in df.columns:
                    expected_type = rule.get('expected_type', '')
                    if expected_type == 'integer':
                        reject_condition = ~col(column).rlike(r'^\d+$')
                    elif expected_type == 'decimal':
                        reject_condition = ~col(column).rlike(r'^\d*\.?\d+$')
                    elif expected_type == 'date':
                        reject_condition = col(column).isNull() | (col(column) == '')
                    else:
                        continue
                    reject_conditions.append(reject_condition)
                    
                elif rule_type == 'range' and column in df.columns:
                    min_val = rule.get('min_value')
                    max_val = rule.get('max_value')
                    if min_val is not None and max_val is not None:
                        reject_condition = (col(column).cast('double') < min_val) | (col(column).cast('double') > max_val)
                        reject_conditions.append(reject_condition)
    
    # Combine all reject conditions
    if reject_conditions:
        combined_reject_condition = reject_conditions[0]
        for condition in reject_conditions[1:]:
            combined_reject_condition = combined_reject_condition | condition
        
        # Split clean and reject records
        reject_df = df.filter(combined_reject_condition).withColumn("reject_reason", lit("DQ_RULE_VIOLATION"))
        clean_df = df.filter(~combined_reject_condition)
    else:
        reject_df = spark.createDataFrame([], df.schema)
    
    return clean_df, reject_df

def apply_business_mapping(df, mapping_rules):
    """Apply business transformations based on mapping rules"""
    if not mapping_rules:
        return df
    
    transformed_df = df
    
    for rule in mapping_rules:
        source_col = rule.get('source_column', '')
        target_col = rule.get('target_column', '')
        transformation = rule.get('transformation', '')
        
        if source_col in df.columns and target_col and transformation:
            if transformation == 'uppercase':
                transformed_df = transformed_df.withColumn(target_col, upper(col(source_col)))
            elif transformation == 'lowercase':
                transformed_df = transformed_df.withColumn(target_col, lower(col(source_col)))
            elif transformation == 'trim':
                transformed_df = transformed_df.withColumn(target_col, trim(col(source_col)))
            elif transformation.startswith('substring'):
                # Format: "substring(0,10)"
                params = transformation.split('(')[1].split(')')[0].split(',')
                start = int(params[0])
                length = int(params[1])
                transformed_df = transformed_df.withColumn(target_col, substring(col(source_col), start, length))
            else:
                # Default: just copy the column
                transformed_df = transformed_df.withColumn(target_col, col(source_col))
    
    return transformed_df

def create_glue_table(table_name, s3_path, columns, s3_bucket, aws_region, database_name="default"):
    """Create or update AWS Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Prepare column definitions
        column_list = []
        for col_name, col_type in columns:
            glue_type = "string"  # Default
            if col_type in ["int", "integer", "bigint"]:
                glue_type = "bigint"
            elif col_type in ["double", "float", "decimal"]:
                glue_type = "double"
            elif col_type in ["boolean"]:
                glue_type = "boolean"
            elif col_type in ["date"]:
                glue_type = "date"
            elif col_type in ["timestamp"]:
                glue_type = "timestamp"
                
            column_list.append({
                'Name': col_name,
                'Type': glue_type
            })
        
        # Table definition
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
            'PartitionKeys': [
                {
                    'Name': 'ingest_date',
                    'Type': 'string'
                }
            ],
            'TableType': 'EXTERNAL_TABLE'
        }
        
        # Try to update first, then create if it doesn't exist
        try:
            glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            print(f"Updated Glue table: {table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                glue_client.create_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                print(f"Created Glue table: {table_name}")
            else:
                raise e
                
    except Exception as e:
        print(f"Error creating/updating Glue table {table_name}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to DQ rules JSON file')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', required=True, help='Ingest date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = create_spark_session()
    
    try:
        # Read DQ rules
        dq_rules = read_dq_rules(spark, args.rules_s3)
        
        # Read business mapping
        business_mapping = read_business_mapping(args.s3_bucket, args.aws_region)
        
        # Source path with wildcard
        source_path = f"s3a://{args.s3_bucket}/raw/*.csv"
        
        # Read all CSV files
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        if df.count() == 0:
            print("No data found in source path")
            return
        
        # Determine category from the data or use default
        category = "default"
        
        # Apply DQ rules
        clean_df, reject_df = apply_dq_rules(df, dq_rules, spark)
        
        # Add ingest_date partition column
        clean_df = clean_df.withColumn("ingest_date", lit(args.ingest_date))
        
        # Write clean data to access layer
        access_path = f"s3a://{args.s3_bucket}/access/{category}"
        clean_df.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(access_path)
        
        # Write rejects if any
        if reject_df.count() > 0:
            reject_df = reject_df.withColumn("ingest_date", lit(args.ingest_date))
            reject_path = f"s3a://{args.s3_bucket}/access/{category}/rejects"
            reject_df.write \
                .mode("overwrite") \
                .partitionBy("ingest_date") \
                .parquet(reject_path)
        
        # Apply business mapping for mart layer
        mart_df = apply_business_mapping(clean_df, business_mapping)
        
        # Write to mart layer
        mart_path = f"s3a://{args.s3_bucket}/mart/{category}"
        mart_df.write \
            .mode("overwrite") \
            .partitionBy("ingest_date") \
            .parquet(mart_path)
        
        # Create Glue tables
        clean_columns = [(field.name, field.dataType.simpleString()) for field in clean_df.schema.fields if field.name != "ingest_date"]
        mart_columns = [(field.name, field.dataType.simpleString()) for field in mart_df.schema.fields if field.name != "ingest_date"]
        
        create_glue_table(
            f"access_{category}",
            f"s3://{args.s3_bucket}/access/{category}/",
            clean_columns,
            args.s3_bucket,
            args.aws_region
        )
        
        create_glue_table(
            f"mart_{category}",
            f"s3://{args.s3_bucket}/mart/{category}/",
            mart_columns,
            args.s3_bucket,
            args.aws_region
        )
        
        print(f"ETL job completed successfully")
        print(f"Clean records: {clean_df.count()}")
        print(f"Reject records: {reject_df.count()}")
        
    except Exception as e:
        print(f"ETL job failed: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()