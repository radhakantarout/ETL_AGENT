import argparse
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="ETL_Job"):
    """Create Spark session with S3 configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_dq_rules(spark, rules_s3_path):
    """Read data quality rules from S3"""
    try:
        # Read JSON file from S3
        df_json = spark.read.text(rules_s3_path)
        json_content = df_json.collect()[0][0]
        rules = json.loads(json_content)
        logger.info(f"Successfully loaded DQ rules from {rules_s3_path}")
        return rules
    except Exception as e:
        logger.error(f"Failed to read DQ rules from {rules_s3_path}: {str(e)}")
        raise

def read_business_mapping(s3_bucket, aws_region):
    """Read business mapping from Excel file in S3"""
    try:
        s3_client = boto3.client('s3', region_name=aws_region)
        mapping_key = "business_mapping.xlsx"
        
        # Download Excel file temporarily
        local_file = "/tmp/business_mapping.xlsx"
        s3_client.download_file(s3_bucket, mapping_key, local_file)
        
        # Read Excel file
        mapping_df = pd.read_excel(local_file)
        logger.info("Successfully loaded business mapping")
        return mapping_df.to_dict('records')
    except Exception as e:
        logger.warning(f"Failed to read business mapping: {str(e)}")
        return []

def apply_dq_rules(df, rules):
    """Apply data quality rules and separate clean vs rejected records"""
    try:
        clean_df = df
        reject_conditions = []
        
        for rule in rules.get('rules', []):
            rule_name = rule.get('name', 'unknown_rule')
            rule_condition = rule.get('condition', '')
            rule_type = rule.get('type', 'filter')
            
            if rule_type == 'not_null':
                columns = rule.get('columns', [])
                for col in columns:
                    if col in df.columns:
                        reject_conditions.append(f"({col} IS NULL)")
                        clean_df = clean_df.filter(col(col).isNotNull())
            
            elif rule_type == 'unique':
                columns = rule.get('columns', [])
                if columns:
                    # Remove duplicates for clean data
                    clean_df = clean_df.dropDuplicates(columns)
            
            elif rule_type == 'range':
                column = rule.get('column', '')
                min_val = rule.get('min_value')
                max_val = rule.get('max_value')
                if column in df.columns:
                    if min_val is not None:
                        reject_conditions.append(f"({column} < {min_val})")
                        clean_df = clean_df.filter(col(column) >= min_val)
                    if max_val is not None:
                        reject_conditions.append(f"({column} > {max_val})")
                        clean_df = clean_df.filter(col(column) <= max_val)
            
            elif rule_type == 'regex':
                column = rule.get('column', '')
                pattern = rule.get('pattern', '')
                if column in df.columns and pattern:
                    reject_conditions.append(f"({column} NOT RLIKE '{pattern}')")
                    clean_df = clean_df.filter(col(column).rlike(pattern))
            
            elif rule_type == 'custom' and rule_condition:
                reject_conditions.append(f"NOT ({rule_condition})")
                clean_df = clean_df.filter(expr(rule_condition))
        
        # Create rejects dataframe
        if reject_conditions:
            reject_condition = " OR ".join(reject_conditions)
            rejects_df = df.filter(expr(reject_condition)).withColumn("reject_reason", lit(reject_condition))
        else:
            # Create empty rejects dataframe with same schema plus reject_reason
            rejects_df = spark.createDataFrame([], df.schema.add("reject_reason", StringType()))
        
        logger.info(f"Applied {len(rules.get('rules', []))} DQ rules")
        return clean_df, rejects_df
        
    except Exception as e:
        logger.error(f"Error applying DQ rules: {str(e)}")
        return df, spark.createDataFrame([], df.schema.add("reject_reason", StringType()))

def apply_business_mapping(df, mapping_rules):
    """Apply business mapping transformations"""
    try:
        if not mapping_rules:
            return df
        
        mapped_df = df
        
        for rule in mapping_rules:
            source_col = rule.get('source_column', '')
            target_col = rule.get('target_column', source_col)
            transformation = rule.get('transformation', 'direct')
            
            if source_col in df.columns:
                if transformation == 'direct':
                    if source_col != target_col:
                        mapped_df = mapped_df.withColumnRenamed(source_col, target_col)
                
                elif transformation == 'upper':
                    mapped_df = mapped_df.withColumn(target_col, upper(col(source_col)))
                
                elif transformation == 'lower':
                    mapped_df = mapped_df.withColumn(target_col, lower(col(source_col)))
                
                elif transformation == 'trim':
                    mapped_df = mapped_df.withColumn(target_col, trim(col(source_col)))
                
                elif transformation == 'date_format':
                    target_format = rule.get('target_format', 'yyyy-MM-dd')
                    source_format = rule.get('source_format', 'yyyy-MM-dd')
                    mapped_df = mapped_df.withColumn(target_col, 
                                                   date_format(to_date(col(source_col), source_format), target_format))
        
        logger.info(f"Applied {len(mapping_rules)} business mapping rules")
        return mapped_df
        
    except Exception as e:
        logger.error(f"Error applying business mapping: {str(e)}")
        return df

def create_glue_table(table_name, s3_path, schema, s3_bucket, aws_region, database_name="default"):
    """Create or update Glue external table"""
    try:
        glue_client = boto3.client('glue', region_name=aws_region)
        
        # Convert Spark schema to Glue columns
        columns = []
        for field in schema.fields:
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
                'Location': s3_path,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
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
        logger.error(f"Failed to create/update Glue table {table_name}: {str(e)}")

def spark_to_glue_type(spark_type):
    """Convert Spark data type to Glue data type"""
    type_mapping = {
        'StringType': 'string',
        'IntegerType': 'int',
        'LongType': 'bigint',
        'DoubleType': 'double',
        'FloatType': 'float',
        'BooleanType': 'boolean',
        'DateType': 'date',
        'TimestampType': 'timestamp'
    }
    return type_mapping.get(type(spark_type).__name__, 'string')

def get_csv_files_from_s3(s3_bucket, prefix="raw/"):
    """Get list of CSV files from S3 bucket"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        
        csv_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].lower().endswith('.csv'):
                    csv_files.append(f"s3://{s3_bucket}/{obj['Key']}")
        
        logger.info(f"Found {len(csv_files)} CSV files in s3://{s3_bucket}/{prefix}")
        return csv_files
    except Exception as e:
        logger.error(f"Error listing S3 objects: {str(e)}")
        return []

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
        dq_rules = read_dq_rules(spark, args.rules_s3)
        
        # Read business mapping
        business_mapping = read_business_mapping(args.s3_bucket, args.aws_region)
        
        # Get CSV files from S3
        csv_files = get_csv_files_from_s3(args.s3_bucket, "raw/")
        
        if not csv_files:
            logger.error("No CSV files found in S3 bucket")
            sys.exit(1)
        
        # Process each CSV file or read all at once
        source_path = f"s3://{args.s3_bucket}/raw/*.csv"
        
        try:
            # Read CSV files
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            logger.info(f"Successfully read data from {source_path}")
            
            # Determine category from file path or use default
            category = "general"  # You might want to extract this from file names
            
            # Apply DQ rules
            clean_df, rejects_df = apply_dq_rules(df, dq_rules)
            
            # Add ingest_date partition column
            clean_df = clean_df.withColumn("ingest_date", lit(args.ingest_date))
            rejects_df = rejects_df.withColumn("ingest_date", lit(args.ingest_date))
            
            # Write clean data to access layer
            access_path = f"s3://{args.s3_bucket}/access/{category}"
            clean_df.write.mode("append").partitionBy("ingest_date").parquet(access_path)
            logger.info(f"Written clean data to {access_path}")
            
            # Write rejects
            rejects_path = f"s3://{args.s3_bucket}/access/{category}/rejects"
            if rejects_df.count() > 0:
                rejects_df.write.mode("append").partitionBy("ingest_date").parquet(rejects_path)
                logger.info(f"Written rejects to {rejects_path}")
            
            # Apply business mapping for mart layer
            mart_df = apply_business_mapping(clean_df, business_mapping)
            
            # Write to mart layer
            mart_path = f"s3://{args.s3_bucket}/mart/{category}"
            mart_df.write.mode("append").partitionBy("ingest_date").parquet(mart_path)
            logger.info(f"Written mart data to {mart_path}")
            
            # Create/Update Glue tables
            create_glue_table(f"access_{category}", access_path, clean_df.schema, args.s3_bucket, args.aws_region)
            create_glue_table(f"mart_{category}", mart_path, mart_df.schema, args.s3_bucket, args.aws_region)
            
            if rejects_df.count() > 0:
                create_glue_table(f"rejects_{category}", rejects_path, rejects_df.schema, args.s3_bucket, args.aws_region)
            
            logger.info("ETL job completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()