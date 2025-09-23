import argparse
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, 
    regexp_extract, when, size, split, coalesce,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import *

def setup_logging() -> logging.Logger:
    """Configure logging for the ETL process."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--rules_s3', required=True, help='S3 path to dq_rules.json')
    parser.add_argument('--s3_bucket', required=True, help='S3 bucket name')
    parser.add_argument('--aws_region', required=True, help='AWS region')
    parser.add_argument('--ingest_date', default=datetime.now().strftime('%Y-%m-%d'), 
                       help='Ingest date in YYYY-MM-DD format')
    return parser.parse_args()

def create_spark_session(aws_region: str, s3_bucket: str) -> SparkSession:
    """Create Spark session with S3 and Hive support."""
    return SparkSession.builder \
        .appName("ETL_Data_Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.region", aws_region) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .enableHiveSupport() \
        .getOrCreate()

def read_rules_from_s3(spark: SparkSession, rules_s3_path: str, logger: logging.Logger) -> Dict[str, Any]:
    """Read data quality rules from S3 JSON file."""
    try:
        logger.info(f"Reading DQ rules from: {rules_s3_path}")
        
        # Read JSON file as text
        json_df = spark.read.option("multiline", "true").text(rules_s3_path)
        json_content = json_df.collect()[0][0]
        
        # Parse JSON content
        rules = json.loads(json_content)
        logger.info(f"Successfully loaded DQ rules for {len(rules)} tables")
        return rules
        
    except Exception as e:
        logger.error(f"Error reading DQ rules from S3: {str(e)}")
        raise

def read_business_mapping_from_s3(spark: SparkSession, s3_bucket: str, logger: logging.Logger) -> Dict[str, DataFrame]:
    """Read business mapping from S3. Assumes Excel was converted to CSV format."""
    try:
        mapping_path = f"s3a://{s3_bucket}/config/business_mapping.csv"
        logger.info(f"Reading business mapping from: {mapping_path}")
        
        # Read business mapping CSV
        mapping_df = spark.read.option("header", "true").option("inferSchema", "true").csv(mapping_path)
        
        # Convert to dictionary for easier processing
        # Assuming CSV has columns: source_table, target_table, source_column, target_column, transformation
        mappings = {}
        for row in mapping_df.collect():
            source_table = row['source_table']
            if source_table not in mappings:
                mappings[source_table] = []
            mappings[source_table].append({
                'target_table': row['target_table'],
                'source_column': row['source_column'], 
                'target_column': row['target_column'],
                'transformation': row.get('transformation', None)
            })
        
        logger.info(f"Successfully loaded business mappings for {len(mappings)} source tables")
        return mappings
        
    except Exception as e:
        logger.warning(f"Could not read business mapping from S3: {str(e)}. Proceeding without transformations.")
        return {}

def read_source_data(spark: SparkSession, s3_bucket: str, logger: logging.Logger) -> Dict[str, DataFrame]:
    """Read all CSV files from S3 raw directory."""
    try:
        raw_path = f"s3a://{s3_bucket}/raw/"
        logger.info(f"Reading source data from: {raw_path}")
        
        # Try different delimiters
        delimiters = [',', ';', '|']
        datasets = {}
        
        # Get list of CSV files by reading directory structure
        try:
            # Read all CSV files with wildcard
            for delimiter in delimiters:
                try:
                    df = spark.read \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .option("sep", delimiter) \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .option("multiline", "true") \
                        .csv(f"{raw_path}*.csv")
                    
                    if df.count() > 0:
                        # Add metadata columns
                        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                               .withColumn("source_file_name", regexp_extract(input_file_name(), r"([^/]+)\.csv$", 1)) \
                               .withColumn("record_count", lit(df.count()))
                        
                        # Use source file name as table identifier
                        file_names = df.select("source_file_name").distinct().collect()
                        for row in file_names:
                            table_name = row['source_file_name']
                            if table_name and table_name not in datasets:
                                table_df = df.filter(col("source_file_name") == table_name)
                                datasets[table_name] = table_df
                                logger.info(f"Loaded table {table_name} with {table_df.count()} records")
                        
                        break
                        
                except Exception as e:
                    logger.debug(f"Delimiter '{delimiter}' failed: {str(e)}")
                    continue
            
        except Exception as e:
            logger.warning(f"Error reading with wildcard, trying individual approach: {str(e)}")
        
        if not datasets:
            logger.warning("No CSV files found or readable in raw directory")
            
        return datasets
        
    except Exception as e:
        logger.error(f"Error reading source data: {str(e)}")
        raise

def apply_data_quality(spark: SparkSession, datasets: Dict[str, DataFrame], 
                      rules: Dict[str, Any], logger: logging.Logger) -> Dict[str, Tuple[DataFrame, DataFrame]]:
    """Apply data quality rules and separate clean and reject records."""
    try:
        results = {}
        
        for table_name, df in datasets.items():
            logger.info(f"Applying DQ rules to table: {table_name}")
            
            # Initialize DQ columns
            clean_df = df.withColumn("dq_status", lit("PASS")) \
                        .withColumn("dq_failed_rules", lit("")) \
                        .withColumn("dq_check_timestamp", current_timestamp())
            
            # Apply rules if they exist for this table
            if table_name in rules and "rules" in rules[table_name]:
                table_rules = rules[table_name]["rules"]
                
                for rule in table_rules:
                    rule_type = rule.get("type")
                    column_name = rule.get("column")
                    
                    if column_name not in clean_df.columns:
                        logger.warning(f"Column {column_name} not found in table {table_name}")
                        continue
                    
                    # Apply different rule types
                    if rule_type == "not_null":
                        clean_df = clean_df.withColumn(
                            "dq_status",
                            when(col(column_name).isNull(), lit("FAIL")).otherwise(col("dq_status"))
                        ).withColumn(
                            "dq_failed_rules",
                            when(col(column_name).isNull(), 
                                 coalesce(col("dq_failed_rules") + lit(f";{column_name}_not_null"), 
                                         lit(f"{column_name}_not_null")))
                            .otherwise(col("dq_failed_rules"))
                        )
                    
                    elif rule_type == "range":
                        min_val = rule.get("min", float('-inf'))
                        max_val = rule.get("max", float('inf'))
                        
                        clean_df = clean_df.withColumn(
                            "dq_status",
                            when((col(column_name) < min_val) | (col(column_name) > max_val), lit("FAIL"))
                            .otherwise(col("dq_status"))
                        ).withColumn(
                            "dq_failed_rules",
                            when((col(column_name) < min_val) | (col(column_name) > max_val),
                                 coalesce(col("dq_failed_rules") + lit(f";{column_name}_range"), 
                                         lit(f"{column_name}_range")))
                            .otherwise(col("dq_failed_rules"))
                        )
                    
                    elif rule_type == "pattern":
                        pattern = rule.get("pattern", ".*")
                        
                        clean_df = clean_df.withColumn(
                            "dq_status",
                            when(~col(column_name).rlike(pattern), lit("FAIL"))
                            .otherwise(col("dq_status"))
                        ).withColumn(
                            "dq_failed_rules",
                            when(~col(column_name).rlike(pattern),
                                 coalesce(col("dq_failed_rules") + lit(f";{column_name}_pattern"), 
                                         lit(f"{column_name}_pattern")))
                            .otherwise(col("dq_failed_rules"))
                        )
            
            # Separate clean and reject records
            clean_records = clean_df.filter(col("dq_status") == "PASS")
            reject_records = clean_df.filter(col("dq_status") == "FAIL")
            
            results[table_name] = (clean_records, reject_records)
            
            logger.info(f"Table {table_name}: {clean_records.count()} clean, {reject_records.count()} rejects")
        
        return results
        
    except Exception as e:
        logger.error(f"Error applying data quality rules: {str(e)}")
        raise

def apply_business_transformations(spark: SparkSession, clean_datasets: Dict[str, DataFrame],
                                 business_mappings: Dict[str, Any], logger: logging.Logger) -> Dict[str, DataFrame]:
    """Apply business transformations to create mart data."""
    try:
        mart_datasets = {}
        
        if not business_mappings:
            logger.info("No business mappings found, skipping transformations")
            return mart_datasets
        
        for source_table, mappings in business_mappings.items():
            if source_table not in clean_datasets:
                logger.warning(f"Source table {source_table} not found in clean datasets")
                continue
            
            source_df = clean_datasets[source_table]
            
            # Group mappings by target table
            target_tables = {}
            for mapping in mappings:
                target_table = mapping['target_table']
                if target_table not in target_tables:
                    target_tables[target_table] = []
                target_tables[target_table].append(mapping)
            
            # Create mart tables
            for target_table, table_mappings in target_tables.items():
                logger.info(f"Creating mart table: {target_table}")
                
                # Apply column mappings and transformations
                select_cols = []
                for mapping in table_mappings:
                    source_col = mapping['source_column']
                    target_col = mapping['target_column']
                    transformation = mapping.get('transformation')
                    
                    if source_col in source_df.columns:
                        if transformation:
                            # Apply basic transformations
                            if transformation.lower() == 'upper':
                                select_cols.append(col(source_col).upper().alias(target_col))
                            elif transformation.lower() == 'lower':
                                select_cols.append(col(source_col).lower().alias(target_col))
                            elif transformation.startswith('substring'):
                                # Parse substring(1,10) format
                                params = transformation.replace('substring(', '').replace(')', '').split(',')
                                if len(params) == 2:
                                    start = int(params[0])
                                    length = int(params[1])
                                    select_cols.append(col(source_col).substr(start, length).alias(target_col))
                                else:
                                    select_cols.append(col(source_col).alias(target_col))
                            else:
                                select_cols.append(col(source_col).alias(target_col))
                        else:
                            select_cols.append(col(source_col).alias(target_col))
                
                if select_cols:
                    mart_df = source_df.select(*select_cols)
                    mart_datasets[target_table] = mart_df
                    logger.info(f"Created mart table {target_table} with {mart_df.count()} records")
        
        return mart_datasets
        
    except Exception as e:
        logger.error(f"Error applying business transformations: {str(e)}")
        return {}

def write_to_access_layer(spark: SparkSession, results: Dict[str, Tuple[DataFrame, DataFrame]], 
                         s3_bucket: str, ingest_date: str, logger: logging.Logger) -> None:
    """Write clean data and rejects to access layer."""
    try:
        for table_name, (clean_df, reject_df) in results.items():
            # Write clean data
            clean_path = f"s3a://{s3_bucket}/access/{table_name}/ingest_date={ingest_date}/"
            logger.info(f"Writing clean data for {table_name} to: {clean_path}")
            
            clean_df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(clean_path)
            
            # Write rejects if any
            if reject_df.count() > 0:
                reject_path = f"s3a://{s3_bucket}/access/{table_name}/rejects/ingest_date={ingest_date}/"
                logger.info(f"Writing reject data for {table_name} to: {reject_path}")
                
                reject_df.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(reject_path)
            
            logger.info(f"Successfully wrote {table_name} to access layer")
            
    except Exception as e:
        logger.error(f"Error writing to access layer: {str(e)}")
        raise

def write_to_mart_layer(spark: SparkSession, mart_datasets: Dict[str, DataFrame], 
                       s3_bucket: str, ingest_date: str, logger: logging.Logger) -> None:
    """Write transformed data to mart layer."""
    try:
        if not mart_datasets:
            logger.info("No mart datasets to write")
            return
            
        for table_name, mart_df in mart_datasets.items():
            mart_path = f"s3a://{s3_bucket}/mart/{table_name}/ingest_date={ingest_date}/"
            logger.info(f"Writing mart data for {table_name} to: {mart_path}")
            
            mart_df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(mart_path)
            
            logger.info(f"Successfully wrote {table_name} to mart layer")
            
    except Exception as e:
        logger.error(f"Error writing to mart layer: {str(e)}")
        raise

def create_glue_tables(spark: SparkSession, datasets: Dict[str, DataFrame], 
                      mart_datasets: Dict[str, DataFrame], s3_bucket: str, 
                      logger: logging.Logger) -> None:
    """Create or update AWS Glue external tables."""
    try:
        # Create databases if they don't exist
        spark.sql("CREATE DATABASE IF NOT EXISTS access_db")
        spark.sql("CREATE DATABASE IF NOT EXISTS mart_db")
        
        # Create access layer tables
        for table_name, _ in datasets.items():
            try:
                table_location = f"s3a://{s3_bucket}/access/{table_name}/"
                
                # Drop table if exists
                spark.sql(f"DROP TABLE IF EXISTS access_db.{table_name}")
                
                # Create external table
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS access_db.{table_name}
                USING PARQUET
                OPTIONS (
                  path '{table_location}'
                )
                PARTITIONED BY (ingest_date STRING)
                """
                
                spark.sql(create_table_sql)
                
                # Add partitions
                spark.sql(f"MSCK REPAIR TABLE access_db.{table_name}")
                
                logger.info(f"Created/updated access table: access_db.{table_name}")
                
            except Exception as e:
                logger.error(f"Error creating access table {table_name}: {str(e)}")
        
        # Create mart layer tables
        for table_name, _ in mart_datasets.items():
            try:
                table_location = f"s3a://{s3_bucket}/mart/{table_name}/"
                
                # Drop table if exists
                spark.sql(f"DROP TABLE IF EXISTS mart_db.{table_name}")
                
                # Create external table
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS mart_db.{table_name}
                USING PARQUET
                OPTIONS (
                  path '{table_location}'
                )
                PARTITIONED BY (ingest_date STRING)
                """
                
                spark.sql(create_table_sql)
                
                # Add partitions
                spark.sql(f"MSCK REPAIR TABLE mart_db.{table_name}")
                
                logger.info(f"Created/updated mart table: mart_db.{table_name}")
                
            except Exception as e:
                logger.error(f"Error creating mart table {table_name}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error creating Glue tables: {str(e)}")
        raise

def main() -> None:
    """Main ETL orchestration function."""
    logger = setup_logging()
    logger.info("Starting ETL Pipeline")
    
    try:
        # Parse arguments
        args = parse_arguments()
        logger.info(f"Arguments: bucket={args.s3_bucket}, region={args.aws_region}, date={args.ingest_date}")
        
        # Create Spark session
        spark = create_spark_session(args.aws_region, args.s3_bucket)
        logger.info("Spark session created successfully")
        
        # Read DQ rules
        dq_rules = read_rules_from_s3(spark, args.rules_s3, logger)
        
        # Read business mappings
        business_mappings = read_business_mapping_from_s3(spark, args.s3_bucket, logger)
        
        # Read source data
        source_datasets = read_source_data(spark, args.s3_bucket, logger)
        
        if not source_datasets:
            logger.warning("No source datasets found. Exiting.")
            return
        
        # Apply data quality rules
        dq_results = apply_data_quality(spark, source_datasets, dq_rules, logger)
        
        # Extract clean datasets for business transformations
        clean_datasets = {table: clean_df for table, (clean_df, _) in dq_results.items()}
        
        # Apply business transformations
        mart_datasets = apply_business_transformations(spark, clean_datasets, business_mappings, logger)
        
        # Write to access layer
        write_to_access_layer(spark, dq_results, args.s3_bucket, args.ingest_date, logger)
        
        # Write to mart layer
        write_to_mart_layer(spark, mart_datasets, args.s3_bucket, args.ingest_date, logger)
        
        # Create Glue tables
        create_glue_tables(spark, source_datasets, mart_datasets, args.s3_bucket, logger)
        
        # Log final statistics
        total_clean = sum(clean_df.count() for clean_df, _ in dq_results.values())
        total_rejects = sum(reject_df.count() for _, reject_df in dq_results.values())
        total_mart = sum(df.count() for df in mart_datasets.values()) if mart_datasets else 0
        
        logger.info(f"ETL Pipeline completed successfully!")
        logger.info(f"Statistics: {total_clean} clean records, {total_rejects} rejects, {total_mart} mart records")
        
        spark.stop()
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()