"""
AWS Glue PySpark ETL Job - Customer Order Analytics with SCD Type 2

This job processes customer and order data from S3, applies data cleaning,
implements SCD Type 2 tracking using Apache Hudi, and generates aggregated analytics.

TRD Requirements:
- Data Sources: s3://adif-sdlc/sdlc_wizard/customerdata/, orderdata/
- Glue Catalog: gen_ai_poc_databrickscoe
- Tables: sdlc_wizard_customer, sdlc_wizard_order, ordersummary, customeraggregatespend
- SCD Type 2: IsActive, StartDate, EndDate, OpTs columns
- Format: Apache Hudi with upsert operation
- Data Cleaning: Remove NULLs, 'Null' strings, duplicates
"""

import sys
import os
import yaml
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional, List

# Conditional AWS Glue imports (guarded for local testing)
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False
    GlueContext = None
    Job = None

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, sum as _sum,
    count, avg, max as _max, min as _min, coalesce, trim, upper
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, BooleanType, TimestampType
)


def get_job_parameters() -> Dict[str, Any]:
    """
    Get job parameters from AWS Glue arguments or local YAML config.

    Dual-mode parsing:
    1. AWS Glue Runtime: Parse from getResolvedOptions
    2. Local Testing: Load from glue_params.yaml

    Returns:
        Dict containing all job parameters
    """
    # Determine config file path
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config',
        'glue_params.yaml'
    )

    # Load YAML defaults
    params = {}
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            params = yaml.safe_load(f)

    # Override with Glue arguments if available
    if GLUE_AVAILABLE and '--JOB_NAME' in sys.argv:
        try:
            glue_args = getResolvedOptions(sys.argv, [
                'JOB_NAME',
                'customer_source_path',
                'order_source_path',
                'curated_customer_path',
                'curated_order_path',
                'ordersummary_path',
                'customeraggregatespend_path',
                'glue_database',
                'watermark'
            ])

            # Override YAML with Glue arguments
            if 'customer_source_path' in glue_args:
                params['inputs']['customer_source_path'] = glue_args['customer_source_path']
            if 'order_source_path' in glue_args:
                params['inputs']['order_source_path'] = glue_args['order_source_path']
            if 'curated_customer_path' in glue_args:
                params['outputs']['curated_customer_path'] = glue_args['curated_customer_path']
            if 'curated_order_path' in glue_args:
                params['outputs']['curated_order_path'] = glue_args['curated_order_path']
            if 'ordersummary_path' in glue_args:
                params['outputs']['ordersummary_path'] = glue_args['ordersummary_path']
            if 'customeraggregatespend_path' in glue_args:
                params['outputs']['customeraggregatespend_path'] = glue_args['customeraggregatespend_path']
            if 'glue_database' in glue_args:
                params['glue_catalog']['database'] = glue_args['glue_database']
            if 'watermark' in glue_args:
                params['runtime']['watermark'] = glue_args['watermark']

        except Exception as e:
            print(f"Warning: Could not parse Glue arguments: {e}")

    return params


def get_customer_schema() -> StructType:
    """
    Define customer schema based on TRD requirements.

    Schema from TRD:
    - CustId (String) - Primary Key
    - Name (String)
    - EmailId (String)
    - Region (String)

    Returns:
        StructType for customer data
    """
    return StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])


def get_order_schema() -> StructType:
    """
    Define order schema based on TRD requirements.

    Schema from TRD:
    - OrderId (String) - Primary Key
    - CustId (String)
    - ItemName (String)
    - PricePerUnit (Decimal)
    - Qty (Integer)

    Returns:
        StructType for order data
    """
    return StructType([
        StructField("OrderId", StringType(), False),
        StructField("CustId", StringType(), True),
        StructField("ItemName", StringType(), True),
        StructField("PricePerUnit", DecimalType(10, 2), True),
        StructField("Qty", IntegerType(), True)
    ])


def clean_data(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    Clean data according to TRD requirements.

    Cleaning Rules from TRD:
    1. Remove NULL values
    2. Remove 'Null' string values
    3. Remove duplicate records

    Args:
        df: Input DataFrame
        params: Job parameters

    Returns:
        Cleaned DataFrame
    """
    if not params.get('flags', {}).get('enable_data_cleaning', True):
        return df

    cleaned_df = df
    null_patterns = params.get('data_quality', {}).get('null_patterns', ['Null', 'NULL', 'null', 'None', ''])

    # Remove rows with NULL values in key columns
    if params.get('data_quality', {}).get('remove_nulls', True):
        for column in df.columns:
            cleaned_df = cleaned_df.filter(col(column).isNotNull())

    # Remove rows with 'Null' string values
    if params.get('data_quality', {}).get('remove_null_strings', True):
        for column in df.columns:
            if dict(df.dtypes)[column] == 'string':
                for pattern in null_patterns:
                    cleaned_df = cleaned_df.filter(
                        ~(trim(upper(col(column))) == pattern.upper())
                    )

    # Remove duplicate records
    if params.get('data_quality', {}).get('remove_duplicates', True):
        cleaned_df = cleaned_df.dropDuplicates()

    return cleaned_df


def add_scd2_columns(df: DataFrame, params: Dict[str, Any],
                     is_initial_load: bool = True) -> DataFrame:
    """
    Add SCD Type 2 columns as per TRD requirements.

    SCD Type 2 Columns from TRD:
    - IsActive (Boolean)
    - StartDate (Timestamp)
    - EndDate (Timestamp)
    - OpTs (Timestamp)

    Args:
        df: Input DataFrame
        params: Job parameters
        is_initial_load: Whether this is initial load (all records active)

    Returns:
        DataFrame with SCD Type 2 columns
    """
    scd2_config = params.get('scd2', {})

    current_ts = current_timestamp()
    end_date_default = scd2_config.get('end_date_default', '9999-12-31T23:59:59Z')

    df_with_scd2 = df.withColumn(
        scd2_config.get('is_active_column', 'IsActive'),
        lit(scd2_config.get('active_flag_value', True))
    ).withColumn(
        scd2_config.get('start_date_column', 'StartDate'),
        current_ts
    ).withColumn(
        scd2_config.get('end_date_column', 'EndDate'),
        lit(None).cast(TimestampType())
    ).withColumn(
        scd2_config.get('operation_timestamp_column', 'OpTs'),
        current_ts
    )

    return df_with_scd2


def write_to_hudi(df: DataFrame, output_path: str, table_name: str,
                  record_key: str, params: Dict[str, Any]) -> None:
    """
    Write DataFrame to S3 using Hudi format with SCD Type 2 upsert.

    Hudi Configuration from TRD:
    - Format: hudi
    - Operation: upsert
    - Record Key: CustId or OrderId
    - Precombine Field: OpTs

    Args:
        df: DataFrame to write
        output_path: S3 output path
        table_name: Hudi table name
        record_key: Primary key field
        params: Job parameters
    """
    if not params.get('flags', {}).get('enable_hudi', True):
        # Fallback to parquet if Hudi disabled
        df.write.mode(params['outputs']['write_mode']).parquet(output_path)
        return

    hudi_config = params.get('hudi', {})

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': record_key,
        'hoodie.datasource.write.precombine.field': hudi_config.get('precombine_field', 'OpTs'),
        'hoodie.datasource.write.operation': hudi_config.get('operation', 'upsert'),
        'hoodie.datasource.write.table.type': hudi_config.get('table_type', 'COPY_ON_WRITE'),
        'hoodie.datasource.hive_sync.enable': str(hudi_config.get('hive_sync_enabled', True)),
        'hoodie.datasource.hive_sync.database': params['glue_catalog']['database'],
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.hive_sync.partition_fields': hudi_config.get('hive_partition_fields', 'Region'),
        'hoodie.datasource.hive_sync.partition_extractor_class': hudi_config.get(
            'hive_partition_extractor_class',
            'org.apache.hudi.hive.MultiPartKeysValueExtractor'
        ),
        'hoodie.insert.shuffle.parallelism': str(hudi_config.get('insert_shuffle_parallelism', 2)),
        'hoodie.upsert.shuffle.parallelism': str(hudi_config.get('upsert_shuffle_parallelism', 2)),
        'hoodie.bulkinsert.shuffle.parallelism': str(hudi_config.get('bulk_insert_parallelism', 2))
    }

    df.write.format('hudi').options(**hudi_options).mode('append').save(output_path)


def register_glue_table(spark: SparkSession, df: DataFrame,
                       database: str, table_name: str,
                       s3_path: str, params: Dict[str, Any]) -> None:
    """
    Register DataFrame as Glue Catalog table.

    Glue Catalog from TRD:
    - Database: gen_ai_poc_databrickscoe
    - Tables: sdlc_wizard_customer, sdlc_wizard_order, etc.

    Args:
        spark: SparkSession
        df: DataFrame to register
        database: Glue database name
        table_name: Table name
        s3_path: S3 location
        params: Job parameters
    """
    if not params.get('flags', {}).get('enable_glue_catalog', True):
        return

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Drop table if exists (for testing/reprocessing)
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")

    # Register table
    df.write.mode('overwrite').saveAsTable(f"{database}.{table_name}")


def calculate_customer_aggregate_spend(customer_df: DataFrame,
                                       order_df: DataFrame,
                                       params: Dict[str, Any]) -> DataFrame:
    """
    Calculate customer aggregate spend as per TRD requirements.

    Aggregation from TRD:
    - Group by: CustId, Name, Region
    - Metrics: TotalSpend, OrderCount, AvgOrderValue, MaxOrderValue, MinOrderValue

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        params: Job parameters

    Returns:
        Aggregated DataFrame
    """
    if not params.get('flags', {}).get('enable_aggregations', True):
        return None

    # Join customer and order data
    joined_df = order_df.join(
        customer_df.select('CustId', 'Name', 'Region'),
        on='CustId',
        how='inner'
    )

    # Calculate order value
    joined_df = joined_df.withColumn(
        'OrderValue',
        col('PricePerUnit') * col('Qty')
    )

    # Aggregate by customer
    agg_config = params.get('aggregations', {}).get('customeraggregatespend', {})

    aggregate_df = joined_df.groupBy('CustId', 'Name', 'Region').agg(
        _sum('OrderValue').alias('TotalSpend'),
        count('OrderId').alias('OrderCount'),
        avg('OrderValue').alias('AvgOrderValue'),
        _max('OrderValue').alias('MaxOrderValue'),
        _min('OrderValue').alias('MinOrderValue')
    )

    return aggregate_df


def create_order_summary(order_df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    Create order summary as per TRD requirements.

    Args:
        order_df: Order DataFrame
        params: Job parameters

    Returns:
        Order summary DataFrame
    """
    # Group by OrderId and aggregate
    summary_df = order_df.groupBy('OrderId', 'CustId').agg(
        _sum(col('PricePerUnit') * col('Qty')).alias('TotalOrderValue'),
        _sum('Qty').alias('TotalQuantity'),
        count('ItemName').alias('ItemCount')
    )

    return summary_df


def main():
    """
    Main ETL job execution.

    Process Flow:
    1. Initialize Spark/Glue context
    2. Load parameters from YAML/Glue args
    3. Read customer and order data from S3
    4. Clean data (remove NULLs, 'Null' strings, duplicates)
    5. Add SCD Type 2 columns
    6. Write to curated zone with Hudi format
    7. Calculate aggregations
    8. Register tables in Glue Catalog
    """
    # Get job parameters
    params = get_job_parameters()

    # Initialize Spark context
    if GLUE_AVAILABLE and '--JOB_NAME' in sys.argv:
        # AWS Glue runtime
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(params['job']['name'], sys.argv)
    else:
        # Local testing
        spark = SparkSession.builder \
            .appName(params['job']['name']) \
            .config('spark.sql.adaptive.enabled', 'true') \
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
            .getOrCreate()

    try:
        print(f"Starting job: {params['job']['name']}")
        print(f"Log level: {params['job']['log_level']}")

        # Set log level
        spark.sparkContext.setLogLevel(params['job']['log_level'])

        # ===== STEP 1: Read Customer Data =====
        print(f"Reading customer data from: {params['inputs']['customer_source_path']}")
        customer_schema = get_customer_schema()

        customer_raw_df = spark.read \
            .format(params['inputs'].get('customer_format', 'parquet')) \
            .schema(customer_schema) \
            .load(params['inputs']['customer_source_path'])

        print(f"Customer records read: {customer_raw_df.count()}")

        # ===== STEP 2: Read Order Data =====
        print(f"Reading order data from: {params['inputs']['order_source_path']}")
        order_schema = get_order_schema()

        order_raw_df = spark.read \
            .format(params['inputs'].get('order_format', 'parquet')) \
            .schema(order_schema) \
            .load(params['inputs']['order_source_path'])

        print(f"Order records read: {order_raw_df.count()}")

        # ===== STEP 3: Clean Customer Data =====
        print("Cleaning customer data...")
        customer_cleaned_df = clean_data(customer_raw_df, params)
        print(f"Customer records after cleaning: {customer_cleaned_df.count()}")

        # ===== STEP 4: Clean Order Data =====
        print("Cleaning order data...")
        order_cleaned_df = clean_data(order_raw_df, params)
        print(f"Order records after cleaning: {order_cleaned_df.count()}")

        # ===== STEP 5: Add SCD Type 2 Columns to Customer =====
        print("Adding SCD Type 2 columns to customer data...")
        customer_scd2_df = add_scd2_columns(customer_cleaned_df, params)

        # ===== STEP 6: Add SCD Type 2 Columns to Order =====
        print("Adding SCD Type 2 columns to order data...")
        order_scd2_df = add_scd2_columns(order_cleaned_df, params)

        # ===== STEP 7: Write Customer to Curated Zone (Hudi) =====
        print(f"Writing customer data to: {params['outputs']['curated_customer_path']}")
        write_to_hudi(
            customer_scd2_df,
            params['outputs']['curated_customer_path'],
            params['glue_catalog']['customer_table'],
            params['hudi']['customer_recordkey'],
            params
        )

        # ===== STEP 8: Write Order to Curated Zone (Hudi) =====
        print(f"Writing order data to: {params['outputs']['curated_order_path']}")
        write_to_hudi(
            order_scd2_df,
            params['outputs']['curated_order_path'],
            params['glue_catalog']['order_table'],
            params['hudi']['order_recordkey'],
            params
        )

        # ===== STEP 9: Create Order Summary =====
        print("Creating order summary...")
        ordersummary_df = create_order_summary(order_scd2_df, params)
        print(f"Order summary records: {ordersummary_df.count()}")

        # Write order summary
        print(f"Writing order summary to: {params['outputs']['ordersummary_path']}")
        ordersummary_df.write \
            .mode(params['outputs']['write_mode']) \
            .parquet(params['outputs']['ordersummary_path'])

        # ===== STEP 10: Calculate Customer Aggregate Spend =====
        print("Calculating customer aggregate spend...")
        customeraggregatespend_df = calculate_customer_aggregate_spend(
            customer_scd2_df,
            order_scd2_df,
            params
        )

        if customeraggregatespend_df:
            print(f"Customer aggregate records: {customeraggregatespend_df.count()}")

            # Write customer aggregate spend
            print(f"Writing customer aggregate spend to: {params['outputs']['customeraggregatespend_path']}")
            customeraggregatespend_df.write \
                .mode(params['outputs']['write_mode']) \
                .parquet(params['outputs']['customeraggregatespend_path'])

        # ===== STEP 11: Register Tables in Glue Catalog =====
        if params.get('flags', {}).get('enable_glue_catalog', True):
            print("Registering tables in Glue Catalog...")

            register_glue_table(
                spark,
                customer_scd2_df,
                params['glue_catalog']['database'],
                params['glue_catalog']['customer_table'],
                params['outputs']['curated_customer_path'],
                params
            )

            register_glue_table(
                spark,
                order_scd2_df,
                params['glue_catalog']['database'],
                params['glue_catalog']['order_table'],
                params['outputs']['curated_order_path'],
                params
            )

            register_glue_table(
                spark,
                ordersummary_df,
                params['glue_catalog']['database'],
                params['glue_catalog']['ordersummary_table'],
                params['outputs']['ordersummary_path'],
                params
            )

            if customeraggregatespend_df:
                register_glue_table(
                    spark,
                    customeraggregatespend_df,
                    params['glue_catalog']['database'],
                    params['glue_catalog']['customeraggregatespend_table'],
                    params['outputs']['customeraggregatespend_path'],
                    params
                )

        print("Job completed successfully!")

        # Commit job if running in Glue
        if GLUE_AVAILABLE and '--JOB_NAME' in sys.argv:
            job.commit()

    except Exception as e:
        print(f"Job failed with error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    main()