"""
Customer Order Analytics ETL - AWS Glue PySpark Job
Implements complete ETL pipeline for customer-order analytics
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, concat, lit, initcap, trim, count,
    current_timestamp, to_date, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType
)
import yaml


def initialize_spark_contexts():
    """
    Initialize Spark session with AWS Glue configurations.
    Returns SparkSession and GlueContext if available.
    """
    try:
        from awsglue.context import GlueContext
        from awsglue.utils import getResolvedOptions

        spark = SparkSession.builder \
            .appName("CustomerOrderAnalyticsETL") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()

        glue_context = GlueContext(spark.sparkContext)

        return spark, glue_context
    except ImportError:
        # Running outside Glue environment (local/testing)
        spark = SparkSession.builder \
            .appName("CustomerOrderAnalyticsETL") \
            .master("local[*]") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()

        return spark, None


def get_job_parameters():
    """
    Load job parameters from YAML configuration file.
    Returns dictionary of parameters.
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config',
        'glue_params.yaml'
    )

    if not os.path.exists(config_path):
        # Fallback for Glue environment
        config_path = '/tmp/glue_params.yaml'

    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            params = yaml.safe_load(f)
        return params
    else:
        # Default parameters if config not found
        return {
            'inputs_customers_path': 's3://adif-sdlc/busi_req_doc/customers.csv',
            'inputs_customers_format': 'csv',
            'inputs_orders_path': 's3://adif-sdlc/busi_req_doc/orders.csv',
            'inputs_orders_format': 'csv',
            'outputs_target_path': 's3://adif-sdlc/output/customer_orders/',
            'outputs_target_format': 'parquet',
            'outputs_partition_by': 'processed_date',
            'error_log_base_path': 's3://adif-sdlc/logs/errors/',
            'error_log_customers_path': 's3://adif-sdlc/logs/errors/customers/',
            'error_log_orders_path': 's3://adif-sdlc/logs/errors/orders/',
            'error_log_format': 'csv',
            'csv_header': True,
            'csv_infer_schema': False,
            'parquet_compression': 'snappy',
            'parquet_mode': 'overwrite'
        }


class S3DataReader:
    """Helper class for reading data from S3 with validation"""

    def __init__(self, spark):
        self.spark = spark

    def _read_csv(self, path, schema=None):
        """Internal method to read CSV files"""
        reader = self.spark.read.format('csv') \
            .option('header', 'true') \
            .option('delimiter', ',') \
            .option('quote', '"') \
            .option('escape', '\\')

        if schema:
            reader = reader.schema(schema)

        return reader.load(path)

    def read_data_safe(self, path, format_type='csv', schema=None):
        """
        Safely read data from S3 with error handling.
        Returns DataFrame or None if read fails.
        """
        try:
            if format_type == 'csv':
                df = self._read_csv(path, schema)
            else:
                df = self.spark.read.format(format_type).load(path)

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            record_count = df.count()
            print(f"Successfully read {record_count} records from {path}")

            return df
        except Exception as e:
            print(f"Error reading data from {path}: {str(e)}")
            return None


class S3DataWriter:
    """Helper class for writing data to S3"""

    def __init__(self, spark):
        self.spark = spark

    def _write_parquet(self, df, path, mode='overwrite', partition_by=None):
        """Internal method to write Parquet files"""
        writer = df.write.format('parquet') \
            .option('compression', 'snappy') \
            .mode(mode)

        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.save(path)

    def _write_csv(self, df, path, mode='overwrite'):
        """Internal method to write CSV files"""
        df.write.format('csv') \
            .option('header', 'true') \
            .mode(mode) \
            .save(path)

    def write_data_safe(self, df, path, format_type='parquet', mode='overwrite', partition_by=None):
        """
        Safely write data to S3 with error handling.
        Returns True if successful, False otherwise.
        """
        try:
            if format_type == 'parquet':
                self._write_parquet(df, path, mode, partition_by)
            elif format_type == 'csv':
                self._write_csv(df, path, mode)
            else:
                df.write.format(format_type).mode(mode).save(path)

            print(f"Successfully wrote data to {path}")
            return True
        except Exception as e:
            print(f"Error writing data to {path}: {str(e)}")
            return False


def get_customer_schema():
    """Define customer data schema"""
    return StructType([
        StructField("cid", IntegerType(), False),
        StructField("fname", StringType(), False),
        StructField("lname", StringType(), False)
    ])


def get_order_schema():
    """Define order data schema"""
    return StructType([
        StructField("oid", IntegerType(), False),
        StructField("cid", IntegerType(), False),
        StructField("oname", StringType(), False)
    ])


def extract_customers(spark, params):
    """
    TR-EXTRACT-001: Load customer master data from S3 CSV
    """
    print("Starting customer data extraction...")

    reader = S3DataReader(spark)
    customers_path = params['inputs_customers_path']
    schema = get_customer_schema()

    df = reader.read_data_safe(customers_path, 'csv', schema)

    if df is None:
        raise Exception("Failed to extract customer data")

    return df


def extract_orders(spark, params):
    """
    TR-EXTRACT-002: Load order transaction data from S3 CSV
    """
    print("Starting order data extraction...")

    reader = S3DataReader(spark)
    orders_path = params['inputs_orders_path']
    schema = get_order_schema()

    df = reader.read_data_safe(orders_path, 'csv', schema)

    if df is None:
        raise Exception("Failed to extract order data")

    return df


def validate_customers(spark, df_customers, params):
    """
    TR-VALIDATE-001: Validate customer data quality
    - Check for nulls
    - Check for invalid data types
    - Remove duplicates
    - Log errors
    """
    print("Starting customer data validation...")

    # Identify invalid records (nulls or empty strings)
    df_invalid = df_customers.filter(
        col("cid").isNull() |
        (col("cid") <= 0) |
        col("fname").isNull() |
        (trim(col("fname")) == "") |
        col("lname").isNull() |
        (trim(col("lname")) == "")
    )

    invalid_count = df_invalid.count()
    if invalid_count > 0:
        print(f"Found {invalid_count} invalid customer records")

        # Log errors to S3
        error_path = params['error_log_customers_path']
        run_date = datetime.now().strftime('%Y-%m-%d')
        error_full_path = f"{error_path}run_date={run_date}/"

        writer = S3DataWriter(spark)
        writer.write_data_safe(df_invalid, error_full_path, 'csv', 'append')

    # Filter valid records
    df_valid = df_customers.filter(
        col("cid").isNotNull() &
        (col("cid") > 0) &
        col("fname").isNotNull() &
        (trim(col("fname")) != "") &
        col("lname").isNotNull() &
        (trim(col("lname")) != "")
    )

    # Remove duplicates based on CID
    df_valid = df_valid.dropDuplicates(["cid"])

    valid_count = df_valid.count()
    print(f"Customer validation complete: {valid_count} valid records, {invalid_count} invalid records")

    return df_valid


def validate_orders(spark, df_orders, params):
    """
    TR-VALIDATE-002: Validate order data quality
    - Check for nulls
    - Check for invalid data types
    - Remove duplicates
    - Log errors
    """
    print("Starting order data validation...")

    # Identify invalid records
    df_invalid = df_orders.filter(
        col("oid").isNull() |
        (col("oid") <= 0) |
        col("cid").isNull() |
        (col("cid") <= 0) |
        col("oname").isNull() |
        (trim(col("oname")) == "")
    )

    invalid_count = df_invalid.count()
    if invalid_count > 0:
        print(f"Found {invalid_count} invalid order records")

        # Log errors to S3
        error_path = params['error_log_orders_path']
        run_date = datetime.now().strftime('%Y-%m-%d')
        error_full_path = f"{error_path}run_date={run_date}/"

        writer = S3DataWriter(spark)
        writer.write_data_safe(df_invalid, error_full_path, 'csv', 'append')

    # Filter valid records
    df_valid = df_orders.filter(
        col("oid").isNotNull() &
        (col("oid") > 0) &
        col("cid").isNotNull() &
        (col("cid") > 0) &
        col("oname").isNotNull() &
        (trim(col("oname")) != "")
    )

    # Remove duplicates based on OID
    df_valid = df_valid.dropDuplicates(["oid"])

    valid_count = df_valid.count()
    print(f"Order validation complete: {valid_count} valid records, {invalid_count} invalid records")

    return df_valid


def transform_derive_full_name(df_customers):
    """
    TR-TRANSFORM-001: Derive FULL_NAME column
    Concatenate FNAME and LNAME with title case formatting
    """
    print("Deriving FULL_NAME column...")

    df_transformed = df_customers.withColumn(
        "full_name",
        concat(
            initcap(trim(col("fname"))),
            lit(" "),
            initcap(trim(col("lname")))
        )
    )

    return df_transformed


def transform_join_customer_orders(df_customers, df_orders):
    """
    TR-TRANSFORM-002: Join customers and orders on CID
    Perform inner join to create integrated dataset
    """
    print("Joining customer and order data...")

    df_joined = df_customers.join(
        df_orders,
        df_customers["cid"] == df_orders["cid"],
        "inner"
    ).select(
        df_customers["cid"],
        df_customers["fname"],
        df_customers["lname"],
        df_customers["full_name"],
        df_orders["oid"],
        df_orders["oname"]
    )

    join_count = df_joined.count()
    print(f"Join complete: {join_count} customer-order records")

    return df_joined


def transform_calculate_order_count(df_joined):
    """
    TR-TRANSFORM-003: Calculate ORDER_COUNT per customer
    Use window function partitioned by CID
    """
    print("Calculating order counts per customer...")

    window_spec = Window.partitionBy("cid")

    df_with_count = df_joined.withColumn(
        "order_count",
        count("oid").over(window_spec)
    )

    return df_with_count


def transform_add_processed_date(df):
    """
    TR-TRANSFORM-004: Add PROCESSED_DATE column
    Add current timestamp for audit trail
    """
    print("Adding processed date timestamp...")

    df_with_date = df.withColumn(
        "processed_date",
        to_date(current_timestamp())
    )

    return df_with_date


def load_customer_orders(spark, df_final, params):
    """
    TR-LOAD-001: Write final dataset to S3 Parquet
    Partition by PROCESSED_DATE with overwrite mode
    """
    print("Loading final dataset to S3...")

    target_path = params['outputs_target_path']
    partition_by = params.get('outputs_partition_by', 'processed_date')

    writer = S3DataWriter(spark)
    success = writer.write_data_safe(
        df_final,
        target_path,
        'parquet',
        'overwrite',
        partition_by
    )

    if not success:
        raise Exception("Failed to load data to S3")

    final_count = df_final.count()
    print(f"Successfully loaded {final_count} records to {target_path}")

    return success


def main():
    """
    Main ETL orchestration function
    Implements complete customer-order analytics pipeline
    """
    print("=" * 80)
    print("Customer Order Analytics ETL - Starting")
    print("=" * 80)

    # Initialize Spark
    spark, glue_context = initialize_spark_contexts()

    try:
        # Load configuration
        params = get_job_parameters()
        print(f"Loaded configuration parameters")

        # EXTRACT
        df_customers_raw = extract_customers(spark, params)
        df_orders_raw = extract_orders(spark, params)

        # VALIDATE
        df_customers_valid = validate_customers(spark, df_customers_raw, params)
        df_orders_valid = validate_orders(spark, df_orders_raw, params)

        # TRANSFORM
        df_customers_transformed = transform_derive_full_name(df_customers_valid)
        df_joined = transform_join_customer_orders(df_customers_transformed, df_orders_valid)
        df_with_count = transform_calculate_order_count(df_joined)
        df_final = transform_add_processed_date(df_with_count)

        # LOAD
        load_customer_orders(spark, df_final, params)

        print("=" * 80)
        print("Customer Order Analytics ETL - Completed Successfully")
        print("=" * 80)

    except Exception as e:
        print(f"ERROR: ETL job failed - {str(e)}")
        raise
    finally:
        # Don't stop spark in Glue environment
        if glue_context is None:
            spark.stop()


if __name__ == "__main__":
    main()