"""
AWS Glue PySpark Job - Customer Order Data Processing

This job implements:
- Customer and Order data ingestion from S3
- Data quality validation and cleansing
- AWS Glue Data Catalog registration
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend calculation
"""

import sys
import os
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, count, lit, current_timestamp,
    when, trim, lower, upper, to_date
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import yaml
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_job_parameters():
    """
    Load job parameters from YAML configuration file.

    Returns:
        dict: Job parameters
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config',
        'glue_params.yaml'
    )

    try:
        with open(config_path, 'r') as f:
            params = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return params
    except FileNotFoundError:
        logger.warning(f"Config file not found at {config_path}, using defaults")
        return get_default_parameters()


def get_default_parameters():
    """
    Provide default parameters when config file is not available.

    Returns:
        dict: Default job parameters
    """
    return {
        'inputs_customer_source_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
        'inputs_customer_source_format': 'csv',
        'inputs_order_source_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
        'inputs_order_source_format': 'csv',
        'outputs_customer_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/customer/',
        'outputs_order_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/order/',
        'outputs_ordersummary_hudi_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
        'outputs_customeraggregatespend_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
        'catalog_database_name': 'gen_ai_poc_databrickscoe',
        'catalog_customer_table_name': 'customer',
        'catalog_order_table_name': 'order',
        'catalog_ordersummary_table_name': 'ordersummary',
        'catalog_customeraggregatespend_table_name': 'customeraggregatespend',
        'dq_remove_nulls': True,
        'dq_remove_null_strings': True,
        'dq_remove_duplicates': True,
        'hudi_table_name': 'ordersummary',
        'hudi_record_key': 'OrderId',
        'hudi_precombine_field': 'OpTs',
        'hudi_partition_path': 'Date',
        'hudi_operation': 'upsert',
        'hudi_table_type': 'COPY_ON_WRITE',
        'scd_enable': True,
        'scd_active_column': 'IsActive',
        'scd_start_date_column': 'StartDate',
        'scd_end_date_column': 'EndDate',
        'scd_operation_timestamp_column': 'OpTs',
        'csv_header': True,
        'csv_infer_schema': True,
        'csv_delimiter': ',',
        'csv_encoding': 'UTF-8',
        'write_mode': 'overwrite',
        'write_format_parquet': 'parquet',
        'write_format_hudi': 'hudi'
    }


def initialize_spark_contexts():
    """
    Initialize Spark and Glue contexts.

    Returns:
        tuple: (SparkContext, GlueContext, SparkSession, Job)
    """
    try:
        # Check if running in Glue environment
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job_name = args['JOB_NAME']
    except Exception:
        job_name = 'local-test-job'
        logger.info("Running in local/test mode")

    # Initialize contexts
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(job_name, {})

    logger.info(f"Initialized Spark contexts for job: {job_name}")
    return sc, glue_context, spark, job


def validate_s3_path(path):
    """
    Validate S3 path format.

    Args:
        path (str): S3 path to validate

    Returns:
        bool: True if valid, False otherwise
    """
    if not path:
        return False
    return path.startswith('s3://') or path.startswith('s3a://')


class DataReader:
    """Helper class for reading data from S3"""

    def __init__(self, spark):
        self.spark = spark

    def read_csv(self, path, header=True, infer_schema=True, delimiter=','):
        """
        Read CSV file from S3.

        Args:
            path (str): S3 path to CSV file
            header (bool): Whether CSV has header
            infer_schema (bool): Whether to infer schema
            delimiter (str): CSV delimiter

        Returns:
            DataFrame: Loaded DataFrame
        """
        logger.info(f"Reading CSV from: {path}")
        return self.spark.read.csv(
            path,
            header=header,
            inferSchema=infer_schema,
            sep=delimiter
        )


class DataWriter:
    """Helper class for writing data to S3"""

    def __init__(self, spark):
        self.spark = spark

    def _write_parquet(self, df, path, mode='overwrite'):
        """Internal method to write Parquet format"""
        df.write.mode(mode).parquet(path)

    def _write_csv(self, df, path, mode='overwrite', header=True):
        """Internal method to write CSV format"""
        df.write.mode(mode).option('header', header).csv(path)

    def _write_hudi(self, df, path, hudi_options):
        """Internal method to write Hudi format"""
        df.write.format('hudi').options(**hudi_options).mode('append').save(path)

    def write_dataframe(self, df, path, format_type='parquet', mode='overwrite', **options):
        """
        Write DataFrame to S3.

        Args:
            df (DataFrame): DataFrame to write
            path (str): S3 destination path
            format_type (str): Output format (parquet, csv, hudi)
            mode (str): Write mode
            **options: Additional write options
        """
        logger.info(f"Writing {format_type} to: {path}")

        if format_type == 'parquet':
            self._write_parquet(df, path, mode)
        elif format_type == 'csv':
            header = options.get('header', True)
            self._write_csv(df, path, mode, header)
        elif format_type == 'hudi':
            hudi_options = options.get('hudi_options', {})
            self._write_hudi(df, path, hudi_options)
        else:
            raise ValueError(f"Unsupported format: {format_type}")


def read_data_safe(spark, path, format_type='csv', **options):
    """
    Safely read data from S3 with validation.

    Args:
        spark (SparkSession): Spark session
        path (str): S3 path
        format_type (str): Data format
        **options: Read options

    Returns:
        DataFrame: Loaded DataFrame or None
    """
    if not validate_s3_path(path):
        logger.error(f"Invalid S3 path: {path}")
        return None

    try:
        reader = DataReader(spark)
        if format_type == 'csv':
            header = options.get('header', True)
            infer_schema = options.get('infer_schema', True)
            delimiter = options.get('delimiter', ',')
            return reader.read_csv(path, header, infer_schema, delimiter)
        else:
            return spark.read.format(format_type).load(path)
    except Exception as e:
        logger.error(f"Error reading from {path}: {str(e)}")
        return None


def write_data_safe(df, path, format_type='parquet', mode='overwrite', **options):
    """
    Safely write data to S3 with validation.

    Args:
        df (DataFrame): DataFrame to write
        path (str): S3 destination path
        format_type (str): Output format
        mode (str): Write mode
        **options: Write options
    """
    if not validate_s3_path(path):
        logger.error(f"Invalid S3 path: {path}")
        return

    if df is None or df.rdd.isEmpty():
        logger.warning(f"Empty DataFrame, skipping write to {path}")
        return

    try:
        writer = DataWriter(df.sql_ctx.sparkSession)
        writer.write_dataframe(df, path, format_type, mode, **options)
        logger.info(f"Successfully wrote data to {path}")
    except Exception as e:
        logger.error(f"Error writing to {path}: {str(e)}")
        raise


def normalize_columns(df):
    """
    Normalize DataFrame column names to lowercase.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: DataFrame with normalized columns
    """
    return df.toDF(*[c.lower() for c in df.columns])


def apply_data_quality(df, params):
    """
    Apply data quality rules to DataFrame.

    Args:
        df (DataFrame): Input DataFrame
        params (dict): Job parameters

    Returns:
        DataFrame: Cleaned DataFrame
    """
    logger.info("Applying data quality rules")

    cleaned_df = df

    # Remove rows with NULL values
    if params.get('dq_remove_nulls', True):
        logger.info("Removing NULL values")
        cleaned_df = cleaned_df.dropna()

    # Remove rows with 'Null' string values
    if params.get('dq_remove_null_strings', True):
        logger.info("Removing 'Null' string values")
        for column in cleaned_df.columns:
            cleaned_df = cleaned_df.filter(
                ~(trim(lower(col(column))) == 'null')
            )

    # Remove duplicate records
    if params.get('dq_remove_duplicates', True):
        logger.info("Removing duplicate records")
        cleaned_df = cleaned_df.dropDuplicates()

    logger.info(f"Data quality applied. Rows: {cleaned_df.count()}")
    return cleaned_df


def ingest_customer_data(spark, params):
    """
    TR-INGEST-001: Load Customer CSV Files from S3

    Args:
        spark (SparkSession): Spark session
        params (dict): Job parameters

    Returns:
        DataFrame: Customer DataFrame
    """
    logger.info("TR-INGEST-001: Ingesting customer data")

    customer_path = params['inputs_customer_source_path']
    customer_df = read_data_safe(
        spark,
        customer_path,
        format_type='csv',
        header=params.get('csv_header', True),
        infer_schema=params.get('csv_infer_schema', True),
        delimiter=params.get('csv_delimiter', ',')
    )

    if customer_df is None:
        raise ValueError(f"Failed to read customer data from {customer_path}")

    # Normalize column names
    customer_df = normalize_columns(customer_df)

    logger.info(f"Customer data ingested. Rows: {customer_df.count()}")
    return customer_df


def ingest_order_data(spark, params):
    """
    TR-INGEST-002: Load Order CSV Files from S3

    Args:
        spark (SparkSession): Spark session
        params (dict): Job parameters

    Returns:
        DataFrame: Order DataFrame
    """
    logger.info("TR-INGEST-002: Ingesting order data")

    order_path = params['inputs_order_source_path']
    order_df = read_data_safe(
        spark,
        order_path,
        format_type='csv',
        header=params.get('csv_header', True),
        infer_schema=params.get('csv_infer_schema', True),
        delimiter=params.get('csv_delimiter', ',')
    )

    if order_df is None:
        raise ValueError(f"Failed to read order data from {order_path}")

    # Normalize column names
    order_df = normalize_columns(order_df)

    logger.info(f"Order data ingested. Rows: {order_df.count()}")
    return order_df


def clean_data(customer_df, order_df, params):
    """
    TR-DQ-001: Apply data quality rules

    Args:
        customer_df (DataFrame): Customer DataFrame
        order_df (DataFrame): Order DataFrame
        params (dict): Job parameters

    Returns:
        tuple: (cleaned_customer_df, cleaned_order_df)
    """
    logger.info("TR-DQ-001: Applying data quality rules")

    cleaned_customer_df = apply_data_quality(customer_df, params)
    cleaned_order_df = apply_data_quality(order_df, params)

    return cleaned_customer_df, cleaned_order_df


def register_catalog_tables(customer_df, order_df, params):
    """
    TR-CATALOG-001: Register cleaned datasets in AWS Glue Data Catalog

    Args:
        customer_df (DataFrame): Cleaned customer DataFrame
        order_df (DataFrame): Cleaned order DataFrame
        params (dict): Job parameters
    """
    logger.info("TR-CATALOG-001: Registering catalog tables")

    database_name = params['catalog_database_name']
    customer_table = params['catalog_customer_table_name']
    order_table = params['catalog_order_table_name']

    # Write customer data to catalog location
    customer_catalog_path = params['outputs_customer_catalog_path']
    write_data_safe(
        customer_df,
        customer_catalog_path,
        format_type='parquet',
        mode=params.get('write_mode', 'overwrite')
    )

    # Write order data to catalog location
    order_catalog_path = params['outputs_order_catalog_path']
    write_data_safe(
        order_df,
        order_catalog_path,
        format_type='parquet',
        mode=params.get('write_mode', 'overwrite')
    )

    # Register as temporary views for catalog integration
    customer_df.createOrReplaceTempView(customer_table)
    order_df.createOrReplaceTempView(order_table)

    logger.info(f"Registered tables in database: {database_name}")


def implement_scd_type2(order_df, customer_df, params):
    """
    TR-INCR-001: Implement SCD Type 2 using Apache Hudi

    Args:
        order_df (DataFrame): Order DataFrame
        customer_df (DataFrame): Customer DataFrame
        params (dict): Job parameters
    """
    logger.info("TR-INCR-001: Implementing SCD Type 2 with Hudi")

    if not params.get('scd_enable', True):
        logger.info("SCD Type 2 disabled, skipping")
        return

    # Add SCD Type 2 columns
    scd_df = order_df.withColumn(
        params['scd_active_column'],
        lit(True)
    ).withColumn(
        params['scd_start_date_column'],
        current_timestamp()
    ).withColumn(
        params['scd_end_date_column'],
        lit(None).cast('timestamp')
    ).withColumn(
        params['scd_operation_timestamp_column'],
        current_timestamp()
    )

    # Prepare Hudi options
    hudi_options = {
        'hoodie.table.name': params['hudi_table_name'],
        'hoodie.datasource.write.recordkey.field': params['hudi_record_key'],
        'hoodie.datasource.write.precombine.field': params['hudi_precombine_field'],
        'hoodie.datasource.write.partitionpath.field': params['hudi_partition_path'],
        'hoodie.datasource.write.operation': params['hudi_operation'],
        'hoodie.datasource.write.table.type': params['hudi_table_type'],
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': params['catalog_database_name'],
        'hoodie.datasource.hive_sync.table': params['catalog_ordersummary_table_name'],
        'hoodie.datasource.hive_sync.partition_fields': params['hudi_partition_path'],
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
    }

    # Write to Hudi
    hudi_path = params['outputs_ordersummary_hudi_path']
    write_data_safe(
        scd_df,
        hudi_path,
        format_type='hudi',
        mode='append',
        hudi_options=hudi_options
    )

    logger.info(f"SCD Type 2 data written to Hudi at {hudi_path}")


def calculate_customer_aggregate_spend(order_df, customer_df, params):
    """
    TR-AGG-001: Calculate customer aggregate spend

    Args:
        order_df (DataFrame): Order DataFrame
        customer_df (DataFrame): Customer DataFrame
        params (dict): Job parameters
    """
    logger.info("TR-AGG-001: Calculating customer aggregate spend")

    # Calculate total amount per order
    order_with_amount = order_df.withColumn(
        'totalamount',
        col('priceperunit') * col('qty')
    )

    # Aggregate by customer
    customer_spend = order_with_amount.groupBy('custid').agg(
        _sum('totalamount').alias('totalspend'),
        count('orderid').alias('ordercount')
    )

    # Join with customer data for enrichment
    customer_aggregate = customer_df.join(
        customer_spend,
        customer_df.custid == customer_spend.custid,
        'inner'
    ).select(
        customer_df.custid,
        customer_df.name,
        customer_df.emailid,
        customer_df.region,
        customer_spend.totalspend,
        customer_spend.ordercount
    )

    # Write to analytics location
    analytics_path = params['outputs_customeraggregatespend_path']
    write_data_safe(
        customer_aggregate,
        analytics_path,
        format_type='parquet',
        mode=params.get('write_mode', 'overwrite')
    )

    logger.info(f"Customer aggregate spend written to {analytics_path}")
    logger.info(f"Total customers with spend: {customer_aggregate.count()}")


def main():
    """
    Main job execution function
    """
    logger.info("Starting AWS Glue PySpark Job")

    # Initialize Spark contexts
    sc, glue_context, spark, job = initialize_spark_contexts()

    try:
        # Load job parameters
        params = get_job_parameters()

        # TR-INGEST-001: Ingest customer data
        customer_df = ingest_customer_data(spark, params)

        # TR-INGEST-002: Ingest order data
        order_df = ingest_order_data(spark, params)

        # TR-DQ-001: Apply data quality rules
        cleaned_customer_df, cleaned_order_df = clean_data(
            customer_df,
            order_df,
            params
        )

        # TR-CATALOG-001: Register catalog tables
        register_catalog_tables(cleaned_customer_df, cleaned_order_df, params)

        # TR-INCR-001: Implement SCD Type 2
        implement_scd_type2(cleaned_order_df, cleaned_customer_df, params)

        # TR-AGG-001: Calculate customer aggregate spend
        calculate_customer_aggregate_spend(
            cleaned_order_df,
            cleaned_customer_df,
            params
        )

        logger.info("Job completed successfully")
        job.commit()

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        # Cleanup
        if job:
            job.commit()


if __name__ == '__main__':
    main()