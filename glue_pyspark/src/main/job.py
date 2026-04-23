"""
AWS Glue PySpark ETL Job - Customer Order Summary with SCD Type 2

This module implements the complete ETL pipeline as per FRD/TRD requirements:
- FR-INGEST-001: Data ingestion from S3
- FR-CLEAN-001: Data cleaning (NULLs, 'Null' strings, duplicates)
- FR-SCD2-001: SCD Type 2 implementation with Hudi
- FR-AGG-001: Customer spending aggregation
- FR-CATALOG-001: Glue Catalog registration

Technical Requirements (TRD):
- Customer Data: s3://adif-sdlc/sdlc_wizard/customerdata/*
- Order Data: s3://adif-sdlc/sdlc_wizard/orderdata/*
- Output: s3://adif-sdlc/analytics/customeraggregatespend/
- Output: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
- SCD Type 2 columns: IsActive, StartDate, EndDate, OpTs
- Format: Apache Hudi with upsert operation
"""

import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, lit, current_timestamp,
    to_timestamp, when, trim, lower, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_spark_contexts() -> Tuple[SparkContext, GlueContext, SparkSession]:
    """
    Initialize Spark, Glue, and SparkSession contexts.

    Returns:
        Tuple[SparkContext, GlueContext, SparkSession]: Initialized contexts

    Raises:
        RuntimeError: If context initialization fails
    """
    try:
        logger.info("Initializing Spark contexts...")
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session

        # Configure Spark for optimal performance
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        logger.info("Spark contexts initialized successfully")
        return sc, glue_context, spark

    except Exception as e:
        logger.error(f"Failed to initialize Spark contexts: {str(e)}")
        raise RuntimeError(f"Context initialization failed: {str(e)}")


def validate_s3_path(s3_path: str) -> bool:
    """
    Validate S3 path exists and is accessible.

    Args:
        s3_path: S3 path to validate (s3://bucket/key)

    Returns:
        bool: True if path is valid and accessible

    Raises:
        ValueError: If S3 path format is invalid
    """
    try:
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")

        # Parse S3 path
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''

        # Validate bucket access
        s3_client = boto3.client('s3')
        s3_client.head_bucket(Bucket=bucket)

        logger.info(f"S3 path validated: {s3_path}")
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.error(f"S3 bucket not found: {bucket}")
        elif error_code == '403':
            logger.error(f"Access denied to S3 bucket: {bucket}")
        else:
            logger.error(f"S3 validation error: {str(e)}")
        return False

    except Exception as e:
        logger.error(f"Unexpected error validating S3 path: {str(e)}")
        return False


def read_data_safe(
    spark: SparkSession,
    s3_path: str,
    file_format: str = "parquet",
    schema: Optional[StructType] = None
) -> Optional[DataFrame]:
    """
    Safely read data from S3 with validation and error handling.

    Args:
        spark: SparkSession instance
        s3_path: S3 path to read from
        file_format: File format (parquet, csv, json)
        schema: Optional schema to enforce

    Returns:
        DataFrame or None if read fails

    Implementation of TR-INGEST-001
    """
    try:
        logger.info(f"Reading data from: {s3_path}")

        # Validate S3 path
        if not validate_s3_path(s3_path):
            logger.error(f"S3 path validation failed: {s3_path}")
            return None

        # Read data with schema if provided
        if schema:
            df = spark.read.format(file_format).schema(schema).load(s3_path)
        else:
            df = spark.read.format(file_format).load(s3_path)

        # Normalize column names to lowercase
        df = df.toDF(*[c.lower() for c in df.columns])

        record_count = df.count()
        logger.info(f"Successfully read {record_count} records from {s3_path}")

        return df

    except Exception as e:
        logger.error(f"Failed to read data from {s3_path}: {str(e)}")
        return None


def write_data_safe(
    df: DataFrame,
    s3_path: str,
    file_format: str = "parquet",
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    hudi_options: Optional[Dict[str, str]] = None
) -> bool:
    """
    Safely write data to S3 with validation and error handling.

    Args:
        df: DataFrame to write
        s3_path: S3 destination path
        file_format: Output format (parquet, hudi)
        mode: Write mode (overwrite, append)
        partition_by: Columns to partition by
        hudi_options: Hudi-specific options for SCD Type 2

    Returns:
        bool: True if write successful

    Implementation of TR-SCD2-001 (Hudi write)
    """
    try:
        logger.info(f"Writing data to: {s3_path}")

        # Validate S3 path format
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")

        writer = df.write.mode(mode)

        # Apply partitioning if specified
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        # Handle Hudi format with SCD Type 2
        if file_format == "hudi" and hudi_options:
            logger.info("Writing with Hudi format for SCD Type 2")
            for key, value in hudi_options.items():
                writer = writer.option(key, value)
            writer.format("hudi").save(s3_path)
        else:
            writer.format(file_format).save(s3_path)

        logger.info(f"Successfully wrote data to {s3_path}")
        return True

    except Exception as e:
        logger.error(f"Failed to write data to {s3_path}: {str(e)}")
        return False


class CustomerOrderETLJob:
    """
    Main ETL Job class implementing all FRD/TRD requirements.

    Functional Requirements:
    - FR-INGEST-001: Data ingestion
    - FR-CLEAN-001: Data cleaning
    - FR-SCD2-001: SCD Type 2 implementation
    - FR-AGG-001: Aggregation
    - FR-CATALOG-001: Catalog registration
    """

    # TRD: S3 Paths
    CUSTOMER_INPUT_PATH = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    ORDER_INPUT_PATH = "s3://adif-sdlc/sdlc_wizard/orderdata/"
    AGGREGATE_OUTPUT_PATH = "s3://adif-sdlc/analytics/customeraggregatespend/"
    CURATED_OUTPUT_PATH = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
    ATHENA_RESULTS_PATH = "s3://adif-sdlc/athena-results/"

    # TRD: Glue Catalog
    GLUE_DATABASE = "sdlc_wizard_db"
    CUSTOMER_AGGREGATE_TABLE = "customer_aggregate_spend"
    ORDER_SUMMARY_TABLE = "order_summary_curated"

    # TRD: Customer Schema
    CUSTOMER_SCHEMA = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("EmailId", StringType(), False),
        StructField("Region", StringType(), False)
    ])

    # TRD: Order Schema
    ORDER_SCHEMA = StructType([
        StructField("OrderId", StringType(), False),
        StructField("ItemName", StringType(), False),
        StructField("PricePerUnit", DoubleType(), False),
        StructField("Qty", IntegerType(), False),
        StructField("Date", StringType(), False),
        StructField("CustId", StringType(), False)
    ])

    def __init__(self, spark: SparkSession, glue_context: GlueContext):
        """
        Initialize ETL job with Spark and Glue contexts.

        Args:
            spark: SparkSession instance
            glue_context: GlueContext instance
        """
        self.spark = spark
        self.glue_context = glue_context
        self.job_start_time = datetime.now()
        logger.info("CustomerOrderETLJob initialized")

    def ingest_customer_data(self) -> Optional[DataFrame]:
        """
        Ingest customer data from S3.

        Implementation of FR-INGEST-001 (Customer Data)
        TRD: s3://adif-sdlc/sdlc_wizard/customerdata/*

        Returns:
            DataFrame with customer data or None
        """
        logger.info("FR-INGEST-001: Ingesting customer data")

        df = read_data_safe(
            self.spark,
            self.CUSTOMER_INPUT_PATH,
            file_format="parquet",
            schema=self.CUSTOMER_SCHEMA
        )

        if df:
            logger.info(f"Customer data ingested: {df.count()} records")

        return df

    def ingest_order_data(self) -> Optional[DataFrame]:
        """
        Ingest order data from S3.

        Implementation of FR-INGEST-001 (Order Data)
        TRD: s3://adif-sdlc/sdlc_wizard/orderdata/*

        Returns:
            DataFrame with order data or None
        """
        logger.info("FR-INGEST-001: Ingesting order data")

        df = read_data_safe(
            self.spark,
            self.ORDER_INPUT_PATH,
            file_format="parquet",
            schema=self.ORDER_SCHEMA
        )

        if df:
            logger.info(f"Order data ingested: {df.count()} records")

        return df

    def clean_data(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Clean data by removing NULLs, 'Null' strings, and duplicates.

        Implementation of FR-CLEAN-001
        TRD Requirements:
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records

        Args:
            df: Input DataFrame
            dataset_name: Name of dataset for logging

        Returns:
            Cleaned DataFrame
        """
        logger.info(f"FR-CLEAN-001: Cleaning {dataset_name} data")

        initial_count = df.count()

        # Step 1: Remove rows with NULL values in any column
        df_no_nulls = df.na.drop()
        null_removed = initial_count - df_no_nulls.count()
        logger.info(f"Removed {null_removed} rows with NULL values")

        # Step 2: Remove rows with 'Null' string values
        # TRD: Remove 'Null' as string value
        null_string_values = ['Null', 'NULL', 'null', 'None', 'NONE', '']

        for column in df_no_nulls.columns:
            for null_val in null_string_values:
                df_no_nulls = df_no_nulls.filter(col(column) != null_val)

        string_null_removed = df_no_nulls.count()
        logger.info(f"Removed rows with 'Null' string values: {initial_count - null_removed - string_null_removed}")

        # Step 3: Remove duplicate records
        df_clean = df_no_nulls.dropDuplicates()
        duplicates_removed = string_null_removed - df_clean.count()
        logger.info(f"Removed {duplicates_removed} duplicate records")

        final_count = df_clean.count()
        logger.info(f"Data cleaning complete: {initial_count} -> {final_count} records")

        return df_clean

    def add_scd2_columns(self, df: DataFrame) -> DataFrame:
        """
        Add SCD Type 2 columns to DataFrame.

        Implementation of FR-SCD2-001
        TRD: Add columns: IsActive, StartDate, EndDate, OpTs

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with SCD Type 2 columns
        """
        logger.info("FR-SCD2-001: Adding SCD Type 2 columns")

        # TRD: SCD Type 2 columns
        df_scd2 = df.withColumn("isactive", lit(True).cast(BooleanType())) \
                    .withColumn("startdate", current_timestamp().cast(TimestampType())) \
                    .withColumn("enddate", to_timestamp(lit("9999-12-31 23:59:59"))) \
                    .withColumn("opts", current_timestamp().cast(TimestampType()))

        logger.info("SCD Type 2 columns added: isactive, startdate, enddate, opts")

        return df_scd2

    def aggregate_customer_spend(
        self,
        customer_df: DataFrame,
        order_df: DataFrame
    ) -> Optional[DataFrame]:
        """
        Aggregate customer spending data.

        Implementation of FR-AGG-001
        TRD: Calculate total spend per customer by joining customer and order data

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Aggregated DataFrame or None
        """
        logger.info("FR-AGG-001: Aggregating customer spend")

        try:
            # Calculate total amount per order
            order_with_total = order_df.withColumn(
                "totalamount",
                col("priceperunit") * col("qty")
            )

            # Aggregate by customer
            customer_spend = order_with_total.groupBy("custid").agg(
                _sum("totalamount").alias("totalspend"),
                count("orderid").alias("ordercount")
            )

            # Join with customer data to get customer details
            result = customer_df.join(
                customer_spend,
                customer_df.custid == customer_spend.custid,
                "inner"
            ).select(
                customer_df.custid,
                customer_df.name,
                customer_df.emailid,
                customer_df.region,
                customer_spend.totalspend,
                customer_spend.ordercount
            )

            logger.info(f"Customer spend aggregated: {result.count()} customers")

            return result

        except Exception as e:
            logger.error(f"Failed to aggregate customer spend: {str(e)}")
            return None

    def write_customer_aggregate(self, df: DataFrame) -> bool:
        """
        Write customer aggregate data with SCD Type 2 using Hudi.

        Implementation of FR-SCD2-001 + FR-AGG-001
        TRD: s3://adif-sdlc/analytics/customeraggregatespend/
        TRD: Use Hudi format with upsert operation

        Args:
            df: Aggregated customer DataFrame

        Returns:
            bool: True if write successful
        """
        logger.info("Writing customer aggregate data with Hudi SCD Type 2")

        # Add SCD Type 2 columns
        df_scd2 = self.add_scd2_columns(df)

        # TRD: Hudi configuration for SCD Type 2
        hudi_options = {
            'hoodie.table.name': self.CUSTOMER_AGGREGATE_TABLE,
            'hoodie.datasource.write.recordkey.field': 'custid',
            'hoodie.datasource.write.precombine.field': 'opts',
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.partitionpath.field': 'region',
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': self.GLUE_DATABASE,
            'hoodie.datasource.hive_sync.table': self.CUSTOMER_AGGREGATE_TABLE,
            'hoodie.datasource.hive_sync.partition_fields': 'region',
            'hoodie.datasource.hive_sync.partition_extractor_class':
                'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.hive_sync.mode': 'hms'
        }

        return write_data_safe(
            df_scd2,
            self.AGGREGATE_OUTPUT_PATH,
            file_format="hudi",
            mode="append",
            partition_by=["region"],
            hudi_options=hudi_options
        )

    def write_order_summary(self, order_df: DataFrame) -> bool:
        """
        Write curated order summary data.

        Implementation of FR-SCD2-001
        TRD: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/

        Args:
            order_df: Order DataFrame

        Returns:
            bool: True if write successful
        """
        logger.info("Writing curated order summary")

        # Add SCD Type 2 columns
        df_scd2 = self.add_scd2_columns(order_df)

        # TRD: Hudi configuration for order summary
        hudi_options = {
            'hoodie.table.name': self.ORDER_SUMMARY_TABLE,
            'hoodie.datasource.write.recordkey.field': 'orderid',
            'hoodie.datasource.write.precombine.field': 'opts',
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.partitionpath.field': 'date',
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': self.GLUE_DATABASE,
            'hoodie.datasource.hive_sync.table': self.ORDER_SUMMARY_TABLE,
            'hoodie.datasource.hive_sync.partition_fields': 'date',
            'hoodie.datasource.hive_sync.partition_extractor_class':
                'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.hive_sync.mode': 'hms'
        }

        return write_data_safe(
            df_scd2,
            self.CURATED_OUTPUT_PATH,
            file_format="hudi",
            mode="append",
            partition_by=["date"],
            hudi_options=hudi_options
        )

    def register_glue_catalog_tables(self) -> bool:
        """
        Register tables in AWS Glue Catalog.

        Implementation of FR-CATALOG-001
        TRD: Database: sdlc_wizard_db

        Returns:
            bool: True if registration successful
        """
        logger.info("FR-CATALOG-001: Registering tables in Glue Catalog")

        try:
            glue_client = boto3.client('glue')

            # Create database if not exists
            try:
                glue_client.create_database(
                    DatabaseInput={
                        'Name': self.GLUE_DATABASE,
                        'Description': 'SDLC Wizard database for customer and order data'
                    }
                )
                logger.info(f"Created Glue database: {self.GLUE_DATABASE}")
            except glue_client.exceptions.AlreadyExistsException:
                logger.info(f"Glue database already exists: {self.GLUE_DATABASE}")

            # Tables are auto-registered by Hudi with hive_sync enabled
            logger.info("Tables registered via Hudi Hive sync")

            return True

        except Exception as e:
            logger.error(f"Failed to register Glue Catalog tables: {str(e)}")
            return False

    def run(self) -> bool:
        """
        Execute the complete ETL pipeline.

        Pipeline Steps:
        1. FR-INGEST-001: Ingest customer and order data
        2. FR-CLEAN-001: Clean data
        3. FR-AGG-001: Aggregate customer spend
        4. FR-SCD2-001: Write with SCD Type 2 (Hudi)
        5. FR-CATALOG-001: Register in Glue Catalog

        Returns:
            bool: True if pipeline completes successfully
        """
        logger.info("=" * 80)
        logger.info("Starting Customer Order ETL Job with SCD Type 2")
        logger.info("=" * 80)

        try:
            # Step 1: FR-INGEST-001 - Ingest customer data
            customer_df = self.ingest_customer_data()
            if customer_df is None:
                logger.error("Failed to ingest customer data")
                return False

            # Step 2: FR-INGEST-001 - Ingest order data
            order_df = self.ingest_order_data()
            if order_df is None:
                logger.error("Failed to ingest order data")
                return False

            # Step 3: FR-CLEAN-001 - Clean customer data
            customer_clean = self.clean_data(customer_df, "customer")

            # Step 4: FR-CLEAN-001 - Clean order data
            order_clean = self.clean_data(order_df, "order")

            # Step 5: FR-AGG-001 - Aggregate customer spend
            customer_aggregate = self.aggregate_customer_spend(
                customer_clean,
                order_clean
            )
            if customer_aggregate is None:
                logger.error("Failed to aggregate customer spend")
                return False

            # Step 6: FR-SCD2-001 - Write customer aggregate with Hudi
            if not self.write_customer_aggregate(customer_aggregate):
                logger.error("Failed to write customer aggregate data")
                return False

            # Step 7: FR-SCD2-001 - Write order summary with Hudi
            if not self.write_order_summary(order_clean):
                logger.error("Failed to write order summary data")
                return False

            # Step 8: FR-CATALOG-001 - Register tables in Glue Catalog
            if not self.register_glue_catalog_tables():
                logger.error("Failed to register Glue Catalog tables")
                return False

            # Job completion
            job_duration = (datetime.now() - self.job_start_time).total_seconds()
            logger.info("=" * 80)
            logger.info(f"ETL Job completed successfully in {job_duration:.2f} seconds")
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(f"ETL Job failed with error: {str(e)}", exc_info=True)
            return False


def main():
    """
    Main entry point for AWS Glue job.
    """
    try:
        # Get job parameters
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])

        # Initialize contexts
        sc, glue_context, spark = initialize_spark_contexts()

        # Initialize Glue job
        job = Job(glue_context)
        job.init(args['JOB_NAME'], args)

        # Create and run ETL job
        etl_job = CustomerOrderETLJob(spark, glue_context)
        success = etl_job.run()

        # Commit job
        job.commit()

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()