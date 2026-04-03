"""
SDLC Wizard Customer-Order Data Pipeline
AWS Glue PySpark Job with SCD Type 2 Implementation

Functional Requirements:
- FR-INGEST-001: Ingest customer data from S3 CSV sources
- FR-INGEST-002: Ingest order data from S3 sources
- FR-CLEAN-001: Remove NULL values from datasets
- FR-CLEAN-002: Remove 'Null' string values
- FR-CLEAN-003: Remove duplicate records
- FR-SCD2-001: Implement SCD Type 2 with IsActive, StartDate, EndDate, OpTs
- FR-HUDI-001: Use Apache Hudi format for upsert operations
- FR-CATALOG-001: Register tables in AWS Glue Catalog

Technical Requirements:
- Customer Source: s3://adif-sdlc/sdlc_wizard/customerdata/
- Order Source: s3://adif-sdlc/sdlc_wizard/orderdata/
- Curated Output: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
- Analytics Output: s3://adif-sdlc/analytics/
- Glue Database: sdlc_wizard_db
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import yaml
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce,
    trim, upper, count, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType
)

# Conditional imports for AWS Glue (guarded for local testing)
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False
    GlueContext = None
    Job = None
    getResolvedOptions = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SDLCWizardPipeline:
    """
    Main pipeline class for SDLC Wizard Customer-Order data processing
    with SCD Type 2 implementation using Apache Hudi.
    """

    # Customer schema from TRD
    CUSTOMER_SCHEMA = StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), True),
        StructField("EmailId", StringType(), True),
        StructField("Region", StringType(), True)
    ])

    # Order schema from TRD
    ORDER_SCHEMA = StructType([
        StructField("CustId", StringType(), False),
        StructField("OrderId", StringType(), False)
    ])

    def __init__(self, spark: SparkSession, params: Dict[str, Any]):
        """
        Initialize the pipeline with Spark session and parameters.

        Args:
            spark: SparkSession instance
            params: Configuration parameters from YAML
        """
        self.spark = spark
        self.params = params
        self.glue_context = None
        self.job = None

        # Initialize Glue context if available
        if GLUE_AVAILABLE and isinstance(spark.sparkContext, SparkContext):
            try:
                self.glue_context = GlueContext(spark.sparkContext)
                self.job = Job(self.glue_context)
                if params.get('job', {}).get('name'):
                    self.job.init(params['job']['name'], params)
                logger.info("AWS Glue context initialized successfully")
            except Exception as e:
                logger.warning(f"Could not initialize Glue context: {e}")

        # Extract configuration
        self.customer_source_path = params['inputs']['customer_source_path']
        self.order_source_path = params['inputs']['order_source_path']
        self.curated_target_path = params['outputs']['curated_target_path']
        self.analytics_target_path = params['outputs']['analytics_target_path']
        self.database_name = params['glue_catalog']['database_name']
        self.enable_scd2 = params['flags']['enable_scd2']
        self.enable_hudi = params['flags']['enable_hudi']
        self.enable_data_cleaning = params['flags']['enable_data_cleaning']

        logger.info(f"Pipeline initialized with customer source: {self.customer_source_path}")
        logger.info(f"Order source: {self.order_source_path}")
        logger.info(f"Curated target: {self.curated_target_path}")

    def read_customer_data(self) -> DataFrame:
        """
        FR-INGEST-001: Ingest customer data from S3 CSV sources.

        Returns:
            DataFrame with customer data
        """
        logger.info(f"Reading customer data from {self.customer_source_path}")

        try:
            df = self.spark.read \
                .format(self.params['inputs']['source_format']) \
                .option("header", str(self.params['inputs'].get('csv_header', True)).lower()) \
                .option("delimiter", self.params['inputs'].get('csv_delimiter', ',')) \
                .schema(self.CUSTOMER_SCHEMA) \
                .load(self.customer_source_path)

            row_count = df.count()
            logger.info(f"Successfully read {row_count} customer records from {self.customer_source_path}")

            return df

        except Exception as e:
            logger.error(f"Error reading customer data: {e}")
            raise

    def read_order_data(self) -> DataFrame:
        """
        FR-INGEST-002: Ingest order data from S3 sources.

        Returns:
            DataFrame with order data
        """
        logger.info(f"Reading order data from {self.order_source_path}")

        try:
            df = self.spark.read \
                .format(self.params['inputs']['source_format']) \
                .option("header", str(self.params['inputs'].get('csv_header', True)).lower()) \
                .option("delimiter", self.params['inputs'].get('csv_delimiter', ',')) \
                .schema(self.ORDER_SCHEMA) \
                .load(self.order_source_path)

            row_count = df.count()
            logger.info(f"Successfully read {row_count} order records from {self.order_source_path}")

            return df

        except Exception as e:
            logger.error(f"Error reading order data: {e}")
            raise

    def clean_data(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        FR-CLEAN-001, FR-CLEAN-002, FR-CLEAN-003: Clean data by removing NULLs,
        'Null' strings, and duplicates.

        Args:
            df: Input DataFrame
            dataset_name: Name of dataset for logging

        Returns:
            Cleaned DataFrame
        """
        if not self.enable_data_cleaning:
            logger.info(f"Data cleaning disabled for {dataset_name}")
            return df

        logger.info(f"Cleaning {dataset_name} data")
        initial_count = df.count()

        # FR-CLEAN-002: Remove 'Null' string values (replace with actual NULL)
        for column in df.columns:
            df = df.withColumn(
                column,
                when(
                    (trim(upper(col(column))) == "NULL") |
                    (trim(col(column)) == ""),
                    lit(None)
                ).otherwise(col(column))
            )

        # FR-CLEAN-001: Remove rows with NULL values in key columns
        key_columns = [field.name for field in df.schema.fields if not field.nullable]
        if key_columns:
            df = df.dropna(subset=key_columns)
            logger.info(f"Removed NULL values from key columns: {key_columns}")

        # FR-CLEAN-003: Remove duplicate records
        df = df.dropDuplicates()

        final_count = df.count()
        removed_count = initial_count - final_count
        logger.info(f"Cleaned {dataset_name}: {initial_count} -> {final_count} records ({removed_count} removed)")

        return df

    def add_scd2_columns(self, df: DataFrame) -> DataFrame:
        """
        FR-SCD2-001: Add SCD Type 2 columns (IsActive, StartDate, EndDate, OpTs).

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with SCD Type 2 columns
        """
        if not self.enable_scd2:
            logger.info("SCD Type 2 disabled")
            return df

        logger.info("Adding SCD Type 2 columns")

        scd2_config = self.params.get('scd2_config', {})
        default_end_date = scd2_config.get('default_end_date', '9999-12-31T23:59:59Z')

        df = df \
            .withColumn(scd2_config.get('is_active_column', 'IsActive'), lit(True)) \
            .withColumn(scd2_config.get('start_date_column', 'StartDate'), current_timestamp()) \
            .withColumn(scd2_config.get('end_date_column', 'EndDate'), lit(default_end_date).cast(TimestampType())) \
            .withColumn(scd2_config.get('op_timestamp_column', 'OpTs'), current_timestamp())

        logger.info("SCD Type 2 columns added successfully")
        return df

    def join_customer_order_data(self, customer_df: DataFrame, order_df: DataFrame) -> DataFrame:
        """
        Join customer and order data on CustId.

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Joined DataFrame
        """
        logger.info("Joining customer and order data")

        joined_df = customer_df.join(
            order_df,
            on="CustId",
            how="inner"
        )

        row_count = joined_df.count()
        logger.info(f"Successfully joined data: {row_count} records")

        return joined_df

    def write_to_hudi(self, df: DataFrame, target_path: str, table_name: str) -> None:
        """
        FR-HUDI-001: Write DataFrame to S3 using Apache Hudi format with upsert operation.

        Args:
            df: DataFrame to write
            target_path: S3 target path
            table_name: Hudi table name
        """
        if not self.enable_hudi:
            logger.info(f"Hudi disabled, writing as parquet to {target_path}")
            df.write \
                .mode(self.params['outputs']['write_mode']) \
                .parquet(target_path)
            return

        logger.info(f"Writing data to Hudi table {table_name} at {target_path}")

        hudi_config = self.params.get('hudi_config', {})

        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': hudi_config.get('record_key_field', 'CustId'),
            'hoodie.datasource.write.precombine.field': hudi_config.get('precombine_field', 'OpTs'),
            'hoodie.datasource.write.partitionpath.field': hudi_config.get('partition_path_field', 'Region'),
            'hoodie.datasource.write.table.type': hudi_config.get('table_type', 'COPY_ON_WRITE'),
            'hoodie.datasource.write.operation': hudi_config.get('operation', 'upsert'),
            'hoodie.datasource.hive_sync.enable': str(hudi_config.get('hive_sync_enabled', True)).lower(),
            'hoodie.datasource.hive_sync.database': self.database_name,
            'hoodie.datasource.hive_sync.table': table_name,
            'hoodie.datasource.hive_sync.mode': hudi_config.get('hive_sync_mode', 'hms'),
            'hoodie.datasource.hive_sync.partition_fields': hudi_config.get('partition_path_field', 'Region'),
            'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
        }

        try:
            df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode(self.params['outputs']['write_mode']) \
                .save(target_path)

            logger.info(f"Successfully wrote {df.count()} records to Hudi table {table_name}")

        except Exception as e:
            logger.error(f"Error writing to Hudi: {e}")
            raise

    def register_glue_catalog_table(self, df: DataFrame, table_name: str, s3_path: str) -> None:
        """
        FR-CATALOG-001: Register table in AWS Glue Catalog.

        Args:
            df: DataFrame to register
            table_name: Table name in Glue Catalog
            s3_path: S3 path of the data
        """
        if not self.params['flags'].get('enable_catalog_registration', True):
            logger.info(f"Glue Catalog registration disabled for {table_name}")
            return

        if not self.glue_context:
            logger.warning("Glue context not available, skipping catalog registration")
            return

        logger.info(f"Registering table {table_name} in Glue Catalog database {self.database_name}")

        try:
            # Create temp view for catalog registration
            df.createOrReplaceTempView(table_name)

            # Register in Glue Catalog
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.database_name}.{table_name}
                USING parquet
                LOCATION '{s3_path}'
            """)

            logger.info(f"Successfully registered table {self.database_name}.{table_name}")

        except Exception as e:
            logger.error(f"Error registering Glue Catalog table: {e}")
            # Don't raise - catalog registration is not critical

    def run(self) -> None:
        """
        Main pipeline execution method.
        Orchestrates all data processing steps.
        """
        logger.info("=" * 80)
        logger.info("Starting SDLC Wizard Customer-Order Data Pipeline")
        logger.info("=" * 80)

        try:
            # FR-INGEST-001: Read customer data
            customer_df = self.read_customer_data()

            # FR-INGEST-002: Read order data
            order_df = self.read_order_data()

            # FR-CLEAN-001, FR-CLEAN-002, FR-CLEAN-003: Clean data
            customer_df_clean = self.clean_data(customer_df, "customer")
            order_df_clean = self.clean_data(order_df, "order")

            # Join customer and order data
            joined_df = self.join_customer_order_data(customer_df_clean, order_df_clean)

            # FR-SCD2-001: Add SCD Type 2 columns
            final_df = self.add_scd2_columns(joined_df)

            # FR-HUDI-001: Write to curated zone with Hudi
            self.write_to_hudi(
                final_df,
                self.curated_target_path,
                self.params['hudi_config']['table_name']
            )

            # FR-CATALOG-001: Register in Glue Catalog
            self.register_glue_catalog_table(
                final_df,
                self.params['glue_catalog']['curated_table_name'],
                self.curated_target_path
            )

            # Write analytics summary
            analytics_df = final_df.groupBy("Region") \
                .agg(
                    count("OrderId").alias("TotalOrders"),
                    count("CustId").alias("TotalCustomers")
                )

            analytics_path = f"{self.analytics_target_path}order_summary_by_region/"
            logger.info(f"Writing analytics summary to {analytics_path}")

            analytics_df.write \
                .mode(self.params['outputs']['write_mode']) \
                .parquet(analytics_path)

            logger.info("=" * 80)
            logger.info("Pipeline completed successfully")
            logger.info("=" * 80)

            # Commit Glue job if available
            if self.job:
                self.job.commit()

        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}")
            raise


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def get_job_parameters() -> Dict[str, Any]:
    """
    Get job parameters with dual-mode parsing (Glue + local fallback).
    Glue arguments override YAML defaults.

    Returns:
        Merged configuration dictionary
    """
    # Determine config path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', '..', 'config', 'glue_params.yaml')

    # Load YAML defaults
    config = load_config(config_path)

    # If running in Glue, merge with Glue arguments
    if GLUE_AVAILABLE and '--JOB_NAME' in sys.argv:
        try:
            args = getResolvedOptions(sys.argv, [
                'JOB_NAME',
                'customer_source_path',
                'order_source_path',
                'curated_target_path',
                'analytics_target_path',
                'database_name',
                'enable_scd2',
                'enable_hudi',
                'enable_data_cleaning'
            ])

            # Override YAML with Glue arguments
            if 'customer_source_path' in args:
                config['inputs']['customer_source_path'] = args['customer_source_path']
            if 'order_source_path' in args:
                config['inputs']['order_source_path'] = args['order_source_path']
            if 'curated_target_path' in args:
                config['outputs']['curated_target_path'] = args['curated_target_path']
            if 'analytics_target_path' in args:
                config['outputs']['analytics_target_path'] = args['analytics_target_path']
            if 'database_name' in args:
                config['glue_catalog']['database_name'] = args['database_name']
            if 'enable_scd2' in args:
                config['flags']['enable_scd2'] = args['enable_scd2'].lower() == 'true'
            if 'enable_hudi' in args:
                config['flags']['enable_hudi'] = args['enable_hudi'].lower() == 'true'
            if 'enable_data_cleaning' in args:
                config['flags']['enable_data_cleaning'] = args['enable_data_cleaning'].lower() == 'true'

            logger.info("Glue arguments merged with YAML configuration")

        except Exception as e:
            logger.warning(f"Could not parse Glue arguments: {e}")

    return config


def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session.

    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder \
        .appName("SDLC Wizard Customer-Order Pipeline") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false")

    # Add Hudi configurations
    builder = builder \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")

    spark = builder.getOrCreate()

    logger.info(f"Spark session created: {spark.version}")
    return spark


def main():
    """
    Main entry point for the Glue job.
    """
    # Get job parameters
    params = get_job_parameters()

    # Create Spark session
    spark = create_spark_session()

    # Initialize and run pipeline
    pipeline = SDLCWizardPipeline(spark, params)
    pipeline.run()

    # Stop Spark session
    spark.stop()
    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()