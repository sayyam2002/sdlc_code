"""
AWS Glue PySpark ETL Job - Customer Order Processing
Main job implementation with data ingestion, cleaning, SCD Type 2, and aggregation
"""

import sys
import yaml
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, lit, current_timestamp,
    when, row_number, max as _max
)
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkContextManager:
    """Manages Spark and Glue contexts initialization"""

    @staticmethod
    def initialize_spark_contexts():
        """
        Initialize Spark, Glue contexts and Job
        Returns: tuple of (spark, glueContext, job)
        """
        try:
            # Check if running in Glue environment
            if hasattr(sys.modules['__main__'], 'spark'):
                spark = sys.modules['__main__'].spark
                logger.info("Using existing Spark session from Glue environment")
            else:
                # Create new Spark session for local testing
                spark = SparkSession.builder \
                    .appName("CustomerOrderETL") \
                    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .getOrCreate()
                logger.info("Created new Spark session for local execution")

            sc = spark.sparkContext
            glueContext = GlueContext(sc)
            job = Job(glueContext)

            logger.info("Successfully initialized Spark contexts")
            return spark, glueContext, job

        except Exception as e:
            logger.error(f"Failed to initialize Spark contexts: {str(e)}")
            raise


class ConfigManager:
    """Manages configuration loading and parameter resolution"""

    @staticmethod
    def load_config(config_path: str = "config/glue_params.yaml") -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Successfully loaded configuration from {config_path}")
            return config
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return {}
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return {}

    @staticmethod
    def get_job_parameters() -> Dict[str, Any]:
        """
        Get job parameters from Glue job arguments or config file
        Returns: Dictionary of job parameters
        """
        try:
            # Try to get Glue job arguments
            args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            logger.info(f"Running as Glue job: {args['JOB_NAME']}")

            # Load additional parameters from config
            config = ConfigManager.load_config()

            # Merge Glue args with config
            params = {**config, **args}

        except Exception as e:
            logger.info(f"Not running in Glue environment, loading from config: {str(e)}")
            params = ConfigManager.load_config()

        # Set defaults if not provided
        defaults = {
            "customer_source_path": "s3://adif-sdlc/sdlc_wizard/customerdata/",
            "order_source_path": "s3://adif-sdlc/sdlc_wizard/orderdata/",
            "ordersummary_target_path": "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/",
            "customeraggregatespend_target_path": "s3://adif-sdlc/analytics/customeraggregatespend/",
            "glue_database": "gen_ai_poc_databrickscoe",
            "customer_table_name": "customer_cleaned",
            "order_table_name": "order_cleaned",
            "ordersummary_table_name": "ordersummary",
            "customeraggregatespend_table_name": "customeraggregatespend",
            "csv_delimiter": ",",
            "csv_header": "true",
            "hudi_record_key": "OrderId",
            "hudi_precombine_key": "OpTs",
            "hudi_table_type": "COPY_ON_WRITE",
            "hudi_operation": "upsert"
        }

        for key, value in defaults.items():
            if key not in params:
                params[key] = value

        return params


class S3DataReader:
    """Handles reading data from S3"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _read_csv_internal(self, path: str, delimiter: str = ",", header: str = "true") -> DataFrame:
        """Internal method to read CSV - can be mocked in tests"""
        return self.spark.read \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("inferSchema", "true") \
            .csv(path)

    def read_data_safe(self, path: str, format_type: str = "csv",
                       delimiter: str = ",", header: str = "true") -> Optional[DataFrame]:
        """
        Safely read data from S3 with error handling

        Args:
            path: S3 path to read from
            format_type: Format of the data (csv, parquet, etc.)
            delimiter: CSV delimiter
            header: Whether CSV has header

        Returns:
            DataFrame or None if read fails
        """
        try:
            logger.info(f"Reading data from {path}")

            if format_type.lower() == "csv":
                df = self._read_csv_internal(path, delimiter, header)
            elif format_type.lower() == "parquet":
                df = self.spark.read.parquet(path)
            else:
                raise ValueError(f"Unsupported format: {format_type}")

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            row_count = df.count()
            logger.info(f"Successfully loaded {row_count} records from {path}")

            return df

        except Exception as e:
            logger.error(f"Failed to read data from {path}: {str(e)}")
            raise


class S3DataWriter:
    """Handles writing data to S3"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """Internal method to write Parquet - can be mocked in tests"""
        df.write.mode(mode).parquet(path)

    def _write_hudi(self, df: DataFrame, path: str, hudi_options: Dict[str, str]):
        """Internal method to write Hudi - can be mocked in tests"""
        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)

    def write_data_safe(self, df: DataFrame, path: str, format_type: str = "parquet",
                       mode: str = "overwrite", hudi_options: Optional[Dict[str, str]] = None) -> bool:
        """
        Safely write data to S3 with error handling

        Args:
            df: DataFrame to write
            path: S3 path to write to
            format_type: Format to write (parquet, hudi)
            mode: Write mode (overwrite, append)
            hudi_options: Hudi configuration options

        Returns:
            True if write succeeds, False otherwise
        """
        try:
            logger.info(f"Writing data to {path} in {format_type} format")

            if format_type.lower() == "parquet":
                self._write_parquet(df, path, mode)
            elif format_type.lower() == "hudi":
                if not hudi_options:
                    raise ValueError("Hudi options required for Hudi format")
                self._write_hudi(df, path, hudi_options)
            else:
                raise ValueError(f"Unsupported format: {format_type}")

            logger.info(f"Successfully wrote data to {path}")
            return True

        except Exception as e:
            logger.error(f"Failed to write data to {path}: {str(e)}")
            raise


class DataCleaner:
    """Handles data cleaning operations"""

    @staticmethod
    def remove_nulls_and_duplicates(df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Remove NULL values (both actual NULL and string 'Null') and duplicates

        Args:
            df: Input DataFrame
            dataset_name: Name of dataset for logging

        Returns:
            Cleaned DataFrame
        """
        try:
            initial_count = df.count()
            logger.info(f"Starting data cleaning for {dataset_name}: {initial_count} records")

            # Remove rows with actual NULL values in any column
            df_no_nulls = df.na.drop()

            # Remove rows with string 'Null' in any column
            for column in df.columns:
                df_no_nulls = df_no_nulls.filter(
                    (col(column) != "Null") & (col(column) != "null")
                )

            # Remove duplicates
            df_cleaned = df_no_nulls.dropDuplicates()

            final_count = df_cleaned.count()
            removed_count = initial_count - final_count

            logger.info(f"Cleaned {dataset_name}: removed {removed_count} records, "
                       f"{final_count} records remaining")

            return df_cleaned

        except Exception as e:
            logger.error(f"Error cleaning {dataset_name}: {str(e)}")
            raise


class CatalogManager:
    """Handles AWS Glue Data Catalog operations"""

    def __init__(self, spark: SparkSession, database: str):
        self.spark = spark
        self.database = database

    def register_table(self, df: DataFrame, table_name: str, path: str) -> bool:
        """
        Register DataFrame as table in Glue Data Catalog

        Args:
            df: DataFrame to register
            table_name: Name of the table
            path: S3 path where data is stored

        Returns:
            True if registration succeeds
        """
        try:
            logger.info(f"Registering table {self.database}.{table_name}")

            # Create database if not exists
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")

            # Drop table if exists
            self.spark.sql(f"DROP TABLE IF EXISTS {self.database}.{table_name}")

            # Create temporary view
            df.createOrReplaceTempView(f"{table_name}_temp")

            # Create external table
            self.spark.sql(f"""
                CREATE TABLE {self.database}.{table_name}
                USING parquet
                LOCATION '{path}'
                AS SELECT * FROM {table_name}_temp
            """)

            logger.info(f"Successfully registered table {self.database}.{table_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register table {table_name}: {str(e)}")
            raise


class HudiManager:
    """Handles Apache Hudi operations for SCD Type 2"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def prepare_scd_type2_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for SCD Type 2 by adding required columns

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with SCD Type 2 columns
        """
        try:
            logger.info("Preparing data for SCD Type 2")

            current_ts = current_timestamp()

            df_scd = df \
                .withColumn("IsActive", lit(True)) \
                .withColumn("StartDate", current_ts) \
                .withColumn("EndDate", lit(None).cast("timestamp")) \
                .withColumn("OpTs", current_ts)

            logger.info("Successfully prepared SCD Type 2 data")
            return df_scd

        except Exception as e:
            logger.error(f"Error preparing SCD Type 2 data: {str(e)}")
            raise

    def get_hudi_options(self, params: Dict[str, Any], table_name: str) -> Dict[str, str]:
        """
        Generate Hudi configuration options

        Args:
            params: Job parameters
            table_name: Name of the Hudi table

        Returns:
            Dictionary of Hudi options
        """
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': params.get('hudi_record_key', 'OrderId'),
            'hoodie.datasource.write.precombine.field': params.get('hudi_precombine_key', 'OpTs'),
            'hoodie.datasource.write.table.type': params.get('hudi_table_type', 'COPY_ON_WRITE'),
            'hoodie.datasource.write.operation': params.get('hudi_operation', 'upsert'),
            'hoodie.datasource.write.hive_style_partitioning': 'false',
            'hoodie.upsert.shuffle.parallelism': '2',
            'hoodie.insert.shuffle.parallelism': '2'
        }

        return hudi_options


class AggregationEngine:
    """Handles data aggregation operations"""

    @staticmethod
    def calculate_customer_aggregate_spend(order_df: DataFrame) -> DataFrame:
        """
        Calculate total spend per customer

        Args:
            order_df: Order DataFrame with columns: CustId, PricePerUnit, Qty

        Returns:
            DataFrame with CustId and TotalSpend
        """
        try:
            logger.info("Calculating customer aggregate spend")

            # Calculate total spend per order line
            order_with_total = order_df.withColumn(
                "LineTotal",
                col("priceperunit") * col("qty")
            )

            # Aggregate by customer
            agg_df = order_with_total.groupBy("custid") \
                .agg(_sum("LineTotal").alias("TotalSpend"))

            # Rename columns to match expected schema
            agg_df = agg_df.select(
                col("custid").alias("CustId"),
                col("TotalSpend")
            )

            customer_count = agg_df.count()
            logger.info(f"Calculated aggregate spend for {customer_count} customers")

            return agg_df

        except Exception as e:
            logger.error(f"Error calculating customer aggregate spend: {str(e)}")
            raise


def main():
    """Main ETL job execution"""
    try:
        logger.info("=" * 80)
        logger.info("Starting Customer Order ETL Job")
        logger.info("=" * 80)

        # Initialize Spark contexts
        spark, glueContext, job = SparkContextManager.initialize_spark_contexts()

        # Get job parameters
        params = ConfigManager.get_job_parameters()
        logger.info(f"Job parameters loaded: {list(params.keys())}")

        # Initialize job
        if 'JOB_NAME' in params:
            job.init(params['JOB_NAME'], params)

        # Initialize components
        reader = S3DataReader(spark)
        writer = S3DataWriter(spark)
        cleaner = DataCleaner()
        catalog = CatalogManager(spark, params['glue_database'])
        hudi_mgr = HudiManager(spark)

        # ===================================================================
        # TR-INGEST-001: Load Customer Data from S3
        # ===================================================================
        logger.info("TR-INGEST-001: Loading customer data from S3")
        customer_raw_df = reader.read_data_safe(
            path=params['customer_source_path'],
            format_type='csv',
            delimiter=params['csv_delimiter'],
            header=params['csv_header']
        )

        if customer_raw_df is None:
            raise Exception(f"Source path not found: {params['customer_source_path']}")

        # Validate customer schema
        expected_customer_cols = ['custid', 'name', 'emailid', 'region']
        actual_cols = customer_raw_df.columns
        if not all(col in actual_cols for col in expected_customer_cols):
            raise Exception(f"Customer CSV schema does not match expected columns. "
                          f"Expected: {expected_customer_cols}, Got: {actual_cols}")

        # ===================================================================
        # TR-INGEST-002: Load Order Data from S3
        # ===================================================================
        logger.info("TR-INGEST-002: Loading order data from S3")
        order_raw_df = reader.read_data_safe(
            path=params['order_source_path'],
            format_type='csv',
            delimiter=params['csv_delimiter'],
            header=params['csv_header']
        )

        if order_raw_df is None:
            raise Exception(f"Source path not found: {params['order_source_path']}")

        # Validate order schema
        expected_order_cols = ['orderid', 'itemname', 'priceperunit', 'qty', 'date', 'custid']
        actual_cols = order_raw_df.columns
        if not all(col in actual_cols for col in expected_order_cols):
            raise Exception(f"Order CSV schema does not match expected columns. "
                          f"Expected: {expected_order_cols}, Got: {actual_cols}")

        # ===================================================================
        # TR-CLEAN-001: Clean Customer Data
        # ===================================================================
        logger.info("TR-CLEAN-001: Cleaning customer data")
        customer_cleaned_df = cleaner.remove_nulls_and_duplicates(
            customer_raw_df, "customer"
        )

        # ===================================================================
        # TR-CLEAN-002: Clean Order Data
        # ===================================================================
        logger.info("TR-CLEAN-002: Cleaning order data")
        order_cleaned_df = cleaner.remove_nulls_and_duplicates(
            order_raw_df, "order"
        )

        # ===================================================================
        # TR-CATALOG-001: Register Customer Table in Glue Catalog
        # ===================================================================
        logger.info("TR-CATALOG-001: Registering customer table in Glue Catalog")
        customer_catalog_path = f"{params['customer_source_path']}catalog/"
        writer.write_data_safe(
            customer_cleaned_df,
            customer_catalog_path,
            format_type='parquet'
        )
        catalog.register_table(
            customer_cleaned_df,
            params['customer_table_name'],
            customer_catalog_path
        )

        # ===================================================================
        # TR-CATALOG-002: Register Order Table in Glue Catalog
        # ===================================================================
        logger.info("TR-CATALOG-002: Registering order table in Glue Catalog")
        order_catalog_path = f"{params['order_source_path']}catalog/"
        writer.write_data_safe(
            order_cleaned_df,
            order_catalog_path,
            format_type='parquet'
        )
        catalog.register_table(
            order_cleaned_df,
            params['order_table_name'],
            order_catalog_path
        )

        # ===================================================================
        # TR-CLEAN-001 (Hudi): Implement SCD Type 2 for Order Summary
        # ===================================================================
        logger.info("Implementing SCD Type 2 for order summary using Hudi")
        order_scd_df = hudi_mgr.prepare_scd_type2_data(order_cleaned_df)

        hudi_options = hudi_mgr.get_hudi_options(
            params,
            params['ordersummary_table_name']
        )

        writer.write_data_safe(
            order_scd_df,
            params['ordersummary_target_path'],
            format_type='hudi',
            hudi_options=hudi_options
        )

        # ===================================================================
        # TR-CATALOG-003: Register Order Summary Hudi Table
        # ===================================================================
        logger.info("TR-CATALOG-003: Registering order summary Hudi table")
        catalog.register_table(
            order_scd_df,
            params['ordersummary_table_name'],
            params['ordersummary_target_path']
        )

        # ===================================================================
        # TR-AGG-001: Calculate Customer Aggregate Spend
        # ===================================================================
        logger.info("TR-AGG-001: Calculating customer aggregate spend")
        customer_agg_df = AggregationEngine.calculate_customer_aggregate_spend(
            order_cleaned_df
        )

        writer.write_data_safe(
            customer_agg_df,
            params['customeraggregatespend_target_path'],
            format_type='parquet'
        )

        # ===================================================================
        # TR-CATALOG-004: Register Customer Aggregate Spend Table
        # ===================================================================
        logger.info("TR-CATALOG-004: Registering customer aggregate spend table")
        catalog.register_table(
            customer_agg_df,
            params['customeraggregatespend_table_name'],
            params['customeraggregatespend_target_path']
        )

        # Commit job
        if 'JOB_NAME' in params:
            job.commit()

        logger.info("=" * 80)
        logger.info("Customer Order ETL Job completed successfully")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()