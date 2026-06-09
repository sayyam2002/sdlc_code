"""
AWS Glue PySpark ETL Job - Customer and Order Data Processing

This module implements the complete ETL pipeline for customer and order data:
1. Ingest CSV data from S3
2. Clean and transform data
3. Write to Parquet format
4. Register tables in Glue Catalog
5. Calculate customer aggregate spending
"""

import sys
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

import yaml
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, lit, when
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

        Returns:
            tuple: (spark, glueContext, job)
        """
        try:
            # Check if running in Glue environment
            if 'spark' in dir() and hasattr(sys.modules.get('__main__', None), 'spark'):
                logger.info("Using existing Spark session from Glue environment")
                spark = sys.modules['__main__'].spark
                sc = spark.sparkContext
            else:
                logger.info("Creating new Spark session")
                sc = SparkContext.getOrCreate()
                spark = SparkSession(sc)

            glueContext = GlueContext(sc)

            # Initialize job if JOB_NAME is available
            job = None
            try:
                args = getResolvedOptions(sys.argv, ['JOB_NAME'])
                job = Job(glueContext)
                job.init(args['JOB_NAME'], args)
                logger.info(f"Initialized Glue job: {args['JOB_NAME']}")
            except Exception as e:
                logger.warning(f"Could not initialize Glue job (may be running locally): {e}")

            return spark, glueContext, job

        except Exception as e:
            logger.error(f"Failed to initialize Spark contexts: {e}")
            raise


class ConfigManager:
    """Manages configuration loading and parameter retrieval"""

    @staticmethod
    def load_config(config_path: str = "config/glue_params.yaml") -> Dict[str, Any]:
        """
        Load configuration from YAML file

        Args:
            config_path: Path to configuration file

        Returns:
            Dictionary containing configuration parameters
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Successfully loaded configuration from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}")
            raise

    @staticmethod
    def get_job_parameters(config_path: str = "config/glue_params.yaml") -> Dict[str, Any]:
        """
        Get job parameters from configuration file

        Args:
            config_path: Path to configuration file

        Returns:
            Dictionary containing job parameters
        """
        return ConfigManager.load_config(config_path)


class S3DataReader:
    """Handles reading data from S3"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _read_csv(self, path: str, options: Dict[str, str]) -> DataFrame:
        """
        Internal method to read CSV data

        Args:
            path: S3 path to CSV files
            options: Read options

        Returns:
            DataFrame containing the data
        """
        return self.spark.read.options(**options).csv(path)

    def read_data_safe(
        self,
        path: str,
        format_type: str,
        options: Optional[Dict[str, str]] = None,
        expected_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Safely read data from S3 with validation

        Args:
            path: S3 path to data
            format_type: File format (csv, parquet, etc.)
            options: Read options
            expected_columns: List of expected column names

        Returns:
            DataFrame containing the data

        Raises:
            Exception: If source path doesn't exist, parsing fails, or schema validation fails
        """
        try:
            logger.info(f"Reading data from {path} in {format_type} format")

            # Validate path exists (basic check)
            if not path or path.strip() == "":
                raise ValueError(f"Source path is empty or invalid")

            # Read data based on format
            if format_type.lower() == "csv":
                read_options = options or {}
                df = self._read_csv(path, read_options)
            elif format_type.lower() == "parquet":
                df = self.spark.read.parquet(path)
            else:
                raise ValueError(f"Unsupported format: {format_type}")

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            # Validate schema if expected columns provided
            if expected_columns:
                actual_columns = set(df.columns)
                expected_columns_lower = set([c.lower() for c in expected_columns])

                if not expected_columns_lower.issubset(actual_columns):
                    missing_cols = expected_columns_lower - actual_columns
                    raise ValueError(
                        f"Schema validation failed. Missing columns: {missing_cols}. "
                        f"Expected columns: {expected_columns_lower}"
                    )

            # Get row count
            row_count = df.count()

            if row_count == 0:
                logger.warning(f"Successfully loaded 0 rows from {path} (empty file)")
            else:
                logger.info(f"Successfully loaded {row_count} rows from {path}")

            return df

        except Exception as e:
            if "Path does not exist" in str(e) or "FileNotFound" in str(e):
                error_msg = f"Source path not found: {path}"
                logger.error(error_msg)
                raise Exception(error_msg)
            elif "Schema validation failed" in str(e):
                logger.error(str(e))
                raise Exception(str(e))
            else:
                error_msg = f"Failed to parse {format_type.upper()} files in source path: {path}"
                logger.error(f"{error_msg}. Error: {e}")
                raise Exception(error_msg)


class S3DataWriter:
    """Handles writing data to S3"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _write_parquet(self, df: DataFrame, path: str, mode: str):
        """
        Internal method to write Parquet data

        Args:
            df: DataFrame to write
            path: S3 target path
            mode: Write mode (overwrite, append, etc.)
        """
        df.write.mode(mode).parquet(path)

    def _write_csv(self, df: DataFrame, path: str, mode: str, options: Dict[str, str]):
        """
        Internal method to write CSV data

        Args:
            df: DataFrame to write
            path: S3 target path
            mode: Write mode
            options: Write options
        """
        df.write.mode(mode).options(**options).csv(path)

    def write_data_safe(
        self,
        df: DataFrame,
        path: str,
        format_type: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, str]] = None
    ):
        """
        Safely write data to S3

        Args:
            df: DataFrame to write
            path: S3 target path
            format_type: File format (parquet, csv, etc.)
            mode: Write mode (overwrite, append, etc.)
            options: Write options
        """
        try:
            logger.info(f"Writing data to {path} in {format_type} format with mode {mode}")

            row_count = df.count()
            logger.info(f"Writing {row_count} rows")

            if format_type.lower() == "parquet":
                self._write_parquet(df, path, mode)
            elif format_type.lower() == "csv":
                write_options = options or {"header": "true"}
                self._write_csv(df, path, mode, write_options)
            else:
                raise ValueError(f"Unsupported format: {format_type}")

            logger.info(f"Successfully wrote {row_count} rows to {path}")

        except Exception as e:
            logger.error(f"Failed to write data to {path}: {e}")
            raise


class DataCleaner:
    """Handles data cleaning operations"""

    @staticmethod
    def remove_nulls_and_null_strings(df: DataFrame) -> DataFrame:
        """
        Remove records containing NULL values or string literal "Null"

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        logger.info("Removing NULL values and 'Null' string literals")

        initial_count = df.count()

        # Build filter condition for all columns
        filter_condition = None
        for column in df.columns:
            # Check for NULL values and string "Null"
            col_condition = (col(column).isNotNull()) & (col(column) != "Null")

            if filter_condition is None:
                filter_condition = col_condition
            else:
                filter_condition = filter_condition & col_condition

        df_cleaned = df.filter(filter_condition)
        final_count = df_cleaned.count()

        removed_count = initial_count - final_count
        logger.info(f"Removed {removed_count} rows with NULL or 'Null' values")

        return df_cleaned

    @staticmethod
    def remove_duplicates(df: DataFrame) -> DataFrame:
        """
        Remove duplicate records, retaining first occurrence

        Args:
            df: Input DataFrame

        Returns:
            Deduplicated DataFrame
        """
        logger.info("Removing duplicate records")

        initial_count = df.count()
        df_deduped = df.dropDuplicates()
        final_count = df_deduped.count()

        removed_count = initial_count - final_count
        logger.info(f"Removed {removed_count} duplicate rows")

        return df_deduped

    @staticmethod
    def clean_dataframe(df: DataFrame, enable_cleaning: bool = True) -> DataFrame:
        """
        Apply all cleaning operations to DataFrame

        Args:
            df: Input DataFrame
            enable_cleaning: Whether to enable cleaning

        Returns:
            Cleaned DataFrame
        """
        if not enable_cleaning:
            logger.info("Data cleaning is disabled")
            return df

        df = DataCleaner.remove_nulls_and_null_strings(df)
        df = DataCleaner.remove_duplicates(df)

        return df


class GlueCatalogManager:
    """Manages AWS Glue Data Catalog operations"""

    def __init__(self, spark: SparkSession, database: str):
        self.spark = spark
        self.database = database

    def register_table(
        self,
        df: DataFrame,
        table_name: str,
        path: str
    ):
        """
        Register DataFrame as a table in Glue Data Catalog

        Args:
            df: DataFrame to register
            table_name: Name of the table
            path: S3 path where data is stored
        """
        try:
            logger.info(f"Registering table {self.database}.{table_name} in Glue Catalog")

            # Create database if not exists
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")

            # Drop table if exists
            self.spark.sql(f"DROP TABLE IF EXISTS {self.database}.{table_name}")

            # Create external table
            df.write.format("parquet").mode("overwrite").option("path", path).saveAsTable(
                f"{self.database}.{table_name}"
            )

            logger.info(f"Successfully registered table {self.database}.{table_name}")

        except Exception as e:
            logger.error(f"Failed to register table {table_name}: {e}")
            raise


class DataAggregator:
    """Handles data aggregation operations"""

    @staticmethod
    def calculate_customer_aggregate_spend(
        df_customer: DataFrame,
        df_order: DataFrame
    ) -> DataFrame:
        """
        Calculate total spending per customer per date

        Args:
            df_customer: Customer DataFrame
            df_order: Order DataFrame

        Returns:
            Aggregated DataFrame with customer spending
        """
        logger.info("Calculating customer aggregate spending")

        # Join customer and order data
        df_joined = df_order.join(df_customer, on="custid", how="inner")

        # Calculate total amount (priceperunit * qty)
        df_joined = df_joined.withColumn(
            "totalamount",
            col("priceperunit").cast("double") * col("qty").cast("double")
        )

        # Aggregate by customer and date
        df_aggregated = df_joined.groupBy("custid", "name", "emailid", "region", "date").agg(
            _sum("totalamount").alias("totalspend")
        )

        row_count = df_aggregated.count()
        logger.info(f"Calculated aggregate spending for {row_count} customer-date combinations")

        return df_aggregated


def main():
    """
    Main ETL job execution function
    """
    try:
        logger.info("Starting AWS Glue PySpark ETL Job")

        # Initialize Spark contexts
        spark, glueContext, job = SparkContextManager.initialize_spark_contexts()

        # Load configuration
        params = ConfigManager.get_job_parameters()

        # Initialize components
        reader = S3DataReader(spark)
        writer = S3DataWriter(spark)
        catalog_manager = GlueCatalogManager(spark, params["glue_database"])

        # ===== STEP 1: Ingest Customer Data =====
        logger.info("=" * 80)
        logger.info("STEP 1: Ingesting Customer Data")
        logger.info("=" * 80)

        df_customer_raw = reader.read_data_safe(
            path=params["customer_source_path"],
            format_type=params["customer_source_format"],
            options=params.get("customer_source_options"),
            expected_columns=params.get("customer_expected_columns")
        )

        # ===== STEP 2: Ingest Order Data =====
        logger.info("=" * 80)
        logger.info("STEP 2: Ingesting Order Data")
        logger.info("=" * 80)

        df_order_raw = reader.read_data_safe(
            path=params["order_source_path"],
            format_type=params["order_source_format"],
            options=params.get("order_source_options"),
            expected_columns=params.get("order_expected_columns")
        )

        # ===== STEP 3: Clean Customer Data =====
        logger.info("=" * 80)
        logger.info("STEP 3: Cleaning Customer Data")
        logger.info("=" * 80)

        df_customer_clean = DataCleaner.clean_dataframe(
            df_customer_raw,
            enable_cleaning=params.get("enable_data_cleaning", True)
        )

        # ===== STEP 4: Clean Order Data =====
        logger.info("=" * 80)
        logger.info("STEP 4: Cleaning Order Data")
        logger.info("=" * 80)

        df_order_clean = DataCleaner.clean_dataframe(
            df_order_raw,
            enable_cleaning=params.get("enable_data_cleaning", True)
        )

        # ===== STEP 5: Write and Catalog Customer Data =====
        logger.info("=" * 80)
        logger.info("STEP 5: Writing and Cataloging Customer Data")
        logger.info("=" * 80)

        writer.write_data_safe(
            df=df_customer_clean,
            path=params["customer_target_path"],
            format_type=params["customer_target_format"],
            mode=params.get("customer_target_mode", "overwrite")
        )

        catalog_manager.register_table(
            df=df_customer_clean,
            table_name=params["customer_table_name"],
            path=params["customer_target_path"]
        )

        # ===== STEP 6: Write and Catalog Order Data =====
        logger.info("=" * 80)
        logger.info("STEP 6: Writing and Cataloging Order Data")
        logger.info("=" * 80)

        writer.write_data_safe(
            df=df_order_clean,
            path=params["order_target_path"],
            format_type=params["order_target_format"],
            mode=params.get("order_target_mode", "overwrite")
        )

        catalog_manager.register_table(
            df=df_order_clean,
            table_name=params["order_table_name"],
            path=params["order_target_path"]
        )

        # ===== STEP 7: Calculate Customer Aggregate Spend =====
        logger.info("=" * 80)
        logger.info("STEP 7: Calculating Customer Aggregate Spend")
        logger.info("=" * 80)

        df_aggregate = DataAggregator.calculate_customer_aggregate_spend(
            df_customer=df_customer_clean,
            df_order=df_order_clean
        )

        # ===== STEP 8: Write and Catalog Aggregate Data =====
        logger.info("=" * 80)
        logger.info("STEP 8: Writing and Cataloging Aggregate Data")
        logger.info("=" * 80)

        writer.write_data_safe(
            df=df_aggregate,
            path=params["aggregate_target_path"],
            format_type=params["aggregate_target_format"],
            mode=params.get("aggregate_target_mode", "overwrite")
        )

        catalog_manager.register_table(
            df=df_aggregate,
            table_name=params["aggregate_table_name"],
            path=params["aggregate_target_path"]
        )

        # Commit job if initialized
        if job:
            job.commit()

        logger.info("=" * 80)
        logger.info("AWS Glue PySpark ETL Job completed successfully")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Job failed with error: {e}")
        raise


if __name__ == "__main__":
    main()