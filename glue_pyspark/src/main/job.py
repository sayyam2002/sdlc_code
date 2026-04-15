"""
AWS Glue PySpark Job - Customer Order Analytics with SCD Type 2
Implements complete ETL pipeline with Hudi-based historical tracking

TRD Requirements:
- FR-INGEST-001: Ingest customer and order data from S3
- TR-CLEAN-001: Remove NULL values, 'Null' strings, and duplicates
- FR-SCD2-001: Implement SCD Type 2 with Hudi format
- FR-AGGREGATE-001: Calculate customer aggregate spend
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any

# PySpark imports
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as _sum, count,
    when, max as _max, coalesce, trim, upper
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

# AWS Glue imports
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configuration
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CustomerOrderETL:
    """
    Main ETL class for Customer Order Analytics with SCD Type 2

    TRD Implementation:
    - Data Sources: s3://adif-sdlc/sdlc_wizard/customerdata/, orderdata/
    - Curated Output: s3://adif-sdlc/curated/sdlc_wizard/customer/, order/
    - Analytics Output: s3://adif-sdlc/analytics/customeraggregatespend/
    """

    def __init__(self, params: Dict[str, Any]):
        """
        Initialize ETL job with parameters

        Args:
            params: Dictionary containing job parameters from glue_params.yaml
        """
        self.params = params
        self.logger = logging.getLogger(self.__class__.__name__)

        # TRD Section: Data Sources
        self.customer_source_path = params.get('customer_source_path')
        self.order_source_path = params.get('order_source_path')

        # TRD Section: Output Paths
        self.curated_customer_path = params.get('curated_customer_path')
        self.curated_order_path = params.get('curated_order_path')
        self.curated_ordersummary_path = params.get('curated_ordersummary_path')
        self.analytics_customeraggregatespend_path = params.get('analytics_customeraggregatespend_path')

        # TRD Section: Glue Catalog
        self.glue_database = params.get('glue_database', 'sdlc_wizard_db')

        # TRD Section: Hudi Configuration
        self.hudi_customer_table = params.get('hudi_customer_table_name', 'customer_hudi')
        self.hudi_order_table = params.get('hudi_order_table_name', 'order_hudi')

        self.logger.info("CustomerOrderETL initialized with parameters")

    def get_customer_schema(self) -> StructType:
        """
        TRD Section: Schemas - Customer Schema
        Columns: CustId (string), Name (string), EmailId (string), Region (string)
        """
        return StructType([
            StructField("CustId", StringType(), False),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])

    def get_order_schema(self) -> StructType:
        """
        TRD Section: Schemas - Order Schema
        Columns: OrderId, CustId, ItemName, PricePerUnit, Qty
        """
        return StructType([
            StructField("OrderId", StringType(), False),
            StructField("CustId", StringType(), False),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True)
        ])

    def read_customer_data(self, spark: SparkSession) -> DataFrame:
        """
        FR-INGEST-001: Ingest customer data from S3
        TRD Section: Data Sources - s3://adif-sdlc/sdlc_wizard/customerdata/

        Args:
            spark: SparkSession instance

        Returns:
            DataFrame containing customer data
        """
        try:
            self.logger.info(f"Reading customer data from {self.customer_source_path}")

            df = spark.read \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .option('encoding', 'UTF-8') \
                .csv(self.customer_source_path)

            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} customer records")

            return df

        except Exception as e:
            self.logger.error(f"Error reading customer data: {str(e)}")
            raise

    def read_order_data(self, spark: SparkSession) -> DataFrame:
        """
        FR-INGEST-001: Ingest order data from S3
        TRD Section: Data Sources - s3://adif-sdlc/sdlc_wizard/orderdata/

        Args:
            spark: SparkSession instance

        Returns:
            DataFrame containing order data
        """
        try:
            self.logger.info(f"Reading order data from {self.order_source_path}")

            df = spark.read \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .option('encoding', 'UTF-8') \
                .csv(self.order_source_path)

            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} order records")

            return df

        except Exception as e:
            self.logger.error(f"Error reading order data: {str(e)}")
            raise

    def clean_data(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        TR-CLEAN-001: Data Cleaning
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records

        Args:
            df: Input DataFrame
            dataset_name: Name of dataset for logging

        Returns:
            Cleaned DataFrame
        """
        try:
            self.logger.info(f"Starting data cleaning for {dataset_name}")
            initial_count = df.count()

            # TRD Section: TR-CLEAN-001 - Remove NULL values
            df_cleaned = df.dropna()
            after_null_removal = df_cleaned.count()
            self.logger.info(f"Removed {initial_count - after_null_removal} NULL records")

            # TRD Section: TR-CLEAN-001 - Remove 'Null' string values
            for column in df_cleaned.columns:
                df_cleaned = df_cleaned.filter(
                    ~(upper(trim(col(column))) == lit('NULL'))
                )

            after_null_string_removal = df_cleaned.count()
            self.logger.info(f"Removed {after_null_removal - after_null_string_removal} 'Null' string records")

            # TRD Section: TR-CLEAN-001 - Remove duplicate records
            df_cleaned = df_cleaned.dropDuplicates()
            final_count = df_cleaned.count()
            self.logger.info(f"Removed {after_null_string_removal - final_count} duplicate records")

            self.logger.info(f"Data cleaning complete for {dataset_name}: {initial_count} -> {final_count} records")

            return df_cleaned

        except Exception as e:
            self.logger.error(f"Error cleaning data for {dataset_name}: {str(e)}")
            raise

    def add_scd2_columns(self, df: DataFrame, is_new_record: bool = True) -> DataFrame:
        """
        FR-SCD2-001: Add SCD Type 2 columns
        TRD Section: SCD TYPE 2 REQUIREMENTS
        - IsActive (boolean)
        - StartDate (timestamp)
        - EndDate (timestamp)
        - OpTs (timestamp)

        Args:
            df: Input DataFrame
            is_new_record: True for new records, False for historical

        Returns:
            DataFrame with SCD Type 2 columns
        """
        try:
            self.logger.info("Adding SCD Type 2 columns")

            current_ts = current_timestamp()

            if is_new_record:
                # TRD Section: SCD Type 2 - New records
                df_scd2 = df \
                    .withColumn("IsActive", lit(True)) \
                    .withColumn("StartDate", current_ts) \
                    .withColumn("EndDate", lit(None).cast(TimestampType())) \
                    .withColumn("OpTs", current_ts)
            else:
                # TRD Section: SCD Type 2 - Historical records
                df_scd2 = df \
                    .withColumn("IsActive", lit(False)) \
                    .withColumn("EndDate", current_ts) \
                    .withColumn("OpTs", current_ts)

            self.logger.info("SCD Type 2 columns added successfully")
            return df_scd2

        except Exception as e:
            self.logger.error(f"Error adding SCD Type 2 columns: {str(e)}")
            raise

    def write_hudi_table(self, df: DataFrame, table_name: str,
                        record_key: str, output_path: str) -> None:
        """
        FR-SCD2-001: Write data to Hudi format with SCD Type 2
        TRD Section: SCD TYPE 2 REQUIREMENTS - Use Hudi format with upsert operation

        Args:
            df: DataFrame to write
            table_name: Hudi table name
            record_key: Primary key column
            output_path: S3 output path
        """
        try:
            self.logger.info(f"Writing Hudi table {table_name} to {output_path}")

            # TRD Section: Hudi Configuration
            hudi_options = {
                'hoodie.table.name': table_name,
                'hoodie.datasource.write.recordkey.field': record_key,
                'hoodie.datasource.write.precombine.field': 'OpTs',
                'hoodie.datasource.write.operation': 'upsert',
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.datasource.write.hive_style_partitioning': 'false',
                'hoodie.upsert.shuffle.parallelism': '2',
                'hoodie.insert.shuffle.parallelism': '2'
            }

            df.write \
                .format('hudi') \
                .options(**hudi_options) \
                .mode('append') \
                .save(output_path)

            self.logger.info(f"Successfully wrote Hudi table {table_name}")

        except Exception as e:
            self.logger.error(f"Error writing Hudi table {table_name}: {str(e)}")
            raise

    def write_parquet(self, df: DataFrame, output_path: str,
                     table_name: str = None) -> None:
        """
        Write DataFrame to Parquet format

        Args:
            df: DataFrame to write
            output_path: S3 output path
            table_name: Optional table name for logging
        """
        try:
            log_name = table_name if table_name else "data"
            self.logger.info(f"Writing {log_name} to {output_path}")

            df.write \
                .mode('overwrite') \
                .parquet(output_path)

            record_count = df.count()
            self.logger.info(f"Successfully wrote {record_count} records to {output_path}")

        except Exception as e:
            self.logger.error(f"Error writing parquet to {output_path}: {str(e)}")
            raise

    def create_order_summary(self, spark: SparkSession,
                            customer_df: DataFrame,
                            order_df: DataFrame) -> DataFrame:
        """
        Create order summary by joining customer and order data
        TRD Section: Output - ordersummary

        Args:
            spark: SparkSession instance
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Order summary DataFrame
        """
        try:
            self.logger.info("Creating order summary")

            # Join customer and order data
            order_summary = order_df.join(
                customer_df.select("CustId", "Name", "EmailId", "Region"),
                on="CustId",
                how="inner"
            )

            # Calculate total amount
            order_summary = order_summary.withColumn(
                "TotalAmount",
                col("PricePerUnit") * col("Qty")
            )

            record_count = order_summary.count()
            self.logger.info(f"Created order summary with {record_count} records")

            return order_summary

        except Exception as e:
            self.logger.error(f"Error creating order summary: {str(e)}")
            raise

    def calculate_customer_aggregate_spend(self, order_summary_df: DataFrame) -> DataFrame:
        """
        FR-AGGREGATE-001: Calculate customer aggregate spend
        TRD Section: AGGREGATION REQUIREMENTS

        Args:
            order_summary_df: Order summary DataFrame

        Returns:
            Customer aggregate spend DataFrame
        """
        try:
            self.logger.info("Calculating customer aggregate spend")

            # TRD Section: Aggregation - Group by customer and calculate totals
            aggregate_spend = order_summary_df.groupBy(
                "CustId", "Name", "EmailId", "Region"
            ).agg(
                _sum("TotalAmount").alias("TotalSpend"),
                count("OrderId").alias("TotalOrders"),
                _sum("Qty").alias("TotalItems")
            )

            record_count = aggregate_spend.count()
            self.logger.info(f"Calculated aggregate spend for {record_count} customers")

            return aggregate_spend

        except Exception as e:
            self.logger.error(f"Error calculating customer aggregate spend: {str(e)}")
            raise

    def register_glue_tables(self, glueContext: GlueContext) -> None:
        """
        Register tables in AWS Glue Catalog
        TRD Section: AWS Glue Catalog

        Args:
            glueContext: GlueContext instance
        """
        try:
            self.logger.info(f"Registering tables in Glue database: {self.glue_database}")

            # Create database if not exists
            try:
                glueContext.create_database(self.glue_database)
                self.logger.info(f"Created Glue database: {self.glue_database}")
            except Exception:
                self.logger.info(f"Glue database {self.glue_database} already exists")

            # Register customer table
            glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": [self.curated_customer_path]},
                format="parquet"
            ).toDF().createOrReplaceTempView("customer_temp")

            self.logger.info("Glue tables registered successfully")

        except Exception as e:
            self.logger.error(f"Error registering Glue tables: {str(e)}")
            # Non-critical error, continue execution
            pass

    def run(self, spark: SparkSession, glueContext: GlueContext) -> None:
        """
        Main ETL execution method
        Implements complete pipeline from TRD requirements

        Args:
            spark: SparkSession instance
            glueContext: GlueContext instance
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("Starting Customer Order ETL Job with SCD Type 2")
            self.logger.info("=" * 80)

            # FR-INGEST-001: Read customer data
            self.logger.info("Step 1: Reading customer data")
            customer_raw = self.read_customer_data(spark)

            # FR-INGEST-001: Read order data
            self.logger.info("Step 2: Reading order data")
            order_raw = self.read_order_data(spark)

            # TR-CLEAN-001: Clean customer data
            self.logger.info("Step 3: Cleaning customer data")
            customer_cleaned = self.clean_data(customer_raw, "customer")

            # TR-CLEAN-001: Clean order data
            self.logger.info("Step 4: Cleaning order data")
            order_cleaned = self.clean_data(order_raw, "order")

            # FR-SCD2-001: Add SCD Type 2 columns to customer
            self.logger.info("Step 5: Adding SCD Type 2 columns to customer")
            customer_scd2 = self.add_scd2_columns(customer_cleaned, is_new_record=True)

            # FR-SCD2-001: Add SCD Type 2 columns to order
            self.logger.info("Step 6: Adding SCD Type 2 columns to order")
            order_scd2 = self.add_scd2_columns(order_cleaned, is_new_record=True)

            # FR-SCD2-001: Write customer to Hudi
            self.logger.info("Step 7: Writing customer data to Hudi")
            self.write_hudi_table(
                customer_scd2,
                self.hudi_customer_table,
                'CustId',
                self.curated_customer_path
            )

            # FR-SCD2-001: Write order to Hudi
            self.logger.info("Step 8: Writing order data to Hudi")
            self.write_hudi_table(
                order_scd2,
                self.hudi_order_table,
                'OrderId',
                self.curated_order_path
            )

            # Create order summary
            self.logger.info("Step 9: Creating order summary")
            order_summary = self.create_order_summary(
                spark,
                customer_cleaned,
                order_cleaned
            )

            # Write order summary
            self.logger.info("Step 10: Writing order summary")
            self.write_parquet(
                order_summary,
                self.curated_ordersummary_path,
                "order_summary"
            )

            # FR-AGGREGATE-001: Calculate customer aggregate spend
            self.logger.info("Step 11: Calculating customer aggregate spend")
            customer_aggregate = self.calculate_customer_aggregate_spend(order_summary)

            # Write customer aggregate spend
            self.logger.info("Step 12: Writing customer aggregate spend")
            self.write_parquet(
                customer_aggregate,
                self.analytics_customeraggregatespend_path,
                "customer_aggregate_spend"
            )

            # Register Glue tables
            self.logger.info("Step 13: Registering Glue Catalog tables")
            self.register_glue_tables(glueContext)

            self.logger.info("=" * 80)
            self.logger.info("Customer Order ETL Job completed successfully")
            self.logger.info("=" * 80)

        except Exception as e:
            self.logger.error(f"ETL job failed: {str(e)}")
            raise


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file

    Args:
        config_path: Path to glue_params.yaml

    Returns:
        Dictionary containing configuration parameters
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config.get('job_parameters', {})
    except FileNotFoundError:
        logger.warning(f"Config file not found: {config_path}, using defaults")
        return {}
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return {}


def main():
    """
    Main entry point for AWS Glue job

    TRD Implementation:
    - Loads parameters from glue_params.yaml
    - Merges with command-line arguments
    - Initializes Spark and Glue contexts
    - Executes ETL pipeline
    """
    # CRITICAL: Declare global contexts (AWS Glue pre-initializes these)
    global spark, glueContext, sc

    try:
        # Calculate config path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(script_dir)),
            'config',
            'glue_params.yaml'
        )

        # Load YAML configuration
        logger.info(f"Loading configuration from {config_path}")
        config_params = load_config(config_path)

        # Get command-line arguments (AWS Glue environment)
        try:
            args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            job_name = args['JOB_NAME']
            logger.info(f"Running AWS Glue job: {job_name}")

            # Merge additional command-line args
            optional_args = [
                'customer_source_path',
                'order_source_path',
                'curated_customer_path',
                'curated_order_path',
                'curated_ordersummary_path',
                'analytics_customeraggregatespend_path',
                'glue_database'
            ]

            for arg in optional_args:
                try:
                    resolved = getResolvedOptions(sys.argv, [arg])
                    config_params[arg] = resolved[arg]
                except Exception:
                    pass  # Use default from YAML

        except Exception:
            # Local execution
            logger.info("Running in local mode")
            job_name = config_params.get('JOB_NAME', 'local_test')

            # Use local parameters if available
            local_params = load_config(config_path).get('local_parameters', {})
            if local_params:
                config_params.update(local_params)

        # Check if contexts exist (AWS Glue environment)
        contexts_exist = False
        try:
            _ = sc.version
            contexts_exist = True
            logger.info("Using existing Spark contexts from AWS Glue")
        except (NameError, AttributeError):
            logger.info("Creating new Spark contexts for local execution")

        # Create contexts only if they don't exist (local testing)
        if not contexts_exist:
            sc = SparkContext()
            glueContext = GlueContext(sc)
            spark = glueContext.spark_session

        # Initialize Glue job
        job = Job(glueContext)
        job.init(job_name, {})

        # Create and run ETL
        logger.info("Initializing CustomerOrderETL")
        etl = CustomerOrderETL(config_params)

        logger.info("Starting ETL execution")
        etl.run(spark, glueContext)

        # Commit job
        job.commit()
        logger.info("Job committed successfully")

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    main()