"""
AWS Glue PySpark Job - SDLC Wizard Data Pipeline
Implements data ingestion, cleaning, SCD Type 2, and aggregations

Functional Requirements:
- FR-INGEST-001: Data Ingestion from S3
- FR-CLEAN-001: Data Cleaning and Deduplication
- FR-SCD2-001: SCD Type 2 Implementation
- FR-AGG-001: Customer Aggregate Spend
- FR-SUMMARY-001: Order Summary Generation

Technical Requirements:
- TR-CLEAN-001: NULL and duplicate removal
- TR-SCD2-001: Hudi-based SCD Type 2
- TR-AGG-001: Customer-level aggregations
"""

import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as _sum, count, avg,
    min as _min, max as _max, when, row_number, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType, BooleanType
)
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SDLCWizardPipeline:
    """
    Main pipeline class for SDLC Wizard data processing
    Implements all FRD and TRD requirements
    """

    def __init__(self, spark_context: SparkContext = None):
        """
        Initialize pipeline with Spark and Glue contexts

        Args:
            spark_context: Optional SparkContext for testing
        """
        self.sc = spark_context
        self.glue_context = None
        self.spark = None
        self.job = None
        self.s3_client = None

        # Configuration from TRD
        self.config = {
            'source_paths': {
                'customer_data': 's3://adif-sdlc/sdlc_wizard/customerdata/',
                'order_data': 's3://adif-sdlc/sdlc_wizard/orderdata/'
            },
            'output_paths': {
                'customer_catalog': 's3://adif-sdlc/catalog/sdlc_wizard_customer/',
                'order_catalog': 's3://adif-sdlc/catalog/sdlc_wizard_order/',
                'order_summary': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
                'customer_aggregate_spend': 's3://adif-sdlc/analytics/customeraggregatespend/'
            },
            'glue_database': 'sdlc_wizard',
            'glue_tables': {
                'customer': 'sdlc_wizard_customer',
                'order': 'sdlc_wizard_order'
            },
            'scd2': {
                'is_active': 'IsActive',
                'start_date': 'StartDate',
                'end_date': 'EndDate',
                'op_ts': 'OpTs',
                'default_end_date': '9999-12-31 23:59:59'
            },
            'hudi': {
                'customer': {
                    'table_name': 'sdlc_wizard_customer_hudi',
                    'record_key': 'customer_id',
                    'precombine_key': 'OpTs',
                    'operation': 'upsert'
                },
                'order': {
                    'table_name': 'sdlc_wizard_order_hudi',
                    'record_key': 'order_id',
                    'precombine_key': 'order_date',
                    'operation': 'upsert'
                }
            }
        }

    def initialize_spark_contexts(self) -> Tuple[SparkContext, GlueContext, SparkSession, Job]:
        """
        Initialize Spark, Glue contexts and Job
        Pattern: FR-INGEST-001

        Returns:
            Tuple of (SparkContext, GlueContext, SparkSession, Job)
        """
        try:
            if self.sc is None:
                self.sc = SparkContext()

            self.glue_context = GlueContext(self.sc)
            self.spark = self.glue_context.spark_session

            # Initialize Glue Job
            try:
                args = getResolvedOptions(sys.argv, ['JOB_NAME'])
                self.job = Job(self.glue_context)
                self.job.init(args['JOB_NAME'], args)
                logger.info(f"Initialized Glue Job: {args['JOB_NAME']}")
            except Exception as e:
                logger.warning(f"Could not initialize Glue Job (may be running locally): {e}")
                self.job = None

            # Configure Spark session
            self.spark.conf.set("spark.sql.shuffle.partitions", "200")
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")

            # Initialize S3 client
            self.s3_client = boto3.client('s3')

            logger.info("Successfully initialized Spark and Glue contexts")
            return self.sc, self.glue_context, self.spark, self.job

        except Exception as e:
            logger.error(f"Failed to initialize Spark contexts: {e}")
            raise

    def validate_s3_access(self, s3_path: str) -> bool:
        """
        Validate S3 bucket access before operations
        Pattern: TR-CLEAN-001

        Args:
            s3_path: S3 path to validate (s3://bucket/prefix/)

        Returns:
            True if accessible, False otherwise
        """
        try:
            # Parse S3 path
            path_parts = s3_path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''

            # Test access
            self.s3_client.head_bucket(Bucket=bucket)
            logger.info(f"S3 access validated for: {s3_path}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"S3 access validation failed for {s3_path}: {error_code}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error validating S3 access: {e}")
            return False

    def read_data_safe(self, s3_path: str, format: str = 'csv',
                       schema: StructType = None, **options) -> Optional[DataFrame]:
        """
        Safely read data from S3 with validation
        Pattern: FR-INGEST-001

        Args:
            s3_path: S3 path to read from
            format: Data format (csv, parquet, json)
            schema: Optional schema to enforce
            **options: Additional read options

        Returns:
            DataFrame or None if read fails
        """
        try:
            # Validate S3 access
            if not self.validate_s3_access(s3_path):
                logger.error(f"Cannot access S3 path: {s3_path}")
                return None

            # Set default options for CSV
            if format == 'csv':
                default_options = {
                    'header': 'true',
                    'inferSchema': 'true' if schema is None else 'false',
                    'mode': 'PERMISSIVE'
                }
                default_options.update(options)
                options = default_options

            # Read data
            reader = self.spark.read.format(format)

            if schema:
                reader = reader.schema(schema)

            for key, value in options.items():
                reader = reader.option(key, value)

            df = reader.load(s3_path)

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            record_count = df.count()
            logger.info(f"Successfully read {record_count} records from {s3_path}")

            return df

        except Exception as e:
            logger.error(f"Failed to read data from {s3_path}: {e}")
            return None

    def write_data_safe(self, df: DataFrame, s3_path: str, format: str = 'parquet',
                       mode: str = 'overwrite', partition_by: List[str] = None,
                       hudi_options: Dict = None) -> bool:
        """
        Safely write data to S3 with validation
        Pattern: FR-SCD2-001, TR-SCD2-001

        Args:
            df: DataFrame to write
            s3_path: S3 destination path
            format: Output format (parquet, hudi, csv)
            mode: Write mode (overwrite, append)
            partition_by: Columns to partition by
            hudi_options: Hudi-specific options for SCD Type 2

        Returns:
            True if write successful, False otherwise
        """
        try:
            # Validate S3 access
            if not self.validate_s3_access(s3_path):
                logger.error(f"Cannot access S3 path: {s3_path}")
                return False

            writer = df.write.format(format).mode(mode)

            # Handle Hudi format for SCD Type 2
            if format == 'hudi' and hudi_options:
                for key, value in hudi_options.items():
                    writer = writer.option(key, value)

            # Add partitioning
            if partition_by:
                writer = writer.partitionBy(*partition_by)

            # Write data
            writer.save(s3_path)

            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to {s3_path}")

            return True

        except Exception as e:
            logger.error(f"Failed to write data to {s3_path}: {e}")
            return False

    def get_customer_schema(self) -> StructType:
        """
        Get customer data schema from TRD
        Pattern: FR-INGEST-001

        Returns:
            StructType schema for customer data
        """
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("registration_date", TimestampType(), True),
            StructField("customer_status", StringType(), True)
        ])

    def get_order_schema(self) -> StructType:
        """
        Get order data schema from TRD
        Pattern: FR-INGEST-001

        Returns:
            StructType schema for order data
        """
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_date", TimestampType(), True),
            StructField("order_amount", DecimalType(10, 2), True),
            StructField("order_status", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("payment_method", StringType(), True)
        ])

    def clean_data(self, df: DataFrame, critical_fields: List[str]) -> DataFrame:
        """
        Clean data by removing NULLs, 'Null' strings, and duplicates
        Pattern: FR-CLEAN-001, TR-CLEAN-001

        Args:
            df: Input DataFrame
            critical_fields: Fields that must not be NULL

        Returns:
            Cleaned DataFrame
        """
        try:
            initial_count = df.count()
            logger.info(f"Starting data cleaning. Initial record count: {initial_count}")

            # Step 1: Remove NULL values in critical fields
            for field in critical_fields:
                if field in df.columns:
                    df = df.filter(col(field).isNotNull())

            after_null_removal = df.count()
            logger.info(f"After NULL removal: {after_null_removal} records "
                       f"({initial_count - after_null_removal} removed)")

            # Step 2: Remove 'Null' string values
            null_strings = ['Null', 'NULL', 'null', 'None', 'NONE', '']
            for field in df.columns:
                if field in critical_fields:
                    for null_str in null_strings:
                        df = df.filter(col(field) != null_str)

            after_null_string_removal = df.count()
            logger.info(f"After 'Null' string removal: {after_null_string_removal} records "
                       f"({after_null_removal - after_null_string_removal} removed)")

            # Step 3: Remove duplicates based on primary key (first field in critical_fields)
            if critical_fields:
                primary_key = critical_fields[0]
                df = df.dropDuplicates([primary_key])

            final_count = df.count()
            logger.info(f"After duplicate removal: {final_count} records "
                       f"({after_null_string_removal - final_count} removed)")
            logger.info(f"Total records removed: {initial_count - final_count}")

            return df

        except Exception as e:
            logger.error(f"Error during data cleaning: {e}")
            raise

    def add_scd2_columns(self, df: DataFrame, is_new_record: bool = True) -> DataFrame:
        """
        Add SCD Type 2 columns to DataFrame
        Pattern: FR-SCD2-001, TR-SCD2-001

        Args:
            df: Input DataFrame
            is_new_record: True for new records, False for updates

        Returns:
            DataFrame with SCD Type 2 columns
        """
        try:
            current_ts = current_timestamp()

            if is_new_record:
                # New records: IsActive=True, StartDate=now, EndDate=9999-12-31
                df = df.withColumn(self.config['scd2']['is_active'], lit(True))
                df = df.withColumn(self.config['scd2']['start_date'], current_ts)
                df = df.withColumn(self.config['scd2']['end_date'],
                                 lit(self.config['scd2']['default_end_date']).cast(TimestampType()))
                df = df.withColumn(self.config['scd2']['op_ts'], current_ts)
            else:
                # Updated records: preserve existing SCD columns or set defaults
                if self.config['scd2']['is_active'] not in df.columns:
                    df = df.withColumn(self.config['scd2']['is_active'], lit(True))
                if self.config['scd2']['start_date'] not in df.columns:
                    df = df.withColumn(self.config['scd2']['start_date'], current_ts)
                if self.config['scd2']['end_date'] not in df.columns:
                    df = df.withColumn(self.config['scd2']['end_date'],
                                     lit(self.config['scd2']['default_end_date']).cast(TimestampType()))
                df = df.withColumn(self.config['scd2']['op_ts'], current_ts)

            logger.info(f"Added SCD Type 2 columns (is_new_record={is_new_record})")
            return df

        except Exception as e:
            logger.error(f"Error adding SCD Type 2 columns: {e}")
            raise

    def perform_scd2_upsert(self, new_df: DataFrame, existing_df: Optional[DataFrame],
                           primary_key: str, compare_columns: List[str]) -> DataFrame:
        """
        Perform SCD Type 2 upsert logic
        Pattern: FR-SCD2-001, TR-SCD2-001

        Args:
            new_df: New/incoming data
            existing_df: Existing data (None if first load)
            primary_key: Primary key column
            compare_columns: Columns to compare for changes

        Returns:
            DataFrame with SCD Type 2 logic applied
        """
        try:
            # If no existing data, all records are new
            if existing_df is None or existing_df.count() == 0:
                logger.info("No existing data found. Treating all records as new.")
                return self.add_scd2_columns(new_df, is_new_record=True)

            # Get active records from existing data
            active_existing = existing_df.filter(col(self.config['scd2']['is_active']) == True)

            # Join new data with existing active records
            joined = new_df.alias('new').join(
                active_existing.alias('existing'),
                col(f'new.{primary_key}') == col(f'existing.{primary_key}'),
                'left_outer'
            )

            # Identify changed records
            change_condition = None
            for compare_col in compare_columns:
                if compare_col != primary_key:
                    condition = (col(f'new.{compare_col}') != col(f'existing.{compare_col}'))
                    change_condition = condition if change_condition is None else (change_condition | condition)

            # Separate new and changed records
            new_records = joined.filter(col(f'existing.{primary_key}').isNull())
            changed_records = joined.filter(
                col(f'existing.{primary_key}').isNotNull() & change_condition
            ) if change_condition else self.spark.createDataFrame([], new_df.schema)

            # Process new records
            new_records_df = new_records.select('new.*')
            new_records_df = self.add_scd2_columns(new_records_df, is_new_record=True)

            # Process changed records - expire old, create new
            if changed_records.count() > 0:
                # Expire old records
                expired_records = changed_records.select('existing.*')
                expired_records = expired_records.withColumn(
                    self.config['scd2']['is_active'], lit(False)
                ).withColumn(
                    self.config['scd2']['end_date'], current_timestamp()
                )

                # Create new versions
                new_versions = changed_records.select('new.*')
                new_versions = self.add_scd2_columns(new_versions, is_new_record=True)

                # Combine all records
                result_df = new_records_df.union(expired_records).union(new_versions)
            else:
                result_df = new_records_df

            # Add unchanged active records
            unchanged_keys = active_existing.select(primary_key).subtract(
                new_df.select(primary_key)
            )
            unchanged_records = active_existing.join(
                unchanged_keys, primary_key, 'inner'
            )

            if unchanged_records.count() > 0:
                result_df = result_df.union(unchanged_records)

            logger.info(f"SCD Type 2 upsert completed. "
                       f"New: {new_records.count()}, "
                       f"Changed: {changed_records.count()}, "
                       f"Unchanged: {unchanged_records.count()}")

            return result_df

        except Exception as e:
            logger.error(f"Error performing SCD Type 2 upsert: {e}")
            raise

    def calculate_customer_aggregate_spend(self, customer_df: DataFrame,
                                          order_df: DataFrame) -> DataFrame:
        """
        Calculate customer aggregate spend
        Pattern: FR-AGG-001, TR-AGG-001

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            DataFrame with customer aggregate spend
        """
        try:
            logger.info("Calculating customer aggregate spend...")

            # Aggregate order data by customer
            order_agg = order_df.groupBy('customer_id').agg(
                _sum('order_amount').alias('total_spend'),
                count('order_id').alias('order_count'),
                avg('order_amount').alias('avg_spend_per_order')
            )

            # Join with customer data
            result = customer_df.join(
                order_agg,
                'customer_id',
                'inner'
            ).select(
                'customer_id',
                'customer_name',
                'total_spend',
                'order_count',
                'avg_spend_per_order',
                current_timestamp().alias('calculation_date')
            )

            result_count = result.count()
            logger.info(f"Customer aggregate spend calculated for {result_count} customers")

            return result

        except Exception as e:
            logger.error(f"Error calculating customer aggregate spend: {e}")
            raise

    def generate_order_summary(self, order_df: DataFrame) -> DataFrame:
        """
        Generate order summary statistics
        Pattern: FR-SUMMARY-001

        Args:
            order_df: Order DataFrame

        Returns:
            DataFrame with order summary
        """
        try:
            logger.info("Generating order summary...")

            # Aggregate order data
            summary = order_df.groupBy('customer_id').agg(
                count('order_id').alias('total_orders'),
                _sum('order_amount').alias('total_amount'),
                avg('order_amount').alias('avg_order_amount'),
                _min('order_date').alias('first_order_date'),
                _max('order_date').alias('last_order_date'),
                current_timestamp().alias('summary_date')
            )

            summary_count = summary.count()
            logger.info(f"Order summary generated for {summary_count} customers")

            return summary

        except Exception as e:
            logger.error(f"Error generating order summary: {e}")
            raise

    def run_pipeline(self) -> bool:
        """
        Execute the complete data pipeline
        Implements all FRD and TRD requirements

        Returns:
            True if pipeline succeeds, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("Starting SDLC Wizard Data Pipeline")
            logger.info("=" * 80)

            # Initialize contexts
            self.initialize_spark_contexts()

            # FR-INGEST-001: Read customer data
            logger.info("\n--- FR-INGEST-001: Reading Customer Data ---")
            customer_df = self.read_data_safe(
                self.config['source_paths']['customer_data'],
                format='csv',
                schema=self.get_customer_schema()
            )

            if customer_df is None:
                logger.error("Failed to read customer data")
                return False

            # FR-INGEST-001: Read order data
            logger.info("\n--- FR-INGEST-001: Reading Order Data ---")
            order_df = self.read_data_safe(
                self.config['source_paths']['order_data'],
                format='csv',
                schema=self.get_order_schema()
            )

            if order_df is None:
                logger.error("Failed to read order data")
                return False

            # FR-CLEAN-001: Clean customer data
            logger.info("\n--- FR-CLEAN-001: Cleaning Customer Data ---")
            customer_df_clean = self.clean_data(
                customer_df,
                critical_fields=['customer_id', 'customer_name', 'email']
            )

            # FR-CLEAN-001: Clean order data
            logger.info("\n--- FR-CLEAN-001: Cleaning Order Data ---")
            order_df_clean = self.clean_data(
                order_df,
                critical_fields=['order_id', 'customer_id', 'order_amount']
            )

            # FR-SCD2-001: Apply SCD Type 2 to customer data
            logger.info("\n--- FR-SCD2-001: Applying SCD Type 2 to Customer Data ---")

            # Try to read existing customer data
            existing_customer_df = self.read_data_safe(
                self.config['output_paths']['customer_catalog'],
                format='parquet'
            )

            # Perform SCD Type 2 upsert
            customer_scd2_df = self.perform_scd2_upsert(
                new_df=customer_df_clean,
                existing_df=existing_customer_df,
                primary_key='customer_id',
                compare_columns=['customer_name', 'email', 'phone', 'address',
                               'city', 'state', 'zip_code', 'customer_status']
            )

            # Write customer data with Hudi
            logger.info("\n--- Writing Customer Data with Hudi ---")
            hudi_customer_options = {
                'hoodie.table.name': self.config['hudi']['customer']['table_name'],
                'hoodie.datasource.write.recordkey.field': self.config['hudi']['customer']['record_key'],
                'hoodie.datasource.write.precombine.field': self.config['hudi']['customer']['precombine_key'],
                'hoodie.datasource.write.operation': self.config['hudi']['customer']['operation'],
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.datasource.hive_sync.enable': 'true',
                'hoodie.datasource.hive_sync.database': self.config['glue_database'],
                'hoodie.datasource.hive_sync.table': self.config['glue_tables']['customer'],
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
            }

            success = self.write_data_safe(
                customer_scd2_df,
                self.config['output_paths']['customer_catalog'],
                format='hudi',
                mode='append',
                hudi_options=hudi_customer_options
            )

            if not success:
                logger.error("Failed to write customer data")
                return False

            # Write cleaned order data
            logger.info("\n--- Writing Cleaned Order Data ---")
            order_df_with_ts = self.add_scd2_columns(order_df_clean, is_new_record=True)

            success = self.write_data_safe(
                order_df_with_ts,
                self.config['output_paths']['order_catalog'],
                format='parquet',
                mode='overwrite'
            )

            if not success:
                logger.error("Failed to write order data")
                return False

            # FR-AGG-001: Calculate customer aggregate spend
            logger.info("\n--- FR-AGG-001: Calculating Customer Aggregate Spend ---")

            # Get active customers only for aggregation
            active_customers = customer_scd2_df.filter(
                col(self.config['scd2']['is_active']) == True
            )

            customer_spend_df = self.calculate_customer_aggregate_spend(
                active_customers,
                order_df_clean
            )

            success = self.write_data_safe(
                customer_spend_df,
                self.config['output_paths']['customer_aggregate_spend'],
                format='parquet',
                mode='overwrite'
            )

            if not success:
                logger.error("Failed to write customer aggregate spend")
                return False

            # FR-SUMMARY-001: Generate order summary
            logger.info("\n--- FR-SUMMARY-001: Generating Order Summary ---")
            order_summary_df = self.generate_order_summary(order_df_clean)

            success = self.write_data_safe(
                order_summary_df,
                self.config['output_paths']['order_summary'],
                format='parquet',
                mode='overwrite'
            )

            if not success:
                logger.error("Failed to write order summary")
                return False

            logger.info("\n" + "=" * 80)
            logger.info("SDLC Wizard Data Pipeline Completed Successfully")
            logger.info("=" * 80)

            # Commit Glue job if initialized
            if self.job:
                self.job.commit()

            return True

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            # Cleanup
            if self.spark:
                logger.info("Stopping Spark session...")


def main():
    """
    Main entry point for AWS Glue job
    """
    pipeline = SDLCWizardPipeline()
    success = pipeline.run_pipeline()

    if not success:
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()