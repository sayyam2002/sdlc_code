"""
AWS Glue PySpark ETL Job - Customer Order Analytics with SCD Type 2

This job implements:
1. Data ingestion from S3 curated zones
2. Data cleaning (NULL removal, deduplication)
3. SCD Type 2 processing using Apache Hudi
4. Customer aggregate spend calculation
5. Glue Catalog registration

Technical Requirements (TRD):
- Source: s3://adif-sdlc/curated/sdlc_wizard/customer/ and order/
- Target: s3://adif-sdlc/analytics/customeraggregatespend/
- Format: Apache Hudi with SCD Type 2 columns
- Catalog: sdlc_wizard_db
"""

import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import yaml
import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    current_timestamp, lit, when, trim, lower, coalesce,
    row_number, desc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType, BooleanType, LongType
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


class SparkContextManager:
    """Manages Spark and Glue contexts with proper initialization and cleanup"""

    def __init__(self, app_name: str = "CustomerOrderSCD2ETL"):
        self.app_name = app_name
        self.spark_context: Optional[SparkContext] = None
        self.glue_context: Optional[GlueContext] = None
        self.spark_session: Optional[SparkSession] = None
        self.job: Optional[Job] = None

    def initialize_spark_contexts(self) -> Tuple[SparkContext, GlueContext, SparkSession]:
        """
        Initialize Spark, Glue contexts and Spark session

        Returns:
            Tuple of (SparkContext, GlueContext, SparkSession)
        """
        try:
            logger.info("Initializing Spark contexts...")

            # Initialize Spark Context
            self.spark_context = SparkContext.getOrCreate()
            self.spark_context.setLogLevel("INFO")

            # Initialize Glue Context
            self.glue_context = GlueContext(self.spark_context)

            # Initialize Spark Session
            self.spark_session = self.glue_context.spark_session

            # Configure Spark session
            self.spark_session.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark_session.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self.spark_session.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            self.spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

            logger.info("Spark contexts initialized successfully")
            return self.spark_context, self.glue_context, self.spark_session

        except Exception as e:
            logger.error(f"Failed to initialize Spark contexts: {str(e)}")
            raise

    def initialize_glue_job(self, job_name: str, args: List[str]) -> Job:
        """
        Initialize AWS Glue Job

        Args:
            job_name: Name of the Glue job
            args: Job arguments

        Returns:
            Initialized Job object
        """
        try:
            logger.info(f"Initializing Glue job: {job_name}")

            if not self.glue_context:
                raise RuntimeError("GlueContext not initialized. Call initialize_spark_contexts() first.")

            # Get job parameters
            resolved_args = getResolvedOptions(sys.argv, args)

            # Initialize job
            self.job = Job(self.glue_context)
            self.job.init(job_name, resolved_args)

            logger.info(f"Glue job initialized: {job_name}")
            return self.job

        except Exception as e:
            logger.error(f"Failed to initialize Glue job: {str(e)}")
            raise

    def cleanup(self):
        """Cleanup Spark contexts and commit job"""
        try:
            if self.job:
                self.job.commit()
                logger.info("Glue job committed successfully")

            if self.spark_session:
                self.spark_session.stop()
                logger.info("Spark session stopped")

        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")


class S3DataManager:
    """Manages S3 data operations with validation and error handling"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.s3_client = boto3.client('s3')

    def validate_s3_path(self, s3_path: str) -> bool:
        """
        Validate if S3 path exists and is accessible

        Args:
            s3_path: S3 path to validate (s3://bucket/key)

        Returns:
            True if path exists and is accessible
        """
        try:
            # Parse S3 path
            if not s3_path.startswith('s3://'):
                logger.error(f"Invalid S3 path format: {s3_path}")
                return False

            path_parts = s3_path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''

            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=bucket)

            # Check if prefix exists (list objects)
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1
            )

            if 'Contents' not in response:
                logger.warning(f"No objects found at path: {s3_path}")
                return False

            logger.info(f"S3 path validated successfully: {s3_path}")
            return True

        except ClientError as e:
            logger.error(f"S3 path validation failed for {s3_path}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error validating S3 path {s3_path}: {str(e)}")
            return False

    def read_data_safe(self, s3_path: str, file_format: str = "parquet",
                       schema: Optional[StructType] = None) -> Optional[DataFrame]:
        """
        Safely read data from S3 with validation

        Args:
            s3_path: S3 path to read from
            file_format: File format (parquet, csv, json)
            schema: Optional schema to enforce

        Returns:
            DataFrame if successful, None otherwise
        """
        try:
            logger.info(f"Reading data from: {s3_path}")

            # Validate S3 path
            if not self.validate_s3_path(s3_path):
                logger.error(f"S3 path validation failed: {s3_path}")
                return None

            # Read data
            if schema:
                df = self.spark.read.format(file_format).schema(schema).load(s3_path)
            else:
                df = self.spark.read.format(file_format).load(s3_path)

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            record_count = df.count()
            logger.info(f"Successfully read {record_count} records from {s3_path}")

            return df

        except Exception as e:
            logger.error(f"Failed to read data from {s3_path}: {str(e)}")
            return None

    def write_data_safe(self, df: DataFrame, s3_path: str,
                       file_format: str = "parquet", mode: str = "overwrite",
                       partition_by: Optional[List[str]] = None) -> bool:
        """
        Safely write data to S3

        Args:
            df: DataFrame to write
            s3_path: S3 path to write to
            file_format: File format (parquet, csv, json)
            mode: Write mode (overwrite, append)
            partition_by: Optional list of columns to partition by

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Writing data to: {s3_path}")

            record_count = df.count()
            logger.info(f"Writing {record_count} records")

            writer = df.write.format(file_format).mode(mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(s3_path)

            logger.info(f"Successfully wrote {record_count} records to {s3_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to write data to {s3_path}: {str(e)}")
            return False

    def write_hudi_safe(self, df: DataFrame, s3_path: str, table_name: str,
                       record_key: str, precombine_key: str,
                       operation: str = "upsert") -> bool:
        """
        Safely write data to S3 using Hudi format

        Args:
            df: DataFrame to write
            s3_path: S3 path to write to
            table_name: Hudi table name
            record_key: Primary key field
            precombine_key: Field for deduplication
            operation: Hudi operation (upsert, insert, bulk_insert)

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Writing Hudi table: {table_name} to {s3_path}")

            record_count = df.count()
            logger.info(f"Writing {record_count} records with operation: {operation}")

            hudi_options = {
                'hoodie.table.name': table_name,
                'hoodie.datasource.write.recordkey.field': record_key,
                'hoodie.datasource.write.precombine.field': precombine_key,
                'hoodie.datasource.write.operation': operation,
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
                'hoodie.combine.before.upsert': 'true',
                'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
                'hoodie.cleaner.commits.retained': '10',
                'hoodie.datasource.hive_sync.enable': 'false'
            }

            df.write.format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(s3_path)

            logger.info(f"Successfully wrote Hudi table: {table_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to write Hudi table {table_name}: {str(e)}")
            return False


class DataCleaner:
    """Handles data cleaning operations"""

    @staticmethod
    def remove_nulls(df: DataFrame, required_fields: List[str]) -> DataFrame:
        """
        Remove rows with NULL values in required fields

        Args:
            df: Input DataFrame
            required_fields: List of fields that cannot be NULL

        Returns:
            Cleaned DataFrame
        """
        try:
            initial_count = df.count()
            logger.info(f"Removing NULLs from {len(required_fields)} required fields")

            # Create filter condition for all required fields
            filter_condition = None
            for field in required_fields:
                if field in df.columns:
                    condition = col(field).isNotNull()
                    filter_condition = condition if filter_condition is None else filter_condition & condition

            if filter_condition is not None:
                df_cleaned = df.filter(filter_condition)
            else:
                df_cleaned = df

            final_count = df_cleaned.count()
            removed_count = initial_count - final_count

            logger.info(f"Removed {removed_count} rows with NULL values")
            return df_cleaned

        except Exception as e:
            logger.error(f"Error removing NULLs: {str(e)}")
            return df

    @staticmethod
    def remove_null_strings(df: DataFrame, null_values: List[str]) -> DataFrame:
        """
        Remove rows with 'Null' string values

        Args:
            df: Input DataFrame
            null_values: List of string values to treat as NULL

        Returns:
            Cleaned DataFrame
        """
        try:
            initial_count = df.count()
            logger.info(f"Removing null string values: {null_values}")

            # Replace null strings with actual NULL
            for column in df.columns:
                if df.schema[column].dataType == StringType():
                    for null_val in null_values:
                        df = df.withColumn(
                            column,
                            when(trim(lower(col(column))) == null_val.lower(), lit(None))
                            .otherwise(col(column))
                        )

            final_count = df.count()
            logger.info(f"Processed {initial_count} rows for null string removal")

            return df

        except Exception as e:
            logger.error(f"Error removing null strings: {str(e)}")
            return df

    @staticmethod
    def remove_duplicates(df: DataFrame, key_columns: List[str],
                         order_column: str = "OpTs") -> DataFrame:
        """
        Remove duplicate records, keeping the latest based on order_column

        Args:
            df: Input DataFrame
            key_columns: Columns to identify duplicates
            order_column: Column to determine latest record

        Returns:
            Deduplicated DataFrame
        """
        try:
            initial_count = df.count()
            logger.info(f"Removing duplicates based on: {key_columns}")

            # Add row number partitioned by key columns, ordered by order_column descending
            window_spec = Window.partitionBy(*key_columns).orderBy(desc(order_column))

            df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))

            # Keep only the first row (latest) for each key
            df_deduped = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

            final_count = df_deduped.count()
            removed_count = initial_count - final_count

            logger.info(f"Removed {removed_count} duplicate rows")
            return df_deduped

        except Exception as e:
            logger.error(f"Error removing duplicates: {str(e)}")
            return df


class SCD2Processor:
    """Handles SCD Type 2 processing"""

    @staticmethod
    def add_scd2_columns(df: DataFrame) -> DataFrame:
        """
        Add SCD Type 2 columns to DataFrame

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with SCD Type 2 columns
        """
        try:
            logger.info("Adding SCD Type 2 columns")

            current_ts = current_timestamp()

            df_scd2 = df \
                .withColumn("IsActive", lit(True).cast(BooleanType())) \
                .withColumn("StartDate", current_ts) \
                .withColumn("EndDate", lit(None).cast(TimestampType())) \
                .withColumn("OpTs", current_ts)

            logger.info("SCD Type 2 columns added successfully")
            return df_scd2

        except Exception as e:
            logger.error(f"Error adding SCD Type 2 columns: {str(e)}")
            return df

    @staticmethod
    def validate_scd2_columns(df: DataFrame) -> bool:
        """
        Validate that DataFrame has all required SCD Type 2 columns

        Args:
            df: DataFrame to validate

        Returns:
            True if all SCD Type 2 columns present
        """
        required_columns = ["IsActive", "StartDate", "EndDate", "OpTs"]

        for col_name in required_columns:
            if col_name not in df.columns:
                logger.error(f"Missing SCD Type 2 column: {col_name}")
                return False

        logger.info("SCD Type 2 columns validated successfully")
        return True


class CustomerAggregator:
    """Handles customer aggregation logic"""

    @staticmethod
    def calculate_customer_aggregate_spend(customer_df: DataFrame,
                                          order_df: DataFrame) -> DataFrame:
        """
        Calculate customer aggregate spend metrics

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            DataFrame with customer aggregate metrics
        """
        try:
            logger.info("Calculating customer aggregate spend")

            # Join customer and order data
            joined_df = order_df.join(
                customer_df.select("customer_id", "customer_name"),
                on="customer_id",
                how="inner"
            )

            # Calculate aggregations
            agg_df = joined_df.groupBy("customer_id", "customer_name").agg(
                count("order_id").alias("total_orders"),
                _sum("order_amount").alias("total_spend"),
                avg("order_amount").alias("avg_order_value"),
                _min("order_date").alias("first_order_date"),
                _max("order_date").alias("last_order_date")
            )

            # Cast decimal columns
            agg_df = agg_df \
                .withColumn("total_spend", col("total_spend").cast(DecimalType(20, 2))) \
                .withColumn("avg_order_value", col("avg_order_value").cast(DecimalType(20, 2)))

            record_count = agg_df.count()
            logger.info(f"Calculated aggregates for {record_count} customers")

            return agg_df

        except Exception as e:
            logger.error(f"Error calculating customer aggregate spend: {str(e)}")
            raise


class GlueCatalogManager:
    """Manages AWS Glue Catalog operations"""

    def __init__(self, database_name: str):
        self.database_name = database_name
        self.glue_client = boto3.client('glue')

    def create_database_if_not_exists(self) -> bool:
        """
        Create Glue Catalog database if it doesn't exist

        Returns:
            True if successful
        """
        try:
            logger.info(f"Creating Glue Catalog database: {self.database_name}")

            try:
                self.glue_client.get_database(Name=self.database_name)
                logger.info(f"Database already exists: {self.database_name}")
                return True
            except self.glue_client.exceptions.EntityNotFoundException:
                self.glue_client.create_database(
                    DatabaseInput={
                        'Name': self.database_name,
                        'Description': 'SDLC Wizard database for customer order analytics'
                    }
                )
                logger.info(f"Database created successfully: {self.database_name}")
                return True

        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            return False

    def register_table(self, table_name: str, s3_path: str,
                      columns: List[Dict[str, str]]) -> bool:
        """
        Register table in Glue Catalog

        Args:
            table_name: Name of the table
            s3_path: S3 location of the table
            columns: List of column definitions

        Returns:
            True if successful
        """
        try:
            logger.info(f"Registering table in Glue Catalog: {table_name}")

            storage_descriptor = {
                'Columns': columns,
                'Location': s3_path,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            }

            try:
                self.glue_client.get_table(
                    DatabaseName=self.database_name,
                    Name=table_name
                )
                # Update existing table
                self.glue_client.update_table(
                    DatabaseName=self.database_name,
                    TableInput={
                        'Name': table_name,
                        'StorageDescriptor': storage_descriptor
                    }
                )
                logger.info(f"Table updated: {table_name}")
            except self.glue_client.exceptions.EntityNotFoundException:
                # Create new table
                self.glue_client.create_table(
                    DatabaseName=self.database_name,
                    TableInput={
                        'Name': table_name,
                        'StorageDescriptor': storage_descriptor
                    }
                )
                logger.info(f"Table created: {table_name}")

            return True

        except Exception as e:
            logger.error(f"Error registering table {table_name}: {str(e)}")
            return False


class CustomerOrderETLJob:
    """Main ETL Job orchestrator"""

    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.context_manager = SparkContextManager()
        self.spark: Optional[SparkSession] = None
        self.glue_context: Optional[GlueContext] = None
        self.s3_manager: Optional[S3DataManager] = None
        self.catalog_manager: Optional[GlueCatalogManager] = None

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            # Try to load from S3 if path starts with s3://
            if config_path.startswith('s3://'):
                s3_client = boto3.client('s3')
                path_parts = config_path.replace('s3://', '').split('/', 1)
                bucket = path_parts[0]
                key = path_parts[1]

                response = s3_client.get_object(Bucket=bucket, Key=key)
                config_content = response['Body'].read().decode('utf-8')
                config = yaml.safe_load(config_content)
            else:
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)

            logger.info(f"Configuration loaded from: {config_path}")
            return config

        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def initialize(self):
        """Initialize all components"""
        try:
            logger.info("Initializing ETL job components")

            # Initialize Spark contexts
            sc, glue_ctx, spark = self.context_manager.initialize_spark_contexts()
            self.spark = spark
            self.glue_context = glue_ctx

            # Initialize managers
            self.s3_manager = S3DataManager(self.spark)
            self.catalog_manager = GlueCatalogManager(
                self.config['glue_catalog']['database']
            )

            # Create Glue Catalog database
            self.catalog_manager.create_database_if_not_exists()

            logger.info("ETL job components initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize ETL job: {str(e)}")
            raise

    def ingest_data(self) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
        """
        Ingest customer and order data from S3

        Returns:
            Tuple of (customer_df, order_df)
        """
        try:
            logger.info("Starting data ingestion")

            # Read customer data
            customer_path = self.config['source_paths']['customer']
            customer_df = self.s3_manager.read_data_safe(customer_path, file_format="parquet")

            if customer_df is None:
                logger.error("Failed to read customer data")
                return None, None

            # Read order data
            order_path = self.config['source_paths']['order']
            order_df = self.s3_manager.read_data_safe(order_path, file_format="parquet")

            if order_df is None:
                logger.error("Failed to read order data")
                return None, None

            logger.info("Data ingestion completed successfully")
            return customer_df, order_df

        except Exception as e:
            logger.error(f"Error during data ingestion: {str(e)}")
            return None, None

    def clean_data(self, customer_df: DataFrame, order_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Clean customer and order data

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Tuple of cleaned (customer_df, order_df)
        """
        try:
            logger.info("Starting data cleaning")

            cleaner = DataCleaner()
            cleaning_config = self.config['data_cleaning']

            # Clean customer data
            logger.info("Cleaning customer data")
            customer_cleaned = customer_df

            if cleaning_config['remove_null_strings']:
                customer_cleaned = cleaner.remove_null_strings(
                    customer_cleaned,
                    cleaning_config['null_string_values']
                )

            if cleaning_config['remove_nulls']:
                customer_cleaned = cleaner.remove_nulls(
                    customer_cleaned,
                    cleaning_config['required_fields']['customer']
                )

            if cleaning_config['remove_duplicates']:
                customer_cleaned = cleaner.remove_duplicates(
                    customer_cleaned,
                    ['customer_id'],
                    'registration_date'
                )

            # Clean order data
            logger.info("Cleaning order data")
            order_cleaned = order_df

            if cleaning_config['remove_null_strings']:
                order_cleaned = cleaner.remove_null_strings(
                    order_cleaned,
                    cleaning_config['null_string_values']
                )

            if cleaning_config['remove_nulls']:
                order_cleaned = cleaner.remove_nulls(
                    order_cleaned,
                    cleaning_config['required_fields']['order']
                )

            if cleaning_config['remove_duplicates']:
                order_cleaned = cleaner.remove_duplicates(
                    order_cleaned,
                    ['order_id'],
                    'order_date'
                )

            logger.info("Data cleaning completed successfully")
            return customer_cleaned, order_cleaned

        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise

    def process_scd2(self, customer_df: DataFrame, order_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Process SCD Type 2 for customer and order data

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Tuple of (customer_df, order_df) with SCD Type 2 columns
        """
        try:
            logger.info("Processing SCD Type 2")

            processor = SCD2Processor()

            # Add SCD Type 2 columns
            customer_scd2 = processor.add_scd2_columns(customer_df)
            order_scd2 = processor.add_scd2_columns(order_df)

            # Validate SCD Type 2 columns
            if not processor.validate_scd2_columns(customer_scd2):
                raise ValueError("Customer DataFrame missing SCD Type 2 columns")

            if not processor.validate_scd2_columns(order_scd2):
                raise ValueError("Order DataFrame missing SCD Type 2 columns")

            logger.info("SCD Type 2 processing completed successfully")
            return customer_scd2, order_scd2

        except Exception as e:
            logger.error(f"Error during SCD Type 2 processing: {str(e)}")
            raise

    def calculate_aggregations(self, customer_df: DataFrame, order_df: DataFrame) -> DataFrame:
        """
        Calculate customer aggregate spend

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Customer aggregate DataFrame
        """
        try:
            logger.info("Calculating aggregations")

            aggregator = CustomerAggregator()

            # Calculate customer aggregate spend
            agg_df = aggregator.calculate_customer_aggregate_spend(customer_df, order_df)

            # Add SCD Type 2 columns to aggregation
            processor = SCD2Processor()
            agg_scd2 = processor.add_scd2_columns(agg_df)

            logger.info("Aggregations calculated successfully")
            return agg_scd2

        except Exception as e:
            logger.error(f"Error calculating aggregations: {str(e)}")
            raise

    def write_results(self, order_df: DataFrame, customer_agg_df: DataFrame) -> bool:
        """
        Write results to S3 using Hudi format

        Args:
            order_df: Order DataFrame with SCD Type 2
            customer_agg_df: Customer aggregate DataFrame

        Returns:
            True if successful
        """
        try:
            logger.info("Writing results to S3")

            hudi_config = self.config['hudi_config']
            target_paths = self.config['target_paths']

            # Write order summary
            logger.info("Writing order summary")
            success_order = self.s3_manager.write_hudi_safe(
                df=order_df,
                s3_path=target_paths['order_summary'],
                table_name=hudi_config['table_names']['order_summary'],
                record_key=hudi_config['record_keys']['order'],
                precombine_key=hudi_config['precombine_field'],
                operation=hudi_config['operation']
            )

            if not success_order:
                logger.error("Failed to write order summary")
                return False

            # Write customer aggregate spend
            logger.info("Writing customer aggregate spend")
            success_agg = self.s3_manager.write_hudi_safe(
                df=customer_agg_df,
                s3_path=target_paths['customer_aggregate'],
                table_name=hudi_config['table_names']['customer_aggregate'],
                record_key=hudi_config['record_keys']['customer_aggregate'],
                precombine_key=hudi_config['precombine_field'],
                operation=hudi_config['operation']
            )

            if not success_agg:
                logger.error("Failed to write customer aggregate spend")
                return False

            logger.info("Results written successfully")
            return True

        except Exception as e:
            logger.error(f"Error writing results: {str(e)}")
            return False

    def register_catalog_tables(self) -> bool:
        """
        Register tables in Glue Catalog

        Returns:
            True if successful
        """
        try:
            logger.info("Registering tables in Glue Catalog")

            target_paths = self.config['target_paths']

            # Define customer aggregate spend columns
            agg_columns = [
                {'Name': 'customer_id', 'Type': 'string'},
                {'Name': 'customer_name', 'Type': 'string'},
                {'Name': 'total_orders', 'Type': 'bigint'},
                {'Name': 'total_spend', 'Type': 'decimal(20,2)'},
                {'Name': 'avg_order_value', 'Type': 'decimal(20,2)'},
                {'Name': 'first_order_date', 'Type': 'timestamp'},
                {'Name': 'last_order_date', 'Type': 'timestamp'},
                {'Name': 'IsActive', 'Type': 'boolean'},
                {'Name': 'StartDate', 'Type': 'timestamp'},
                {'Name': 'EndDate', 'Type': 'timestamp'},
                {'Name': 'OpTs', 'Type': 'timestamp'}
            ]

            # Register customer aggregate spend table
            success = self.catalog_manager.register_table(
                table_name='customer_aggregate_spend',
                s3_path=target_paths['customer_aggregate'],
                columns=agg_columns
            )

            if not success:
                logger.error("Failed to register customer aggregate spend table")
                return False

            logger.info("Tables registered successfully in Glue Catalog")
            return True

        except Exception as e:
            logger.error(f"Error registering catalog tables: {str(e)}")
            return False

    def run(self) -> bool:
        """
        Run the complete ETL job

        Returns:
            True if successful
        """
        try:
            logger.info("=" * 80)
            logger.info("Starting Customer Order SCD Type 2 ETL Job")
            logger.info("=" * 80)

            # Initialize
            self.initialize()

            # Stage 1: Ingest data
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 1: Data Ingestion")
            logger.info("=" * 80)
            customer_df, order_df = self.ingest_data()

            if customer_df is None or order_df is None:
                logger.error("Data ingestion failed")
                return False

            # Stage 2: Clean data
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 2: Data Cleaning")
            logger.info("=" * 80)
            customer_cleaned, order_cleaned = self.clean_data(customer_df, order_df)

            # Stage 3: Process SCD Type 2
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 3: SCD Type 2 Processing")
            logger.info("=" * 80)
            customer_scd2, order_scd2 = self.process_scd2(customer_cleaned, order_cleaned)

            # Stage 4: Calculate aggregations
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 4: Aggregation Calculation")
            logger.info("=" * 80)
            customer_agg = self.calculate_aggregations(customer_scd2, order_scd2)

            # Stage 5: Write results
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 5: Writing Results")
            logger.info("=" * 80)
            write_success = self.write_results(order_scd2, customer_agg)

            if not write_success:
                logger.error("Failed to write results")
                return False

            # Stage 6: Register catalog tables
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 6: Glue Catalog Registration")
            logger.info("=" * 80)
            catalog_success = self.register_catalog_tables()

            if not catalog_success:
                logger.warning("Failed to register catalog tables (non-critical)")

            logger.info("\n" + "=" * 80)
            logger.info("ETL Job Completed Successfully")
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}")
            return False

        finally:
            # Cleanup
            self.context_manager.cleanup()


def main():
    """Main entry point for the Glue job"""
    try:
        # Get configuration path from job arguments
        args = getResolvedOptions(sys.argv, ['config_path'])
        config_path = args['config_path']

        # Create and run ETL job
        job = CustomerOrderETLJob(config_path)
        success = job.run()

        if not success:
            logger.error("ETL job failed")
            sys.exit(1)

        logger.info("ETL job completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()