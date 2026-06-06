"""
AWS Glue PySpark Job - Customer and Order Data Processing

This job processes customer and order data from S3, performs data cleaning,
applies transformations, and writes curated data to S3 with Glue Data Catalog integration.
"""

import sys
import yaml
import os
from datetime import datetime
from decimal import Decimal

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as _sum,
    when, trim, lower, to_date, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, BooleanType, TimestampType
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


class SparkContextManager:
    """Manages Spark and Glue contexts initialization"""

    @staticmethod
    def initialize_spark_contexts():
        """
        Initialize Spark, Glue contexts and Job
        Returns: tuple (spark, glueContext, job, logger)
        """
        try:
            # Check if running in test environment
            if hasattr(__builtins__, 'spark') and __builtins__.spark is not None:
                spark = __builtins__.spark
                sc = spark.sparkContext
                glueContext = GlueContext(sc)
                job = Job(glueContext)
                logger = glueContext.get_logger()
                return spark, glueContext, job, logger

            # Production environment
            sc = SparkContext.getOrCreate()
            glueContext = GlueContext(sc)
            spark = glueContext.spark_session
            job = Job(glueContext)
            logger = glueContext.get_logger()

            return spark, glueContext, job, logger

        except Exception as e:
            print(f"Error initializing Spark contexts: {str(e)}")
            raise


class ConfigManager:
    """Manages configuration loading and parameter retrieval"""

    @staticmethod
    def load_config(config_path="config/glue_params.yaml"):
        """Load configuration from YAML file"""
        try:
            # Try multiple possible paths
            possible_paths = [
                config_path,
                os.path.join(os.path.dirname(__file__), "..", "..", config_path),
                os.path.join(os.path.dirname(__file__), "..", "..", "..", config_path)
            ]

            for path in possible_paths:
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        return yaml.safe_load(f)

            # If file not found, return default config
            return ConfigManager._get_default_config()

        except Exception as e:
            print(f"Warning: Could not load config file: {str(e)}")
            return ConfigManager._get_default_config()

    @staticmethod
    def _get_default_config():
        """Return default configuration"""
        return {
            'inputs': {
                'customer': {
                    'source_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
                    'source_format': 'csv',
                    'options': {'header': 'true', 'inferSchema': 'false'}
                },
                'order': {
                    'source_path': 's3://adif-sdlc/sdlc_wizard/orderdata/',
                    'source_format': 'csv',
                    'options': {'header': 'true', 'inferSchema': 'false'}
                }
            },
            'outputs': {
                'customer_curated': {
                    'target_path': 's3://adif-sdlc/curated/sdlc_wizard/customer/',
                    'target_format': 'parquet',
                    'mode': 'overwrite'
                },
                'order_curated': {
                    'target_path': 's3://adif-sdlc/curated/sdlc_wizard/order/',
                    'target_format': 'parquet',
                    'mode': 'overwrite'
                },
                'order_summary': {
                    'target_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
                    'target_format': 'parquet',
                    'mode': 'overwrite'
                },
                'customer_aggregate_spend': {
                    'target_path': 's3://adif-sdlc/analytics/customeraggregatespend/',
                    'target_format': 'parquet',
                    'mode': 'overwrite'
                }
            },
            'catalog': {
                'database_name': 'gen_ai_poc_databrickscoe',
                'tables': {
                    'customer': 'sdlc_wizard_customer',
                    'order': 'sdlc_wizard_order',
                    'order_summary': 'sdlc_wizard_ordersummary',
                    'customer_aggregate': 'customer_aggregate_spend'
                }
            },
            'hudi': {
                'enabled': True,
                'checkpoint_path': 's3://adif-sdlc/checkpoints/customer_baseline/'
            },
            'data_quality': {
                'remove_nulls': True,
                'remove_null_strings': True,
                'remove_duplicates': True,
                'null_string_values': ['Null', 'NULL', 'null']
            }
        }

    @staticmethod
    def get_job_parameters():
        """Get job parameters from config"""
        config = ConfigManager.load_config()
        return config


class S3DataReader:
    """Handles reading data from S3"""

    def __init__(self, spark):
        self.spark = spark

    def read_data_safe(self, source_path, source_format, options=None):
        """
        Safely read data from S3 with error handling

        Args:
            source_path: S3 path to read from
            source_format: Format of the data (csv, parquet, etc.)
            options: Dictionary of read options

        Returns:
            DataFrame or None if read fails
        """
        try:
            if options is None:
                options = {}

            reader = self.spark.read.format(source_format)

            for key, value in options.items():
                reader = reader.option(key, value)

            df = reader.load(source_path)

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            return df

        except Exception as e:
            print(f"Error reading data from {source_path}: {str(e)}")
            return None


class S3DataWriter:
    """Handles writing data to S3"""

    def __init__(self, spark):
        self.spark = spark

    def _write_parquet(self, df, target_path, mode, partition_by=None):
        """Internal method to write Parquet format"""
        writer = df.write.format("parquet").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)

    def _write_csv(self, df, target_path, mode, options=None):
        """Internal method to write CSV format"""
        writer = df.write.format("csv").mode(mode)
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        writer.save(target_path)

    def _write_json(self, df, target_path, mode):
        """Internal method to write JSON format"""
        df.write.format("json").mode(mode).save(target_path)

    def write_data_safe(self, df, target_path, target_format, mode="overwrite", partition_by=None, options=None):
        """
        Safely write data to S3 with error handling

        Args:
            df: DataFrame to write
            target_path: S3 path to write to
            target_format: Format to write (parquet, csv, json)
            mode: Write mode (overwrite, append, etc.)
            partition_by: List of columns to partition by
            options: Dictionary of write options

        Returns:
            bool: True if write successful, False otherwise
        """
        try:
            if df is None or df.rdd.isEmpty():
                print(f"Warning: Empty DataFrame, skipping write to {target_path}")
                return False

            if target_format.lower() == "parquet":
                self._write_parquet(df, target_path, mode, partition_by)
            elif target_format.lower() == "csv":
                self._write_csv(df, target_path, mode, options)
            elif target_format.lower() == "json":
                self._write_json(df, target_path, mode)
            else:
                print(f"Unsupported format: {target_format}")
                return False

            return True

        except Exception as e:
            print(f"Error writing data to {target_path}: {str(e)}")
            return False


class DataCleaner:
    """Handles data cleaning operations"""

    @staticmethod
    def clean_nulls(df, null_string_values=None):
        """
        Remove rows with NULL values or string 'Null' values

        Args:
            df: Input DataFrame
            null_string_values: List of string values to treat as null

        Returns:
            Cleaned DataFrame
        """
        if null_string_values is None:
            null_string_values = ['Null', 'NULL', 'null']

        # Remove rows with actual NULL values
        df_cleaned = df.na.drop()

        # Remove rows with string 'Null' values
        for column in df_cleaned.columns:
            for null_val in null_string_values:
                df_cleaned = df_cleaned.filter(
                    (col(column) != null_val) | col(column).isNull()
                )

        return df_cleaned

    @staticmethod
    def remove_duplicates(df):
        """
        Remove duplicate rows

        Args:
            df: Input DataFrame

        Returns:
            DataFrame without duplicates
        """
        return df.dropDuplicates()

    @staticmethod
    def clean_dataframe(df, config):
        """
        Apply all cleaning operations based on config

        Args:
            df: Input DataFrame
            config: Data quality configuration

        Returns:
            Cleaned DataFrame
        """
        if config.get('remove_nulls', True) or config.get('remove_null_strings', True):
            null_values = config.get('null_string_values', ['Null', 'NULL', 'null'])
            df = DataCleaner.clean_nulls(df, null_values)

        if config.get('remove_duplicates', True):
            df = DataCleaner.remove_duplicates(df)

        return df


class SchemaValidator:
    """Validates and applies schemas to DataFrames"""

    @staticmethod
    def get_customer_schema():
        """Return customer schema definition"""
        return StructType([
            StructField("custid", StringType(), False),
            StructField("name", StringType(), False),
            StructField("emailid", StringType(), False),
            StructField("region", StringType(), False)
        ])

    @staticmethod
    def get_order_schema():
        """Return order schema definition"""
        return StructType([
            StructField("orderid", StringType(), False),
            StructField("itemname", StringType(), False),
            StructField("priceperunit", DecimalType(10, 2), False),
            StructField("qty", IntegerType(), False),
            StructField("date", StringType(), False),
            StructField("custid", StringType(), False)
        ])

    @staticmethod
    def apply_schema(df, schema):
        """
        Apply schema to DataFrame with type casting

        Args:
            df: Input DataFrame
            schema: Target schema

        Returns:
            DataFrame with applied schema
        """
        try:
            # Create select expressions for casting
            select_exprs = []
            for field in schema.fields:
                if field.name in df.columns:
                    select_exprs.append(col(field.name).cast(field.dataType).alias(field.name))

            return df.select(*select_exprs)

        except Exception as e:
            print(f"Error applying schema: {str(e)}")
            return df


class SCD2Manager:
    """Manages SCD Type 2 operations with Hudi"""

    @staticmethod
    def add_scd2_columns(df):
        """
        Add SCD Type 2 tracking columns

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with SCD2 columns
        """
        current_ts = current_timestamp()

        df_scd2 = df.withColumn("isactive", lit(True).cast(BooleanType())) \
                    .withColumn("startdate", current_ts.cast(TimestampType())) \
                    .withColumn("enddate", lit(None).cast(TimestampType())) \
                    .withColumn("opts", current_ts.cast(TimestampType()))

        return df_scd2

    @staticmethod
    def write_hudi_table(df, target_path, hudi_config, table_name):
        """
        Write DataFrame to Hudi table with SCD2 support

        Args:
            df: Input DataFrame
            target_path: S3 path for Hudi table
            hudi_config: Hudi configuration
            table_name: Name of the table

        Returns:
            bool: True if write successful
        """
        try:
            if not hudi_config.get('enabled', False):
                return False

            hudi_options = {
                'hoodie.table.name': table_name,
                'hoodie.datasource.write.recordkey.field': hudi_config.get('record_key', 'CustId'),
                'hoodie.datasource.write.precombine.field': hudi_config.get('precombine_field', 'OpTs'),
                'hoodie.datasource.write.operation': hudi_config.get('operation', 'upsert'),
                'hoodie.datasource.write.table.type': hudi_config.get('table_type', 'COPY_ON_WRITE'),
                'hoodie.datasource.hive_sync.enable': 'true' if hudi_config.get('hive_sync', {}).get('enabled', True) else 'false',
                'hoodie.datasource.hive_sync.mode': hudi_config.get('hive_sync', {}).get('mode', 'hms')
            }

            df.write.format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(target_path)

            return True

        except Exception as e:
            print(f"Error writing Hudi table: {str(e)}")
            return False


class AggregationEngine:
    """Handles data aggregation operations"""

    @staticmethod
    def calculate_customer_aggregate_spend(df_order, df_customer):
        """
        Calculate total spending per customer by aggregating order amounts

        Args:
            df_order: Order DataFrame
            df_customer: Customer DataFrame

        Returns:
            DataFrame with customer aggregate spending
        """
        try:
            # Calculate order amount (PricePerUnit * Qty)
            df_order_amount = df_order.withColumn(
                "orderamount",
                spark_round(col("priceperunit") * col("qty"), 2)
            )

            # Join with customer to get customer name
            df_joined = df_order_amount.join(
                df_customer.select("custid", "name"),
                on="custid",
                how="inner"
            )

            # Aggregate by customer name and date
            df_aggregate = df_joined.groupBy("name", "date") \
                .agg(_sum("orderamount").alias("totalspend"))

            # Round total spend
            df_aggregate = df_aggregate.withColumn(
                "totalspend",
                spark_round(col("totalspend"), 2)
            )

            return df_aggregate

        except Exception as e:
            print(f"Error calculating customer aggregate spend: {str(e)}")
            return None


class CatalogManager:
    """Manages AWS Glue Data Catalog operations"""

    @staticmethod
    def register_table(spark, df, database_name, table_name, target_path):
        """
        Register DataFrame as table in Glue Data Catalog

        Args:
            spark: Spark session
            df: DataFrame to register
            database_name: Glue database name
            table_name: Table name
            target_path: S3 path where data is stored

        Returns:
            bool: True if registration successful
        """
        try:
            # Create temp view
            df.createOrReplaceTempView(f"temp_{table_name}")

            # Register as table
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
                USING parquet
                LOCATION '{target_path}'
                AS SELECT * FROM temp_{table_name}
            """)

            return True

        except Exception as e:
            print(f"Error registering table {table_name}: {str(e)}")
            return False


def main():
    """Main job execution function"""

    # Initialize Spark contexts
    spark, glueContext, job, logger = SparkContextManager.initialize_spark_contexts()

    try:
        # Initialize job
        args = getResolvedOptions(sys.argv, ['JOB_NAME']) if '--JOB_NAME' in sys.argv else {'JOB_NAME': 'customer_order_processing_job'}
        job.init(args['JOB_NAME'], args)

        logger.info("Starting Customer and Order Data Processing Job")

        # Load configuration
        config = ConfigManager.get_job_parameters()

        # Initialize components
        reader = S3DataReader(spark)
        writer = S3DataWriter(spark)

        # TR-INGEST-001: Load Customer CSV Files from S3
        logger.info("TR-INGEST-001: Loading customer data from S3")
        customer_config = config['inputs']['customer']
        df_customer_raw = reader.read_data_safe(
            customer_config['source_path'],
            customer_config['source_format'],
            customer_config.get('options', {})
        )

        if df_customer_raw is None:
            raise Exception("Failed to load customer data")

        logger.info(f"Loaded {df_customer_raw.count()} customer records")

        # TR-INGEST-002: Load Order CSV Files from S3
        logger.info("TR-INGEST-002: Loading order data from S3")
        order_config = config['inputs']['order']
        df_order_raw = reader.read_data_safe(
            order_config['source_path'],
            order_config['source_format'],
            order_config.get('options', {})
        )

        if df_order_raw is None:
            raise Exception("Failed to load order data")

        logger.info(f"Loaded {df_order_raw.count()} order records")

        # Apply schemas
        df_customer_typed = SchemaValidator.apply_schema(
            df_customer_raw,
            SchemaValidator.get_customer_schema()
        )

        df_order_typed = SchemaValidator.apply_schema(
            df_order_raw,
            SchemaValidator.get_order_schema()
        )

        # TR-CLEAN-001: Clean customer data
        logger.info("TR-CLEAN-001: Cleaning customer data")
        dq_config = config.get('data_quality', {})
        df_customer_clean = DataCleaner.clean_dataframe(df_customer_typed, dq_config)
        logger.info(f"Cleaned customer records: {df_customer_clean.count()}")

        # TR-CLEAN-002: Clean order data
        logger.info("TR-CLEAN-002: Cleaning order data")
        df_order_clean = DataCleaner.clean_dataframe(df_order_typed, dq_config)
        logger.info(f"Cleaned order records: {df_order_clean.count()}")

        # Add SCD2 columns to customer data
        df_customer_scd2 = SCD2Manager.add_scd2_columns(df_customer_clean)

        # TR-CATALOG-001: Write customer data to S3 and register in catalog
        logger.info("TR-CATALOG-001: Writing customer data to S3")
        customer_output = config['outputs']['customer_curated']
        write_success = writer.write_data_safe(
            df_customer_scd2,
            customer_output['target_path'],
            customer_output['target_format'],
            customer_output.get('mode', 'overwrite'),
            customer_output.get('partition_by', [])
        )

        if write_success:
            catalog_config = config.get('catalog', {})
            CatalogManager.register_table(
                spark,
                df_customer_scd2,
                catalog_config.get('database_name', 'gen_ai_poc_databrickscoe'),
                catalog_config.get('tables', {}).get('customer', 'sdlc_wizard_customer'),
                customer_output['target_path']
            )
            logger.info("Customer data written and cataloged successfully")

        # TR-CATALOG-002: Write order data to S3 and register in catalog
        logger.info("TR-CATALOG-002: Writing order data to S3")
        order_output = config['outputs']['order_curated']
        write_success = writer.write_data_safe(
            df_order_clean,
            order_output['target_path'],
            order_output['target_format'],
            order_output.get('mode', 'overwrite'),
            order_output.get('partition_by', [])
        )

        if write_success:
            CatalogManager.register_table(
                spark,
                df_order_clean,
                catalog_config.get('database_name', 'gen_ai_poc_databrickscoe'),
                catalog_config.get('tables', {}).get('order', 'sdlc_wizard_order'),
                order_output['target_path']
            )
            logger.info("Order data written and cataloged successfully")

        # TR-AGG-001: Calculate customer aggregate spending
        logger.info("TR-AGG-001: Calculating customer aggregate spending")
        df_aggregate = AggregationEngine.calculate_customer_aggregate_spend(
            df_order_clean,
            df_customer_clean
        )

        if df_aggregate is not None:
            logger.info(f"Generated {df_aggregate.count()} aggregate records")

            # Write aggregate data
            aggregate_output = config['outputs']['customer_aggregate_spend']
            write_success = writer.write_data_safe(
                df_aggregate,
                aggregate_output['target_path'],
                aggregate_output['target_format'],
                aggregate_output.get('mode', 'overwrite'),
                aggregate_output.get('partition_by', [])
            )

            if write_success:
                CatalogManager.register_table(
                    spark,
                    df_aggregate,
                    catalog_config.get('database_name', 'gen_ai_poc_databrickscoe'),
                    catalog_config.get('tables', {}).get('customer_aggregate', 'customer_aggregate_spend'),
                    aggregate_output['target_path']
                )
                logger.info("Customer aggregate spending written and cataloged successfully")

        # Write Hudi table for SCD2 tracking
        hudi_config = config.get('hudi', {})
        if hudi_config.get('enabled', False):
            logger.info("Writing Hudi table for SCD2 tracking")
            SCD2Manager.write_hudi_table(
                df_customer_scd2,
                hudi_config.get('checkpoint_path', 's3://adif-sdlc/checkpoints/customer_baseline/'),
                hudi_config,
                'customer_baseline'
            )

        logger.info("Job completed successfully")
        job.commit()

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        # Don't stop spark if it was provided by test environment
        if not (hasattr(__builtins__, 'spark') and __builtins__.spark is not None):
            spark.stop()


if __name__ == "__main__":
    main()