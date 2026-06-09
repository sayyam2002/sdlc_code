"""
AWS Glue PySpark ETL Job - Customer Order Pipeline

This job implements:
- Customer and Order data ingestion from S3 CSV
- Data quality transformations (null removal, deduplication)
- AWS Glue Data Catalog registration
- SCD Type 2 implementation using Apache Hudi
- Customer aggregate spend analytics
"""

import sys
import os
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, lit, current_timestamp, to_date
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import yaml


class SparkContextManager:
    """Manages Spark and Glue contexts initialization"""

    @staticmethod
    def initialize_spark_contexts():
        """
        Initialize Spark, Glue contexts and Job
        Returns: tuple (spark, glueContext, job, logger)
        """
        sc = SparkContext.getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

        # Configure Spark for better performance
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

        logger = glueContext.get_logger()

        # Initialize Glue Job
        job = Job(glueContext)

        return spark, glueContext, job, logger


class ConfigManager:
    """Manages configuration loading and parameter resolution"""

    @staticmethod
    def get_job_parameters():
        """
        Load job parameters from YAML config or Glue job arguments
        Returns: dict of parameters
        """
        # Try to load from YAML config file
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'config',
            'glue_params.yaml'
        )

        params = {}

        # Load from YAML if exists
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                params = yaml.safe_load(f)

        # Override with Glue job arguments if available
        try:
            args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            params['job_name'] = args['JOB_NAME']

            # Try to get optional parameters
            optional_args = [
                'inputs_source_customer_path',
                'inputs_source_order_path',
                'outputs_target_customer_path',
                'outputs_target_order_path',
                'outputs_target_ordersummary_path',
                'outputs_target_analytics_path',
                'catalog_database_name'
            ]

            for arg in optional_args:
                try:
                    resolved = getResolvedOptions(sys.argv, [arg])
                    params[arg] = resolved[arg]
                except:
                    pass

        except:
            params['job_name'] = 'customer_order_pipeline'

        return params


class S3DataReader:
    """Handles reading data from S3 with validation"""

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def _read_csv_internal(self, path, delimiter=",", header="true"):
        """Internal method to read CSV from S3"""
        return self.spark.read.format("csv") \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .option("inferSchema", "true") \
            .load(path)

    def read_data_safe(self, path, expected_schema, data_type, delimiter=",", header="true"):
        """
        Safely read data from S3 with schema validation

        Args:
            path: S3 path to read from
            expected_schema: List of expected column names
            data_type: String describing data type (for logging)
            delimiter: CSV delimiter
            header: Whether CSV has header

        Returns:
            DataFrame or None if error
        """
        try:
            self.logger.info(f"Reading {data_type} data from: {path}")

            # Read CSV data
            df = self._read_csv_internal(path, delimiter, header)

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            # Check if empty
            if df.count() == 0:
                self.logger.warn(f"Empty CSV file detected for {data_type}")
                return df

            # Validate schema
            actual_columns = df.columns
            expected_columns_lower = [c.lower() for c in expected_schema]

            if set(actual_columns) != set(expected_columns_lower):
                error_msg = f"Schema validation failed for {data_type}. Expected: {expected_columns_lower}, Found: {actual_columns}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            record_count = df.count()
            self.logger.info(f"Successfully loaded {record_count} {data_type} records from S3")

            return df

        except Exception as e:
            self.logger.error(f"Error reading {data_type} data from {path}: {str(e)}")
            raise


class DataQualityTransformer:
    """Handles data quality transformations"""

    def __init__(self, logger):
        self.logger = logger

    def remove_nulls(self, df, data_type):
        """
        Remove records with NULL values or string literal 'Null'

        Args:
            df: Input DataFrame
            data_type: String describing data type (for logging)

        Returns:
            Cleaned DataFrame
        """
        try:
            initial_count = df.count()

            # Remove rows with any NULL values
            df_clean = df.na.drop()

            # Remove rows with string literal "Null" in any column
            for column in df_clean.columns:
                df_clean = df_clean.filter(
                    (col(column) != "Null") &
                    (col(column) != "null") &
                    (col(column) != "NULL")
                )

            final_count = df_clean.count()
            removed_count = initial_count - final_count

            self.logger.info(f"Removed {removed_count} null records from {data_type}. Remaining: {final_count}")

            return df_clean

        except Exception as e:
            self.logger.error(f"Error removing nulls from {data_type}: {str(e)}")
            raise

    def remove_duplicates(self, df, data_type):
        """
        Remove duplicate records based on all columns

        Args:
            df: Input DataFrame
            data_type: String describing data type (for logging)

        Returns:
            Deduplicated DataFrame
        """
        try:
            initial_count = df.count()

            df_dedup = df.dropDuplicates()

            final_count = df_dedup.count()
            removed_count = initial_count - final_count

            self.logger.info(f"Removed {removed_count} duplicate records from {data_type}. Remaining: {final_count}")

            return df_dedup

        except Exception as e:
            self.logger.error(f"Error removing duplicates from {data_type}: {str(e)}")
            raise


class S3DataWriter:
    """Handles writing data to S3 and Glue Catalog"""

    def __init__(self, spark, glueContext, logger):
        self.spark = spark
        self.glueContext = glueContext
        self.logger = logger

    def _write_parquet(self, df, path):
        """Internal method to write Parquet to S3"""
        df.write.mode("overwrite").parquet(path)

    def _write_hudi(self, df, path, hudi_options):
        """Internal method to write Hudi to S3"""
        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)

    def write_data_safe(self, df, path, table_name, database_name, data_type):
        """
        Safely write data to S3 in Parquet format and register in Glue Catalog

        Args:
            df: DataFrame to write
            path: S3 path to write to
            table_name: Glue Catalog table name
            database_name: Glue Catalog database name
            data_type: String describing data type (for logging)
        """
        try:
            self.logger.info(f"Writing {data_type} data to: {path}")

            # Write to S3 in Parquet format
            self._write_parquet(df, path)

            # Register in Glue Catalog
            self.logger.info(f"Registering {table_name} in Glue Catalog database: {database_name}")

            df.write.mode("overwrite") \
                .format("parquet") \
                .option("path", path) \
                .saveAsTable(f"{database_name}.{table_name}")

            self.logger.info(f"Successfully wrote {df.count()} records to {path} and registered as {database_name}.{table_name}")

        except Exception as e:
            self.logger.error(f"Error writing {data_type} data to {path}: {str(e)}")
            raise

    def write_hudi_scd2(self, df, path, hudi_config, data_type):
        """
        Write data using Apache Hudi for SCD Type 2

        Args:
            df: DataFrame to write
            path: S3 path to write to
            hudi_config: Hudi configuration dict
            data_type: String describing data type (for logging)
        """
        try:
            self.logger.info(f"Writing {data_type} data to Hudi table: {path}")

            # Add SCD Type 2 columns
            df_scd = df.withColumn("IsActive", lit(True)) \
                .withColumn("StartDate", current_timestamp()) \
                .withColumn("EndDate", lit(None).cast("timestamp")) \
                .withColumn("OpTs", current_timestamp())

            # Hudi options
            hudi_options = {
                'hoodie.table.name': hudi_config.get('hudi_table_name', 'ordersummary'),
                'hoodie.datasource.write.recordkey.field': hudi_config.get('hudi_record_key', 'OrderId'),
                'hoodie.datasource.write.precombine.field': hudi_config.get('hudi_precombine_field', 'OpTs'),
                'hoodie.datasource.write.partitionpath.field': hudi_config.get('hudi_partition_field', 'Date'),
                'hoodie.datasource.write.operation': hudi_config.get('hudi_operation', 'upsert'),
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.upsert.shuffle.parallelism': '2',
                'hoodie.insert.shuffle.parallelism': '2'
            }

            self._write_hudi(df_scd, path, hudi_options)

            self.logger.info(f"Successfully wrote {df_scd.count()} records to Hudi table at {path}")

        except Exception as e:
            self.logger.error(f"Error writing {data_type} data to Hudi: {str(e)}")
            raise


class OrderSummaryProcessor:
    """Processes order summary with customer data"""

    def __init__(self, logger):
        self.logger = logger

    def create_order_summary(self, customer_df, order_df):
        """
        Create order summary by joining customer and order data

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Order summary DataFrame
        """
        try:
            self.logger.info("Creating order summary by joining customer and order data")

            # Join customer and order data
            order_summary = order_df.join(
                customer_df,
                order_df.custid == customer_df.custid,
                "inner"
            ).select(
                order_df.orderid,
                order_df.itemname,
                order_df.priceperunit,
                order_df.qty,
                order_df.date,
                order_df.custid,
                customer_df.name,
                customer_df.emailid,
                customer_df.region
            )

            # Calculate total amount
            order_summary = order_summary.withColumn(
                "TotalAmount",
                col("priceperunit") * col("qty")
            )

            record_count = order_summary.count()
            self.logger.info(f"Created order summary with {record_count} records")

            return order_summary

        except Exception as e:
            self.logger.error(f"Error creating order summary: {str(e)}")
            raise


class AggregationProcessor:
    """Processes customer aggregate spend analytics"""

    def __init__(self, logger):
        self.logger = logger

    def calculate_customer_aggregate_spend(self, order_summary_df):
        """
        Calculate total spending per customer per date

        Args:
            order_summary_df: Order summary DataFrame

        Returns:
            Customer aggregate spend DataFrame
        """
        try:
            self.logger.info("Calculating customer aggregate spend")

            # Group by customer and date, sum total amount
            aggregate_spend = order_summary_df.groupBy(
                "custid",
                "name",
                "emailid",
                "region",
                "date"
            ).agg(
                _sum("TotalAmount").alias("TotalSpend")
            )

            record_count = aggregate_spend.count()
            self.logger.info(f"Generated customer aggregate spend with {record_count} records")

            return aggregate_spend

        except Exception as e:
            self.logger.error(f"Error calculating customer aggregate spend: {str(e)}")
            raise


def main():
    """Main ETL job execution"""

    # Initialize Spark contexts
    spark, glueContext, job, logger = SparkContextManager.initialize_spark_contexts()

    try:
        logger.info("Starting Customer Order Pipeline ETL Job")

        # Load configuration
        params = ConfigManager.get_job_parameters()
        job.init(params.get('job_name', 'customer_order_pipeline'), params)

        # Initialize components
        reader = S3DataReader(spark, logger)
        quality = DataQualityTransformer(logger)
        writer = S3DataWriter(spark, glueContext, logger)
        order_processor = OrderSummaryProcessor(logger)
        agg_processor = AggregationProcessor(logger)

        # TR-INGEST-001: Read Customer Data
        customer_schema = ['CustId', 'Name', 'EmailId', 'Region']
        customer_df = reader.read_data_safe(
            path=params.get('inputs_source_customer_path', 's3://adif-sdlc/sdlc_wizard/customerdata/'),
            expected_schema=customer_schema,
            data_type='customer',
            delimiter=params.get('inputs_csv_delimiter', ','),
            header=params.get('inputs_csv_header', 'true')
        )

        # TR-INGEST-002: Read Order Data
        order_schema = ['OrderId', 'ItemName', 'PricePerUnit', 'Qty', 'Date', 'CustId']
        order_df = reader.read_data_safe(
            path=params.get('inputs_source_order_path', 's3://adif-sdlc/sdlc_wizard/orderdata/'),
            expected_schema=order_schema,
            data_type='order',
            delimiter=params.get('inputs_csv_delimiter', ','),
            header=params.get('inputs_csv_header', 'true')
        )

        # TR-CLEAN-001: Remove NULL values
        customer_df_clean = quality.remove_nulls(customer_df, 'customer')
        order_df_clean = quality.remove_nulls(order_df, 'order')

        # TR-CLEAN-002: Remove duplicates
        customer_df_final = quality.remove_duplicates(customer_df_clean, 'customer')
        order_df_final = quality.remove_duplicates(order_df_clean, 'order')

        # TR-CATALOG-001: Write Customer Data to Catalog
        writer.write_data_safe(
            df=customer_df_final,
            path=params.get('outputs_target_customer_path', 's3://adif-sdlc/curated/sdlc_wizard/customer/'),
            table_name=params.get('catalog_table_customer', 'sdlc_wizard_customer'),
            database_name=params.get('catalog_database_name', 'sdlc_wizard_db'),
            data_type='customer'
        )

        # TR-CATALOG-002: Write Order Data to Catalog
        writer.write_data_safe(
            df=order_df_final,
            path=params.get('outputs_target_order_path', 's3://adif-sdlc/curated/sdlc_wizard/order/'),
            table_name=params.get('catalog_table_order', 'sdlc_wizard_order'),
            database_name=params.get('catalog_database_name', 'sdlc_wizard_db'),
            data_type='order'
        )

        # Create Order Summary
        order_summary_df = order_processor.create_order_summary(customer_df_final, order_df_final)

        # SCD Type 2: Write Order Summary using Hudi
        if params.get('enable_scd_type2', 'true') == 'true':
            writer.write_hudi_scd2(
                df=order_summary_df,
                path=params.get('outputs_target_ordersummary_path', 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'),
                hudi_config=params,
                data_type='order_summary'
            )

        # Calculate Customer Aggregate Spend
        aggregate_spend_df = agg_processor.calculate_customer_aggregate_spend(order_summary_df)

        # Write Customer Aggregate Spend
        writer.write_data_safe(
            df=aggregate_spend_df,
            path=params.get('outputs_target_analytics_path', 's3://adif-sdlc/analytics/customeraggregatespend/'),
            table_name=params.get('catalog_table_analytics', 'customeraggregatespend'),
            database_name=params.get('catalog_database_name', 'sdlc_wizard_db'),
            data_type='customer_aggregate_spend'
        )

        logger.info("Customer Order Pipeline ETL Job completed successfully")
        job.commit()

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        # Don't stop spark if it was already running
        pass


if __name__ == "__main__":
    main()