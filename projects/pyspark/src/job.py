"""
PySpark Job for Customer Order Data Processing with SCD Type 2 using Hudi
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, row_number
)
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CustomerOrderProcessor:
    """
    Main processor class for customer order data pipeline
    """

    # S3 Paths from TRD
    CUSTOMER_DATA_PATH = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    ORDER_DATA_PATH = "s3://adif-sdlc/sdlc_wizard/orderdata/"
    CUSTOMER_AGGREGATE_SPEND_PATH = "s3://adif-sdlc/analytics/customeraggregatespend/"
    ATHENA_RESULTS_PATH = "s3://adif-sdlc/athena-results/"
    ORDER_SUMMARY_OUTPUT_PATH = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"

    # Glue Catalog Configuration
    GLUE_DATABASE = "sdlc_wizard"
    GLUE_TABLE_ORDER_SUMMARY = "ordersummary"

    # Hudi Configuration
    HUDI_TABLE_NAME = "ordersummary"
    HUDI_RECORD_KEY = "CustId,OrderId"
    HUDI_PRECOMBINE_FIELD = "OpTs"
    HUDI_PARTITION_FIELD = "Region"

    def __init__(self):
        """Initialize Spark and Glue contexts"""
        try:
            logger.info("Initializing Spark and Glue contexts")

            # Initialize Spark Context
            self.sc = SparkContext()
            self.glue_context = GlueContext(self.sc)
            self.spark = self.glue_context.spark_session

            # Configure Spark for Hudi
            self.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            self.spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

            # Initialize Glue Job
            self.job = Job(self.glue_context)

            logger.info("Spark and Glue contexts initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize contexts: {str(e)}")
            raise

    def read_customer_data(self):
        """
        Read customer data from S3

        Returns:
            DataFrame: Customer data
        """
        try:
            logger.info(f"Reading customer data from {self.CUSTOMER_DATA_PATH}")

            df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "UTF-8") \
                .csv(self.CUSTOMER_DATA_PATH)

            logger.info(f"Customer data read successfully. Record count: {df.count()}")
            return df

        except Exception as e:
            logger.error(f"Failed to read customer data: {str(e)}")
            raise

    def read_order_data(self):
        """
        Read order data from S3

        Returns:
            DataFrame: Order data
        """
        try:
            logger.info(f"Reading order data from {self.ORDER_DATA_PATH}")

            df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "UTF-8") \
                .csv(self.ORDER_DATA_PATH)

            logger.info(f"Order data read successfully. Record count: {df.count()}")
            return df

        except Exception as e:
            logger.error(f"Failed to read order data: {str(e)}")
            raise

    def clean_data(self, df):
        """
        Clean data by removing NULL values, 'Null' strings, and duplicates

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Cleaned DataFrame
        """
        try:
            logger.info("Starting data cleaning process")
            initial_count = df.count()

            # Get all column names
            columns = df.columns

            # Build filter condition: col != "Null" AND col IS NOT NULL for all columns
            filter_condition = None
            for column in columns:
                condition = (col(column) != "Null") & (col(column).isNotNull())
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition = filter_condition & condition

            # Apply filter
            df_cleaned = df.filter(filter_condition)

            # Remove duplicates
            df_cleaned = df_cleaned.dropDuplicates()

            final_count = df_cleaned.count()
            removed_count = initial_count - final_count

            logger.info(f"Data cleaning completed. Records removed: {removed_count}, Final count: {final_count}")

            return df_cleaned

        except Exception as e:
            logger.error(f"Failed to clean data: {str(e)}")
            raise

    def join_customer_order_data(self, customer_df, order_df):
        """
        Join customer and order data

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            DataFrame: Joined DataFrame
        """
        try:
            logger.info("Joining customer and order data")

            # Join on CustId
            joined_df = customer_df.join(order_df, on="CustId", how="inner")

            logger.info(f"Data joined successfully. Record count: {joined_df.count()}")
            return joined_df

        except Exception as e:
            logger.error(f"Failed to join data: {str(e)}")
            raise

    def add_scd_type2_columns(self, df):
        """
        Add SCD Type 2 columns: IsActive, StartDate, EndDate, OpTs

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: DataFrame with SCD Type 2 columns
        """
        try:
            logger.info("Adding SCD Type 2 columns")

            current_ts = current_timestamp()

            df_scd = df \
                .withColumn("IsActive", lit(True)) \
                .withColumn("StartDate", current_ts) \
                .withColumn("EndDate", lit(None).cast("timestamp")) \
                .withColumn("OpTs", current_ts)

            logger.info("SCD Type 2 columns added successfully")
            return df_scd

        except Exception as e:
            logger.error(f"Failed to add SCD Type 2 columns: {str(e)}")
            raise

    def write_to_hudi(self, df):
        """
        Write DataFrame to Hudi format with SCD Type 2 upsert operation

        Args:
            df: DataFrame to write
        """
        try:
            logger.info(f"Writing data to Hudi at {self.ORDER_SUMMARY_OUTPUT_PATH}")

            hudi_options = {
                'hoodie.table.name': self.HUDI_TABLE_NAME,
                'hoodie.datasource.write.recordkey.field': self.HUDI_RECORD_KEY,
                'hoodie.datasource.write.precombine.field': self.HUDI_PRECOMBINE_FIELD,
                'hoodie.datasource.write.partitionpath.field': self.HUDI_PARTITION_FIELD,
                'hoodie.datasource.write.operation': 'upsert',
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.datasource.hive_sync.enable': 'true',
                'hoodie.datasource.hive_sync.database': self.GLUE_DATABASE,
                'hoodie.datasource.hive_sync.table': self.GLUE_TABLE_ORDER_SUMMARY,
                'hoodie.datasource.hive_sync.partition_fields': self.HUDI_PARTITION_FIELD,
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
                'hoodie.datasource.hive_sync.use_jdbc': 'false',
                'hoodie.datasource.hive_sync.mode': 'hms'
            }

            df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(self.ORDER_SUMMARY_OUTPUT_PATH)

            logger.info("Data written to Hudi successfully")

        except Exception as e:
            logger.error(f"Failed to write data to Hudi: {str(e)}")
            raise

    def register_glue_catalog(self):
        """
        Register table in AWS Glue Catalog
        """
        try:
            logger.info(f"Registering table in Glue Catalog: {self.GLUE_DATABASE}.{self.GLUE_TABLE_ORDER_SUMMARY}")

            # Create database if not exists
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.GLUE_DATABASE}")

            # Hudi sync handles catalog registration automatically
            logger.info("Table registered in Glue Catalog successfully")

        except Exception as e:
            logger.error(f"Failed to register table in Glue Catalog: {str(e)}")
            raise

    def run(self):
        """
        Main execution method
        """
        try:
            logger.info("Starting Customer Order Data Processing Pipeline")

            # Initialize job
            self.job.init("customer-order-processing-job")

            # Step 1: Read customer data
            customer_df = self.read_customer_data()

            # Step 2: Read order data
            order_df = self.read_order_data()

            # Step 3: Clean customer data
            customer_df_cleaned = self.clean_data(customer_df)

            # Step 4: Clean order data
            order_df_cleaned = self.clean_data(order_df)

            # Step 5: Join customer and order data
            joined_df = self.join_customer_order_data(customer_df_cleaned, order_df_cleaned)

            # Step 6: Add SCD Type 2 columns
            scd_df = self.add_scd_type2_columns(joined_df)

            # Step 7: Write to Hudi
            self.write_to_hudi(scd_df)

            # Step 8: Register in Glue Catalog
            self.register_glue_catalog()

            # Commit job
            self.job.commit()

            logger.info("Customer Order Data Processing Pipeline completed successfully")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise
        finally:
            if hasattr(self, 'spark'):
                self.spark.stop()


def main():
    """
    Main entry point
    """
    try:
        processor = CustomerOrderProcessor()
        processor.run()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()