"""
PySpark Job for Customer and Order Data Processing
Implements data ingestion, cleaning, SCD Type 2, and aggregations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, sum as _sum,
    count, max as _max, row_number, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType, IntegerType
)
from pyspark import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import sys


class DataProcessingJob:
    """Main job class for data processing pipeline"""

    # S3 Paths from TRD
    S3_CUSTOMER_SOURCE = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    S3_ORDER_SOURCE = "s3://adif-sdlc/sdlc_wizard/orderdata/"
    S3_ORDER_SUMMARY_CURATED = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
    S3_CUSTOMER_AGGREGATE_ANALYTICS = "s3://adif-sdlc/analytics/customeraggregatespend/"

    # Glue Catalog Database
    GLUE_DATABASE = "sdlc_wizard_db"

    # Table Names
    TABLE_CUSTOMER = "customer"
    TABLE_ORDER = "order"
    TABLE_ORDER_SUMMARY = "ordersummary"
    TABLE_CUSTOMER_AGGREGATE = "customeraggregatespend"

    def __init__(self, spark_session=None, glue_context=None):
        """Initialize job with Spark and Glue contexts"""
        if spark_session:
            self.spark = spark_session
            self.glue_context = glue_context
        else:
            # Initialize Spark with Hudi support
            self.spark = SparkSession.builder \
                .appName("CustomerOrderDataProcessing") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.hive.convertMetastoreParquet", "false") \
                .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
                .config("spark.sql.extensions",
                       "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                .getOrCreate()

            # Initialize Glue Context
            sc = SparkContext.getOrCreate()
            self.glue_context = GlueContext(sc)
            self.job = Job(self.glue_context)

    def get_customer_schema(self):
        """Define customer schema from TRD"""
        return StructType([
            StructField("CustId", StringType(), False),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])

    def get_order_schema(self):
        """Define order schema - TODO: Complete from TRD"""
        # TODO: Extract complete order schema from TRD
        return StructType([
            StructField("OrderId", StringType(), False),
            StructField("CustId", StringType(), False),
            StructField("OrderDate", StringType(), True),
            StructField("OrderAmount", DoubleType(), True),
            StructField("OrderStatus", StringType(), True)
        ])

    def add_scd2_columns(self, df, is_new_record=True):
        """
        Add SCD Type 2 columns as per TRD requirements
        - IsActive: Boolean
        - StartDate: Timestamp
        - EndDate: Timestamp
        - OpTs: Timestamp
        """
        current_ts = current_timestamp()

        if is_new_record:
            return df.withColumn("IsActive", lit(True)) \
                    .withColumn("StartDate", current_ts) \
                    .withColumn("EndDate", lit(None).cast(TimestampType())) \
                    .withColumn("OpTs", current_ts)
        else:
            return df

    def clean_data(self, df):
        """
        Implement TR-CLEAN-001 requirements:
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records
        """
        # Get all columns
        all_columns = df.columns

        # Remove rows with NULL values in any column
        df_cleaned = df.dropna(how='any')

        # Remove rows with 'Null' string values
        for column in all_columns:
            df_cleaned = df_cleaned.filter(
                (col(column) != "Null") &
                (col(column) != "null") &
                (col(column) != "NULL")
            )

        # Remove duplicate records
        df_cleaned = df_cleaned.dropDuplicates()

        return df_cleaned

    def ingest_customer_data(self):
        """Ingest customer data from S3 source"""
        try:
            customer_df = self.spark.read \
                .schema(self.get_customer_schema()) \
                .option("header", "true") \
                .csv(self.S3_CUSTOMER_SOURCE)

            return customer_df
        except Exception as e:
            print(f"Error ingesting customer data: {str(e)}")
            raise

    def ingest_order_data(self):
        """Ingest order data from S3 source"""
        try:
            order_df = self.spark.read \
                .schema(self.get_order_schema()) \
                .option("header", "true") \
                .csv(self.S3_ORDER_SOURCE)

            return order_df
        except Exception as e:
            print(f"Error ingesting order data: {str(e)}")
            raise

    def process_customer_data(self):
        """Process customer data with cleaning and SCD Type 2"""
        # Ingest data
        customer_df = self.ingest_customer_data()

        # Clean data (TR-CLEAN-001)
        customer_cleaned = self.clean_data(customer_df)

        # Add SCD Type 2 columns
        customer_scd2 = self.add_scd2_columns(customer_cleaned)

        return customer_scd2

    def process_order_data(self):
        """Process order data with cleaning"""
        # Ingest data
        order_df = self.ingest_order_data()

        # Clean data (TR-CLEAN-001)
        order_cleaned = self.clean_data(order_df)

        return order_cleaned

    def create_order_summary(self, order_df):
        """Create order summary with aggregations"""
        order_summary = order_df.groupBy("CustId") \
            .agg(
                count("OrderId").alias("TotalOrders"),
                _sum("OrderAmount").alias("TotalOrderAmount"),
                _max("OrderDate").alias("LastOrderDate")
            )

        # Add SCD Type 2 columns
        order_summary_scd2 = self.add_scd2_columns(order_summary)

        return order_summary_scd2

    def create_customer_aggregate_spend(self, customer_df, order_df):
        """Create customer aggregate spend analytics"""
        # Join customer and order data
        customer_order_df = customer_df.join(
            order_df,
            customer_df.CustId == order_df.CustId,
            "left"
        )

        # Aggregate spend by customer and region
        aggregate_spend = customer_order_df.groupBy(
            customer_df.CustId,
            customer_df.Name,
            customer_df.Region
        ).agg(
            _sum(coalesce(col("OrderAmount"), lit(0))).alias("TotalSpend"),
            count("OrderId").alias("OrderCount"),
            _max("OrderDate").alias("LastOrderDate")
        )

        return aggregate_spend

    def write_hudi_table(self, df, table_name, record_key, s3_path,
                        precombine_field="OpTs", operation="upsert"):
        """
        Write DataFrame to Hudi format with SCD Type 2 support
        """
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_field,
            'hoodie.datasource.write.operation': operation,
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': self.GLUE_DATABASE,
            'hoodie.datasource.hive_sync.table': table_name,
            'hoodie.datasource.hive_sync.mode': 'hms',
            'hoodie.datasource.hive_sync.use_jdbc': 'false'
        }

        df.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(s3_path)

    def write_to_s3(self, df, s3_path, format="parquet", mode="overwrite"):
        """Write DataFrame to S3 in specified format"""
        df.write \
            .format(format) \
            .mode(mode) \
            .save(s3_path)

    def register_glue_table(self, table_name, s3_path):
        """Register table in AWS Glue Catalog"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.GLUE_DATABASE}")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.GLUE_DATABASE}.{table_name}
                USING parquet
                LOCATION '{s3_path}'
            """)

            print(f"Table {self.GLUE_DATABASE}.{table_name} registered successfully")
        except Exception as e:
            print(f"Error registering table {table_name}: {str(e)}")
            raise

    def run(self):
        """Execute the complete data processing pipeline"""
        try:
            print("Starting data processing pipeline...")

            # Process customer data
            print("Processing customer data...")
            customer_df = self.process_customer_data()

            # Write customer data with Hudi SCD Type 2
            print("Writing customer data to Hudi...")
            self.write_hudi_table(
                customer_df,
                self.TABLE_CUSTOMER,
                "CustId",
                self.S3_CUSTOMER_SOURCE
            )

            # Process order data
            print("Processing order data...")
            order_df = self.process_order_data()

            # Write order data
            print("Writing order data...")
            self.write_to_s3(order_df, self.S3_ORDER_SOURCE)

            # Create and write order summary
            print("Creating order summary...")
            order_summary_df = self.create_order_summary(order_df)
            self.write_hudi_table(
                order_summary_df,
                self.TABLE_ORDER_SUMMARY,
                "CustId",
                self.S3_ORDER_SUMMARY_CURATED
            )

            # Create and write customer aggregate spend
            print("Creating customer aggregate spend...")
            aggregate_spend_df = self.create_customer_aggregate_spend(
                customer_df, order_df
            )
            self.write_to_s3(
                aggregate_spend_df,
                self.S3_CUSTOMER_AGGREGATE_ANALYTICS
            )

            # Register tables in Glue Catalog
            print("Registering tables in Glue Catalog...")
            self.register_glue_table(
                self.TABLE_CUSTOMER_AGGREGATE,
                self.S3_CUSTOMER_AGGREGATE_ANALYTICS
            )

            print("Data processing pipeline completed successfully!")

        except Exception as e:
            print(f"Error in pipeline execution: {str(e)}")
            raise
        finally:
            self.spark.stop()


def main():
    """Main entry point for the job"""
    job = DataProcessingJob()
    job.run()


if __name__ == "__main__":
    main()