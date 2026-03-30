"""
PySpark Job for Customer and Order Data Processing
Implements data ingestion, cleaning, SCD Type 2, and aggregation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as _sum, count,
    when, row_number, max as _max
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import sys


class DataProcessor:
    """Main data processing class for customer and order data"""

    # S3 Paths from TRD
    CUSTOMER_SOURCE_PATH = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    ORDER_SOURCE_PATH = "s3://adif-sdlc/sdlc_wizard/orderdata/"
    CURATED_BASE_PATH = "s3://adif-sdlc/curated/sdlc_wizard/"
    ORDER_SUMMARY_PATH = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
    ANALYTICS_PATH = "s3://adif-sdlc/analytics/customeraggregatespend/"

    # Glue Catalog Database from TRD
    GLUE_DATABASE = "Full"

    def __init__(self):
        """Initialize Spark and Glue contexts"""
        self.spark = SparkSession.builder \
            .appName("CustomerOrderDataProcessing") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
            .getOrCreate()

        self.sc = SparkContext.getOrCreate()
        self.glue_context = GlueContext(self.sc)
        self.job = Job(self.glue_context)

    def get_customer_schema(self):
        """Define customer schema from TRD"""
        return StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])

    def get_order_schema(self):
        """Define order schema from TRD"""
        return StructType([
            StructField("OrderId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True),
            StructField("Date", StringType(), True)
        ])

    def read_customer_data(self):
        """Read customer data from S3"""
        return self.spark.read \
            .schema(self.get_customer_schema()) \
            .option("header", "true") \
            .csv(self.CUSTOMER_SOURCE_PATH)

    def read_order_data(self):
        """Read order data from S3"""
        return self.spark.read \
            .schema(self.get_order_schema()) \
            .option("header", "true") \
            .csv(self.ORDER_SOURCE_PATH)

    def clean_data(self, df):
        """
        Clean data by removing:
        1. Rows where any column is NULL
        2. Rows where any column contains string literal 'Null'
        """
        # Get all column names
        columns = df.columns

        # Filter out rows with NULL values in any column
        for column in columns:
            df = df.filter(col(column).isNotNull())

        # Filter out rows with 'Null' string in any column
        for column in columns:
            df = df.filter(col(column) != "Null")

        return df

    def deduplicate_data(self, df, key_columns):
        """Remove duplicate records based on key columns"""
        return df.dropDuplicates(key_columns)

    def add_scd2_columns(self, df, is_new=True):
        """
        Add SCD Type 2 columns:
        - IsActive: Boolean
        - StartDate: Timestamp
        - EndDate: Timestamp
        - OpTs: Timestamp
        """
        current_ts = current_timestamp()

        df = df.withColumn("IsActive", lit(True).cast(BooleanType()))
        df = df.withColumn("StartDate", current_ts.cast(TimestampType()))
        df = df.withColumn("EndDate", lit(None).cast(TimestampType()))
        df = df.withColumn("OpTs", current_ts.cast(TimestampType()))

        return df

    def write_to_hudi(self, df, table_name, record_key, precombine_key, path):
        """
        Write DataFrame to Hudi format with SCD Type 2 configuration
        """
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_key,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.upsert.shuffle.parallelism': 2,
            'hoodie.insert.shuffle.parallelism': 2
        }

        df.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)

    def register_glue_table(self, table_name, path):
        """Register table in AWS Glue Catalog"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.GLUE_DATABASE}")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.GLUE_DATABASE}.{table_name}
                USING hudi
                LOCATION '{path}'
            """)
        except Exception as e:
            print(f"Warning: Could not register table {table_name}: {str(e)}")

    def calculate_customer_aggregate_spend(self, customer_df, order_df):
        """
        Calculate aggregate spend per customer
        Join customer and order data, then aggregate
        """
        # Add CustId to order data (assuming OrderId contains CustId pattern)
        # TODO: Clarify join key relationship in TRD
        # For now, using a sample join logic

        # Calculate total spend per order
        order_df = order_df.withColumn(
            "TotalSpend",
            col("PricePerUnit") * col("Qty")
        )

        # Aggregate by customer (assuming CustId can be derived from OrderId)
        # TODO: Add proper join key from TRD
        aggregate_df = order_df.groupBy("OrderId").agg(
            _sum("TotalSpend").alias("TotalSpend"),
            count("OrderId").alias("OrderCount")
        )

        return aggregate_df

    def process_customer_data(self):
        """Process customer data with cleaning, deduplication, and SCD Type 2"""
        print("Processing customer data...")

        # Read data
        customer_df = self.read_customer_data()

        # Clean data
        customer_df = self.clean_data(customer_df)

        # Deduplicate
        customer_df = self.deduplicate_data(customer_df, ["CustId"])

        # Add SCD Type 2 columns
        customer_df = self.add_scd2_columns(customer_df)

        # Write to Hudi
        customer_path = f"{self.CURATED_BASE_PATH}customer/"
        self.write_to_hudi(
            customer_df,
            table_name="customer",
            record_key="CustId",
            precombine_key="OpTs",
            path=customer_path
        )

        # Register in Glue Catalog
        self.register_glue_table("customer", customer_path)

        print(f"Customer data processed: {customer_df.count()} records")
        return customer_df

    def process_order_data(self):
        """Process order data with cleaning, deduplication, and SCD Type 2"""
        print("Processing order data...")

        # Read data
        order_df = self.read_order_data()

        # Clean data
        order_df = self.clean_data(order_df)

        # Deduplicate
        order_df = self.deduplicate_data(order_df, ["OrderId"])

        # Add SCD Type 2 columns
        order_df = self.add_scd2_columns(order_df)

        # Write to Hudi
        order_path = f"{self.ORDER_SUMMARY_PATH}"
        self.write_to_hudi(
            order_df,
            table_name="ordersummary",
            record_key="OrderId",
            precombine_key="OpTs",
            path=order_path
        )

        # Register in Glue Catalog
        self.register_glue_table("ordersummary", order_path)

        print(f"Order data processed: {order_df.count()} records")
        return order_df

    def process_analytics(self, customer_df, order_df):
        """Generate customer aggregate spend analytics"""
        print("Processing analytics...")

        # Calculate aggregates
        aggregate_df = self.calculate_customer_aggregate_spend(customer_df, order_df)

        # Add SCD Type 2 columns
        aggregate_df = self.add_scd2_columns(aggregate_df)

        # Write to analytics path
        aggregate_df.write \
            .mode("overwrite") \
            .parquet(self.ANALYTICS_PATH)

        print(f"Analytics processed: {aggregate_df.count()} records")
        return aggregate_df

    def run(self):
        """Main execution method"""
        try:
            print("Starting data processing job...")

            # Process customer data
            customer_df = self.process_customer_data()

            # Process order data
            order_df = self.process_order_data()

            # Process analytics
            analytics_df = self.process_analytics(customer_df, order_df)

            print("Data processing job completed successfully!")

        except Exception as e:
            print(f"Error in data processing: {str(e)}")
            raise
        finally:
            self.spark.stop()


def main():
    """Entry point for the Glue job"""
    processor = DataProcessor()
    processor.run()


if __name__ == "__main__":
    main()