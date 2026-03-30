"""
PySpark Job for Customer and Order Data Processing
Implements data cleaning, SCD Type 2, and aggregations
"""

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as _sum, count,
    row_number, when, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType
)
from datetime import datetime
import sys


class DataProcessor:
    """Main data processing class for customer and order data"""

    # S3 Paths from TRD
    CUSTOMER_SOURCE_PATH = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    ORDER_SOURCE_PATH = "s3://adif-sdlc/sdlc_wizard/orderdata/"
    CLEANED_CUSTOMER_PATH = "s3://adif-sdlc/cleaned/sdlc_wizard/customer/"
    CLEANED_ORDER_PATH = "s3://adif-sdlc/cleaned/sdlc_wizard/order/"
    CURATED_ORDER_SUMMARY_PATH = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
    CUSTOMER_AGGREGATE_SPEND_PATH = "s3://adif-sdlc/analytics/customeraggregatespend/"

    # Database name
    GLUE_DATABASE = "sdlc_wizard"

    def __init__(self, spark_session=None):
        """Initialize DataProcessor with Spark/Glue context"""
        if spark_session:
            self.spark = spark_session
            self.glue_context = None
        else:
            sc = SparkContext()
            self.glue_context = GlueContext(sc)
            self.spark = self.glue_context.spark_session

        self.job = None

    def initialize_job(self, job_name="sdlc_wizard_job"):
        """Initialize Glue Job"""
        if self.glue_context:
            self.job = Job(self.glue_context)
            self.job.init(job_name, {})

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
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("OrderId", StringType(), True)
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
        Clean data according to TR-CLEAN-001:
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records
        """
        # Get all column names
        columns = df.columns

        # Remove rows with NULL values in any column
        df_cleaned = df.dropna(how='any')

        # Remove rows with 'Null' string values in any column
        for column in columns:
            df_cleaned = df_cleaned.filter(
                (col(column) != "Null") &
                (col(column) != "null") &
                (col(column) != "NULL")
            )

        # Remove duplicate records
        df_cleaned = df_cleaned.dropDuplicates()

        return df_cleaned

    def add_scd2_columns(self, df, is_new=True):
        """
        Add SCD Type 2 columns:
        - IsActive: boolean
        - StartDate: timestamp
        - EndDate: timestamp
        - OpTs: timestamp
        """
        current_ts = current_timestamp()

        if is_new:
            df = df.withColumn("IsActive", lit(True)) \
                   .withColumn("StartDate", current_ts) \
                   .withColumn("EndDate", lit(None).cast(TimestampType())) \
                   .withColumn("OpTs", current_ts)

        return df

    def write_to_hudi(self, df, table_name, record_key, path):
        """
        Write DataFrame to Hudi format with SCD Type 2 configuration
        """
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': 'OpTs',
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
        """Register table in Glue Catalog"""
        if self.glue_context:
            # TODO: Implement Glue Catalog registration
            # Requires additional Glue API calls
            pass

    def calculate_customer_aggregate_spend(self, order_df):
        """
        Calculate customer aggregate spend
        Aggregation by CustId
        """
        # TODO: Add spend/amount column to order schema in TRD
        # Currently using count of orders as placeholder
        aggregate_df = order_df.groupBy("CustId", "Name", "EmailId", "Region") \
            .agg(
                count("OrderId").alias("TotalOrders")
            )

        return aggregate_df

    def process_customer_data(self):
        """Process customer data: ingest, clean, apply SCD2, write"""
        # Read customer data
        customer_df = self.read_customer_data()

        # Clean data
        customer_cleaned = self.clean_data(customer_df)

        # Add SCD Type 2 columns
        customer_scd2 = self.add_scd2_columns(customer_cleaned)

        # Write to Hudi
        self.write_to_hudi(
            customer_scd2,
            table_name="customer",
            record_key="CustId",
            path=self.CLEANED_CUSTOMER_PATH
        )

        # Register in Glue Catalog
        self.register_glue_table("customer", self.CLEANED_CUSTOMER_PATH)

        return customer_scd2

    def process_order_data(self):
        """Process order data: ingest, clean, apply SCD2, write"""
        # Read order data
        order_df = self.read_order_data()

        # Clean data
        order_cleaned = self.clean_data(order_df)

        # Add SCD Type 2 columns
        order_scd2 = self.add_scd2_columns(order_cleaned)

        # Write to Hudi
        self.write_to_hudi(
            order_scd2,
            table_name="order",
            record_key="OrderId",
            path=self.CLEANED_ORDER_PATH
        )

        # Register in Glue Catalog
        self.register_glue_table("order", self.CLEANED_ORDER_PATH)

        return order_scd2

    def create_order_summary(self, order_df):
        """Create curated order summary"""
        # Write order summary to curated layer
        order_df.write \
            .mode("overwrite") \
            .parquet(self.CURATED_ORDER_SUMMARY_PATH)

        self.register_glue_table("ordersummary", self.CURATED_ORDER_SUMMARY_PATH)

    def create_customer_aggregate_spend(self, order_df):
        """Create customer aggregate spend analytics"""
        aggregate_df = self.calculate_customer_aggregate_spend(order_df)

        # Write to analytics path
        aggregate_df.write \
            .mode("overwrite") \
            .parquet(self.CUSTOMER_AGGREGATE_SPEND_PATH)

        self.register_glue_table("customeraggregatespend", self.CUSTOMER_AGGREGATE_SPEND_PATH)

    def run(self):
        """Execute complete data processing pipeline"""
        # Initialize job
        self.initialize_job()

        # Process customer data
        customer_df = self.process_customer_data()

        # Process order data
        order_df = self.process_order_data()

        # Create order summary
        self.create_order_summary(order_df)

        # Create customer aggregate spend
        self.create_customer_aggregate_spend(order_df)

        # Commit job
        if self.job:
            self.job.commit()

        return {
            "customer_count": customer_df.count(),
            "order_count": order_df.count()
        }


def main():
    """Main entry point for Glue job"""
    processor = DataProcessor()
    result = processor.run()
    print(f"Processing complete: {result}")


if __name__ == "__main__":
    main()