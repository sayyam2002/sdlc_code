"""
PySpark Job for Customer Order Analytics with SCD Type 2 using Hudi
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, lit, current_timestamp,
    to_timestamp, when, max as _max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import sys


class CustomerOrderAnalyticsPipeline:
    """
    Main pipeline class for customer order analytics with SCD Type 2
    """

    def __init__(self):
        """Initialize Spark and Glue contexts"""
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session

        # Configure Hudi
        self.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        self.spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

        # S3 paths from TRD
        self.customer_data_path = "s3://adif-sdlc/sdlc_wizard/customerdata/"
        self.order_data_path = "s3://adif-sdlc/sdlc_wizard/orderdata/"
        self.order_summary_path = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
        self.customer_aggregate_spend_path = "s3://adif-sdlc/analytics/customeraggregatespend/"
        self.brd_usecase1_path = "s3://adif-sdlc/brd_usecase1/"

        # Glue Catalog details from TRD
        self.glue_database = "read"
        self.glue_table = "customeraggregatespend"

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
            StructField("CustId", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("PricePerUnit", DoubleType(), True),
            StructField("Qty", IntegerType(), True)
        ])

    def read_customer_data(self):
        """
        Read customer data from S3
        FR-INGEST-001: Ingest customer data
        """
        try:
            df = self.spark.read \
                .schema(self.get_customer_schema()) \
                .option("header", "true") \
                .csv(self.customer_data_path)
            return df
        except Exception as e:
            print(f"Error reading customer data: {str(e)}")
            raise

    def read_order_data(self):
        """
        Read order data from S3
        FR-INGEST-001: Ingest order data
        """
        try:
            df = self.spark.read \
                .schema(self.get_order_schema()) \
                .option("header", "true") \
                .csv(self.order_data_path)
            return df
        except Exception as e:
            print(f"Error reading order data: {str(e)}")
            raise

    def clean_data(self, df):
        """
        Clean data by removing NULLs, 'Null' strings, and duplicates
        FR-CLEAN-001: Data cleaning requirements
        """
        # Remove rows with NULL values
        df_cleaned = df.dropna()

        # Remove rows with 'Null' string values in any column
        for column in df.columns:
            df_cleaned = df_cleaned.filter(col(column) != "Null")

        # Remove duplicate records
        df_cleaned = df_cleaned.dropDuplicates()

        return df_cleaned

    def join_customer_order(self, customer_df, order_df):
        """
        Join customer and order data
        FR-JOIN-001: Join customer and order data
        """
        joined_df = customer_df.join(
            order_df,
            customer_df.CustId == order_df.CustId,
            "inner"
        ).select(
            customer_df.CustId,
            customer_df.Name,
            customer_df.EmailId,
            customer_df.Region,
            order_df.OrderId,
            order_df.ItemName,
            order_df.PricePerUnit,
            order_df.Qty
        )
        return joined_df

    def calculate_aggregate_spend(self, joined_df):
        """
        Calculate aggregate spend per customer
        FR-AGG-001: Aggregate spend calculation
        """
        # Calculate total spend per order line
        df_with_total = joined_df.withColumn(
            "TotalSpend",
            col("PricePerUnit") * col("Qty")
        )

        # Aggregate by customer
        aggregate_df = df_with_total.groupBy(
            "CustId",
            "Name",
            "EmailId",
            "Region"
        ).agg(
            _sum("TotalSpend").alias("TotalSpend"),
            count("OrderId").alias("OrderCount")
        )

        return aggregate_df

    def add_scd2_columns(self, df):
        """
        Add SCD Type 2 columns
        FR-SCD2-001: Add SCD Type 2 tracking columns
        """
        current_ts = current_timestamp()

        df_with_scd2 = df \
            .withColumn("IsActive", lit(True).cast(BooleanType())) \
            .withColumn("StartDate", current_ts.cast(TimestampType())) \
            .withColumn("EndDate", lit(None).cast(TimestampType())) \
            .withColumn("OpTs", current_ts.cast(TimestampType()))

        return df_with_scd2

    def write_to_hudi(self, df, table_name, record_key, precombine_key, path):
        """
        Write data to Hudi format with upsert operation
        FR-SCD2-002: Write data with Hudi SCD Type 2
        """
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_key,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.partitionpath.field': 'Region',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': self.glue_database,
            'hoodie.datasource.hive_sync.table': table_name,
            'hoodie.datasource.hive_sync.partition_fields': 'Region',
            'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.hive_sync.mode': 'hms'
        }

        try:
            df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(path)
            print(f"Successfully wrote data to {path}")
        except Exception as e:
            print(f"Error writing to Hudi: {str(e)}")
            raise

    def register_glue_catalog_table(self, path, table_name):
        """
        Register table in AWS Glue Catalog
        FR-CATALOG-001: Register table in Glue Catalog
        """
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.glue_database}")

            # Glue Catalog registration is handled by Hudi hive_sync
            print(f"Table {table_name} registered in Glue Catalog database {self.glue_database}")
        except Exception as e:
            print(f"Error registering Glue Catalog table: {str(e)}")
            raise

    def run(self):
        """
        Main execution method
        """
        print("Starting Customer Order Analytics Pipeline...")

        # FR-INGEST-001: Read data
        print("Reading customer data...")
        customer_df = self.read_customer_data()

        print("Reading order data...")
        order_df = self.read_order_data()

        # FR-CLEAN-001: Clean data
        print("Cleaning customer data...")
        customer_cleaned = self.clean_data(customer_df)

        print("Cleaning order data...")
        order_cleaned = self.clean_data(order_df)

        # FR-JOIN-001: Join data
        print("Joining customer and order data...")
        joined_df = self.join_customer_order(customer_cleaned, order_cleaned)

        # FR-AGG-001: Calculate aggregates
        print("Calculating aggregate spend...")
        aggregate_df = self.calculate_aggregate_spend(joined_df)

        # FR-SCD2-001: Add SCD Type 2 columns
        print("Adding SCD Type 2 columns...")
        final_df = self.add_scd2_columns(aggregate_df)

        # FR-SCD2-002: Write to Hudi
        print("Writing to Hudi format...")
        self.write_to_hudi(
            df=final_df,
            table_name=self.glue_table,
            record_key="CustId",
            precombine_key="OpTs",
            path=self.customer_aggregate_spend_path
        )

        # FR-CATALOG-001: Register in Glue Catalog
        print("Registering table in Glue Catalog...")
        self.register_glue_catalog_table(
            self.customer_aggregate_spend_path,
            self.glue_table
        )

        # Write to BRD use case path
        print("Writing to BRD use case path...")
        final_df.write \
            .mode("overwrite") \
            .parquet(self.brd_usecase1_path)

        print("Pipeline execution completed successfully!")

        return final_df


def main():
    """
    Main entry point for Glue job
    """
    pipeline = CustomerOrderAnalyticsPipeline()
    result_df = pipeline.run()
    result_df.show(10)


if __name__ == "__main__":
    main()