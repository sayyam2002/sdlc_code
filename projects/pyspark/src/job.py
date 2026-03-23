"""
PySpark Job for Customer Order Data Processing with SCD Type 2
Implements data ingestion, cleaning, aggregation, and Hudi-based SCD Type 2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, current_timestamp, lit, when, max as _max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    BooleanType, TimestampType
)
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import sys


class CustomerOrderProcessor:
    """
    Main processor for customer order data with SCD Type 2 implementation
    """

    def __init__(self, spark_session=None):
        """
        Initialize the processor with Spark and Glue contexts
        """
        if spark_session:
            self.spark = spark_session
        else:
            sc = SparkContext()
            self.glue_context = GlueContext(sc)
            self.spark = self.glue_context.spark_session
            self.job = Job(self.glue_context)

        # S3 paths from TRD
        self.order_source_path = "s3://adif-sdlc/sdlc_wizard/orderdata/"
        self.customer_source_path = "s3://adif-sdlc/catalog/sdlc_wizard/customer/"
        self.output_path = "s3://adif-sdlc/analytics/customeraggregatespend/"
        self.catalog_database = "sdlc_wizard"

    def get_customer_schema(self):
        """
        Define customer schema based on TRD
        """
        return StructType([
            StructField("Customer", StringType(), True),
            StructField("name", StringType(), True)
        ])

    def get_order_schema(self):
        """
        Define order schema based on TRD
        """
        return StructType([
            StructField("Customer", StringType(), True),
            StructField("ordersummary", StringType(), True),
            StructField("TotalAmount", DecimalType(10, 2), True)
        ])

    def ingest_data(self, path, schema):
        """
        FR-INGEST-001: Ingest data from S3 source

        Args:
            path: S3 path to data source
            schema: Schema definition for the data

        Returns:
            DataFrame with ingested data
        """
        try:
            df = self.spark.read \
                .schema(schema) \
                .option("header", "true") \
                .csv(path)
            return df
        except Exception as e:
            print(f"Error ingesting data from {path}: {str(e)}")
            raise

    def clean_data(self, df):
        """
        FR-CLEAN-001 / TR-CLEAN-001: Clean data by removing NULLs and duplicates

        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        # Remove rows with NULL values in any column
        df_cleaned = df.na.drop()

        # Remove rows with 'Null' string values
        for column in df_cleaned.columns:
            df_cleaned = df_cleaned.filter(
                (col(column) != "Null") &
                (col(column) != "null") &
                (col(column) != "NULL")
            )

        # Remove duplicate records
        df_cleaned = df_cleaned.dropDuplicates()

        return df_cleaned

    def aggregate_customer_spending(self, order_df, customer_df):
        """
        FR-AGG-001: Calculate aggregated customer spending

        Args:
            order_df: Order DataFrame
            customer_df: Customer DataFrame

        Returns:
            DataFrame with aggregated spending per customer
        """
        # Join orders with customers
        joined_df = order_df.join(
            customer_df,
            on="Customer",
            how="inner"
        )

        # Aggregate spending by customer
        aggregated_df = joined_df.groupBy("Customer", "name") \
            .agg(_sum("TotalAmount").alias("TotalAmount"))

        return aggregated_df

    def add_scd2_columns(self, df, is_initial_load=True):
        """
        FR-SCD2-001: Add SCD Type 2 columns

        Args:
            df: Input DataFrame
            is_initial_load: Whether this is initial load or update

        Returns:
            DataFrame with SCD Type 2 columns added
        """
        current_ts = current_timestamp()

        if is_initial_load:
            # Initial load: all records are active
            df_scd2 = df.withColumn("IsActive", lit(True)) \
                .withColumn("StartDate", current_ts) \
                .withColumn("EndDate", lit(None).cast(TimestampType())) \
                .withColumn("OpTs", current_ts)
        else:
            # Update: add columns for new records
            df_scd2 = df.withColumn("IsActive", lit(True)) \
                .withColumn("StartDate", current_ts) \
                .withColumn("EndDate", lit(None).cast(TimestampType())) \
                .withColumn("OpTs", current_ts)

        return df_scd2

    def write_to_hudi(self, df, table_name, record_key, precombine_field):
        """
        Write DataFrame to Hudi format with SCD Type 2 support

        Args:
            df: DataFrame to write
            table_name: Hudi table name
            record_key: Primary key field(s)
            precombine_field: Field for ordering records
        """
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_field,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.upsert.shuffle.parallelism': 2,
            'hoodie.insert.shuffle.parallelism': 2
        }

        try:
            df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(self.output_path)

            print(f"Successfully wrote data to Hudi table: {table_name}")
        except Exception as e:
            print(f"Error writing to Hudi: {str(e)}")
            raise

    def register_glue_catalog(self, table_name, path):
        """
        Register table in AWS Glue Catalog

        Args:
            table_name: Name of the table
            path: S3 path to the table data
        """
        try:
            # Read the data to infer schema
            df = self.spark.read.format("hudi").load(path)

            # Register as temporary view
            df.createOrReplaceTempView(table_name)

            # Create table in Glue Catalog
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_database}.{table_name}
                USING hudi
                LOCATION '{path}'
            """)

            print(f"Registered table {table_name} in Glue Catalog database {self.catalog_database}")
        except Exception as e:
            print(f"Error registering Glue Catalog table: {str(e)}")
            # Non-fatal error, continue processing

    def run(self):
        """
        Main execution method for the data processing pipeline
        """
        print("Starting Customer Order Data Processing Pipeline")

        # FR-INGEST-001: Ingest customer data
        print("Ingesting customer data...")
        customer_df = self.ingest_data(
            self.customer_source_path,
            self.get_customer_schema()
        )

        # FR-CLEAN-001: Clean customer data
        print("Cleaning customer data...")
        customer_df_clean = self.clean_data(customer_df)

        # FR-INGEST-001: Ingest order data
        print("Ingesting order data...")
        order_df = self.ingest_data(
            self.order_source_path,
            self.get_order_schema()
        )

        # FR-CLEAN-001: Clean order data
        print("Cleaning order data...")
        order_df_clean = self.clean_data(order_df)

        # FR-AGG-001: Aggregate customer spending
        print("Aggregating customer spending...")
        aggregated_df = self.aggregate_customer_spending(
            order_df_clean,
            customer_df_clean
        )

        # FR-SCD2-001: Add SCD Type 2 columns
        print("Adding SCD Type 2 columns...")
        final_df = self.add_scd2_columns(aggregated_df, is_initial_load=True)

        # Write to Hudi
        print("Writing to Hudi format...")
        self.write_to_hudi(
            final_df,
            table_name="customeraggregatespend",
            record_key="Customer",
            precombine_field="OpTs"
        )

        # Register in Glue Catalog
        print("Registering in Glue Catalog...")
        self.register_glue_catalog(
            "customeraggregatespend",
            self.output_path
        )

        print("Pipeline execution completed successfully")

        return final_df


def main():
    """
    Entry point for the Glue job
    """
    processor = CustomerOrderProcessor()
    processor.run()


if __name__ == "__main__":
    main()