"""
PySpark Job for Customer and Order Data Processing with SCD Type 2
Processes customer and order data from S3, applies data cleaning,
implements SCD Type 2 using Hudi, and writes to curated S3 locations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, sum as _sum,
    count, when, trim, upper
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import sys


class CustomerOrderProcessor:
    """Main processor for customer and order data pipeline"""

    # S3 Paths from TRD
    CUSTOMER_SOURCE_PATH = "s3://adif-sdlc/sdlc_wizard/customerdata/"
    ORDER_SOURCE_PATH = "s3://adif-sdlc/sdlc_wizard/orderdata/"
    CURATED_CUSTOMER_PATH = "s3://adif-sdlc/curated/sdlc_wizard/customer/"
    CURATED_ORDER_PATH = "s3://adif-sdlc/curated/sdlc_wizard/order/"
    ORDER_SUMMARY_PATH = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"
    CUSTOMER_AGGREGATE_SPEND_PATH = "s3://adif-sdlc/analytics/customeraggregatespend/"

    # Glue Catalog Database
    GLUE_DATABASE = "sdlc_wizard"

    def __init__(self):
        """Initialize Spark and Glue contexts"""
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session

        # Configure Spark for Hudi
        self.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        self.spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

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
            StructField("PricePerUnit", StringType(), True),  # Read as string, convert to decimal
            StructField("Qty", StringType(), True),  # Read as string, convert to integer
            StructField("Date", StringType(), True),  # Read as string, convert to date
            StructField("CustId", StringType(), True)
        ])

    def read_customer_data(self):
        """Read customer data from S3"""
        return self.spark.read \
            .schema(self.get_customer_schema()) \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .csv(self.CUSTOMER_SOURCE_PATH)

    def read_order_data(self):
        """Read order data from S3"""
        return self.spark.read \
            .schema(self.get_order_schema()) \
            .option("header", "true") \
            .csv(self.ORDER_SOURCE_PATH)

    def clean_data(self, df):
        """
        Clean data according to TRD requirements:
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
                ~(upper(trim(col(column))) == lit("NULL"))
            )

        # Remove duplicate records
        df_cleaned = df_cleaned.dropDuplicates()

        return df_cleaned

    def transform_order_data(self, df):
        """
        Transform order data:
        - Parse Date field as DateType
        - Parse PricePerUnit as DecimalType
        - Parse Qty as IntegerType
        """
        return df \
            .withColumn("Date", to_date(col("Date"))) \
            .withColumn("PricePerUnit", col("PricePerUnit").cast(DecimalType(10, 2))) \
            .withColumn("Qty", col("Qty").cast(IntegerType()))

    def add_scd2_columns(self, df):
        """
        Add SCD Type 2 columns:
        - IsActive: Boolean (default True)
        - StartDate: Timestamp (current timestamp)
        - EndDate: Timestamp (null for active records)
        - OpTs: Timestamp (operation timestamp)
        """
        return df \
            .withColumn("IsActive", lit(True).cast(BooleanType())) \
            .withColumn("StartDate", current_timestamp().cast(TimestampType())) \
            .withColumn("EndDate", lit(None).cast(TimestampType())) \
            .withColumn("OpTs", current_timestamp().cast(TimestampType()))

    def write_hudi_scd2(self, df, table_name, record_key, precombine_field):
        """
        Write data in Hudi format with SCD Type 2 configuration
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

        output_path = self.CURATED_CUSTOMER_PATH if table_name == 'customer' else self.CURATED_ORDER_PATH

        df.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(output_path)

        return output_path

    def write_parquet(self, df, output_path):
        """Write data in Parquet format"""
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(output_path)

    def register_glue_table(self, table_name, s3_path):
        """Register table in AWS Glue Catalog"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.GLUE_DATABASE}")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.GLUE_DATABASE}.{table_name}
                USING PARQUET
                LOCATION '{s3_path}'
            """)

            print(f"Table {self.GLUE_DATABASE}.{table_name} registered successfully")
        except Exception as e:
            print(f"Error registering table {table_name}: {str(e)}")

    def create_order_summary(self, order_df):
        """
        Create order summary aggregation:
        - Group by OrderId
        - Calculate total amount (PricePerUnit * Qty)
        - Count items
        """
        return order_df \
            .withColumn("TotalAmount", col("PricePerUnit") * col("Qty")) \
            .groupBy("OrderId", "CustId", "Date") \
            .agg(
                _sum("TotalAmount").alias("OrderTotal"),
                count("ItemName").alias("ItemCount")
            )

    def create_customer_aggregate_spend(self, customer_df, order_df):
        """
        Create customer aggregate spend:
        - Join customer and order data
        - Calculate total spend per customer
        - Include customer details
        """
        order_with_total = order_df.withColumn(
            "TotalAmount",
            col("PricePerUnit") * col("Qty")
        )

        customer_spend = order_with_total \
            .groupBy("CustId") \
            .agg(_sum("TotalAmount").alias("TotalSpend"))

        return customer_df.join(customer_spend, "CustId", "left") \
            .withColumn("TotalSpend", when(col("TotalSpend").isNull(), lit(0)).otherwise(col("TotalSpend")))

    def run(self):
        """Main execution method"""
        print("Starting Customer and Order Data Processing Pipeline...")

        # Initialize job
        self.job.init("customer-order-processing-job", {})

        # Step 1: Read customer data
        print("Reading customer data...")
        customer_raw = self.read_customer_data()

        # Step 2: Clean customer data
        print("Cleaning customer data...")
        customer_cleaned = self.clean_data(customer_raw)

        # Step 3: Add SCD Type 2 columns to customer data
        print("Adding SCD Type 2 columns to customer data...")
        customer_scd2 = self.add_scd2_columns(customer_cleaned)

        # Step 4: Write customer data in Hudi format
        print("Writing customer data in Hudi format...")
        self.write_hudi_scd2(
            customer_scd2,
            table_name='customer',
            record_key='CustId',
            precombine_field='OpTs'
        )

        # Step 5: Register customer table in Glue Catalog
        print("Registering customer table in Glue Catalog...")
        self.register_glue_table('customer', self.CURATED_CUSTOMER_PATH)

        # Step 6: Read order data
        print("Reading order data...")
        order_raw = self.read_order_data()

        # Step 7: Transform order data (parse types)
        print("Transforming order data...")
        order_transformed = self.transform_order_data(order_raw)

        # Step 8: Clean order data
        print("Cleaning order data...")
        order_cleaned = self.clean_data(order_transformed)

        # Step 9: Write order data in Parquet format
        print("Writing order data in Parquet format...")
        self.write_parquet(order_cleaned, self.CURATED_ORDER_PATH)

        # Step 10: Register order table in Glue Catalog
        print("Registering order table in Glue Catalog...")
        self.register_glue_table('order', self.CURATED_ORDER_PATH)

        # Step 11: Create order summary
        print("Creating order summary...")
        order_summary = self.create_order_summary(order_cleaned)
        self.write_parquet(order_summary, self.ORDER_SUMMARY_PATH)
        self.register_glue_table('ordersummary', self.ORDER_SUMMARY_PATH)

        # Step 12: Create customer aggregate spend
        print("Creating customer aggregate spend...")
        customer_aggregate = self.create_customer_aggregate_spend(
            customer_cleaned,
            order_cleaned
        )
        self.write_parquet(customer_aggregate, self.CUSTOMER_AGGREGATE_SPEND_PATH)
        self.register_glue_table('customeraggregatespend', self.CUSTOMER_AGGREGATE_SPEND_PATH)

        # Commit job
        self.job.commit()

        print("Pipeline execution completed successfully!")

        return {
            'customer_count': customer_scd2.count(),
            'order_count': order_cleaned.count(),
            'order_summary_count': order_summary.count(),
            'customer_aggregate_count': customer_aggregate.count()
        }


def main():
    """Entry point for the job"""
    processor = CustomerOrderProcessor()
    result = processor.run()
    print(f"Processing Results: {result}")


if __name__ == "__main__":
    main()