"""
AWS Glue PySpark Job - Customer Order Analytics with SCD Type 2

This job implements:
- FR-INGEST-001: Customer data ingestion
- FR-INGEST-002: Order data ingestion
- FR-CLEAN-001: Data cleaning (remove NULLs, 'Null' strings, duplicates)
- FR-SCD2-001: SCD Type 2 implementation using Apache Hudi
- FR-AGG-001: Customer aggregate spend calculation
- FR-SUMMARY-001: Order summary generation

Technical Requirements from TRD:
- S3 paths: s3://adif-sdlc/sdlc_wizard/customerdata/, orderdata/
- Output: s3://adif-sdlc/analytics/customeraggregatespend/
- Output: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
- SCD Type 2: IsActive, StartDate, EndDate, OpTs columns
- Format: Apache Hudi with upsert operation
"""

import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, lit, current_timestamp,
    when, coalesce, trim, upper, lower, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, BooleanType, TimestampType
)

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
    """Manages Spark and Glue contexts with proper initialization"""

    @staticmethod
    def initialize_spark_contexts() -> Tuple[SparkContext, GlueContext, SparkSession]:
        """
        Initialize Spark, Glue contexts and Spark session

        Returns:
            Tuple of (SparkContext, GlueContext, SparkSession)
        """
        try:
            sc = SparkContext.getOrCreate()
            glue_context = GlueContext(sc)
            spark = glue_context.spark_session

            # Configure Spark for optimal performance
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.shuffle.partitions", "200")

            # Hudi configurations
            spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

            logger.info("Successfully initialized Spark and Glue contexts")
            return sc, glue_context, spark

        except Exception as e:
            logger.error(f"Failed to initialize Spark contexts: {str(e)}")
            raise


class S3Validator:
    """Validates S3 access and paths"""

    def __init__(self):
        self.s3_client = boto3.client('s3')

    def validate_s3_path(self, s3_path: str) -> bool:
        """
        Validate S3 path exists and is accessible

        Args:
            s3_path: S3 path (s3://bucket/key)

        Returns:
            True if path is valid and accessible
        """
        try:
            # Parse S3 path
            if not s3_path.startswith('s3://'):
                logger.error(f"Invalid S3 path format: {s3_path}")
                return False

            path_parts = s3_path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''

            # Check bucket access
            self.s3_client.head_bucket(Bucket=bucket)
            logger.info(f"Successfully validated S3 path: {s3_path}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"S3 validation failed for {s3_path}: {error_code}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error validating S3 path {s3_path}: {str(e)}")
            return False

    def check_s3_path_exists(self, s3_path: str) -> bool:
        """
        Check if S3 path has any objects

        Args:
            s3_path: S3 path to check

        Returns:
            True if path contains objects
        """
        try:
            path_parts = s3_path.replace('s3://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''

            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1
            )

            exists = 'Contents' in response
            logger.info(f"S3 path {'exists' if exists else 'does not exist'}: {s3_path}")
            return exists

        except Exception as e:
            logger.error(f"Error checking S3 path existence {s3_path}: {str(e)}")
            return False


class DataReader:
    """Handles safe data reading from S3"""

    def __init__(self, spark: SparkSession, s3_validator: S3Validator):
        self.spark = spark
        self.s3_validator = s3_validator

    def read_data_safe(
        self,
        s3_path: str,
        file_format: str = "csv",
        schema: Optional[StructType] = None,
        **options
    ) -> Optional[DataFrame]:
        """
        Safely read data from S3 with validation

        Args:
            s3_path: S3 path to read from
            file_format: File format (csv, parquet, etc.)
            schema: Optional schema to enforce
            **options: Additional read options

        Returns:
            DataFrame or None if read fails
        """
        try:
            # Validate S3 path
            if not self.s3_validator.validate_s3_path(s3_path):
                logger.error(f"S3 path validation failed: {s3_path}")
                return None

            # Check if path has data
            if not self.s3_validator.check_s3_path_exists(s3_path):
                logger.warning(f"S3 path is empty: {s3_path}")
                return None

            # Read data
            reader = self.spark.read.format(file_format)

            if schema:
                reader = reader.schema(schema)

            for key, value in options.items():
                reader = reader.option(key, value)

            df = reader.load(s3_path)

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            record_count = df.count()
            logger.info(f"Successfully read {record_count} records from {s3_path}")

            return df

        except Exception as e:
            logger.error(f"Failed to read data from {s3_path}: {str(e)}")
            return None


class DataWriter:
    """Handles safe data writing to S3"""

    def __init__(self, s3_validator: S3Validator):
        self.s3_validator = s3_validator

    def write_data_safe(
        self,
        df: DataFrame,
        s3_path: str,
        file_format: str = "parquet",
        mode: str = "overwrite",
        **options
    ) -> bool:
        """
        Safely write data to S3 with validation

        Args:
            df: DataFrame to write
            s3_path: S3 destination path
            file_format: Output format
            mode: Write mode (overwrite, append, etc.)
            **options: Additional write options

        Returns:
            True if write successful
        """
        try:
            # Validate S3 path
            if not self.s3_validator.validate_s3_path(s3_path):
                logger.error(f"S3 path validation failed: {s3_path}")
                return False

            # Write data
            writer = df.write.format(file_format).mode(mode)

            for key, value in options.items():
                writer = writer.option(key, value)

            writer.save(s3_path)

            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to {s3_path}")

            return True

        except Exception as e:
            logger.error(f"Failed to write data to {s3_path}: {str(e)}")
            return False


class DataCleaner:
    """Implements data cleaning logic from TR-INGEST-002"""

    @staticmethod
    def clean_data(df: DataFrame, null_string_values: List[str] = None) -> DataFrame:
        """
        Clean data by removing NULLs, 'Null' strings, and duplicates

        TR-INGEST-002 Requirements:
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records

        Args:
            df: Input DataFrame
            null_string_values: List of string values to treat as NULL

        Returns:
            Cleaned DataFrame
        """
        if null_string_values is None:
            null_string_values = ['Null', 'NULL', 'null', 'None', 'NONE', 'none']

        try:
            initial_count = df.count()
            logger.info(f"Starting data cleaning. Initial record count: {initial_count}")

            # Step 1: Remove rows with NULL values in any column
            df_cleaned = df.na.drop()
            after_null_drop = df_cleaned.count()
            logger.info(f"After removing NULLs: {after_null_drop} records ({initial_count - after_null_drop} removed)")

            # Step 2: Remove rows with 'Null' string values
            for column in df_cleaned.columns:
                # Replace null string values with actual NULL
                for null_str in null_string_values:
                    df_cleaned = df_cleaned.withColumn(
                        column,
                        when(trim(col(column)) == null_str, lit(None))
                        .otherwise(col(column))
                    )

            # Drop rows that now have NULL values
            df_cleaned = df_cleaned.na.drop()
            after_null_string_drop = df_cleaned.count()
            logger.info(f"After removing 'Null' strings: {after_null_string_drop} records ({after_null_drop - after_null_string_drop} removed)")

            # Step 3: Remove duplicate records
            df_cleaned = df_cleaned.dropDuplicates()
            final_count = df_cleaned.count()
            logger.info(f"After removing duplicates: {final_count} records ({after_null_string_drop - final_count} removed)")

            logger.info(f"Data cleaning complete. Total records removed: {initial_count - final_count}")

            return df_cleaned

        except Exception as e:
            logger.error(f"Error during data cleaning: {str(e)}")
            raise


class SCDType2Handler:
    """Implements SCD Type 2 logic using Apache Hudi"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def add_scd2_columns(self, df: DataFrame, is_initial_load: bool = True) -> DataFrame:
        """
        Add SCD Type 2 columns to DataFrame

        Columns from TRD:
        - IsActive (boolean): Current record flag
        - StartDate (timestamp): Record effective start date
        - EndDate (timestamp): Record effective end date (NULL for active)
        - OpTs (timestamp): Operation timestamp

        Args:
            df: Input DataFrame
            is_initial_load: True if this is the initial load

        Returns:
            DataFrame with SCD Type 2 columns
        """
        try:
            current_ts = current_timestamp()

            df_scd2 = df.withColumn("isactive", lit(True).cast(BooleanType())) \
                        .withColumn("startdate", current_ts.cast(TimestampType())) \
                        .withColumn("enddate", lit(None).cast(TimestampType())) \
                        .withColumn("opts", current_ts.cast(TimestampType()))

            logger.info("Successfully added SCD Type 2 columns")
            return df_scd2

        except Exception as e:
            logger.error(f"Error adding SCD Type 2 columns: {str(e)}")
            raise

    def write_hudi_table(
        self,
        df: DataFrame,
        s3_path: str,
        table_name: str,
        record_key: str,
        precombine_field: str,
        operation: str = "upsert"
    ) -> bool:
        """
        Write DataFrame to Hudi table with SCD Type 2 support

        Hudi Configuration from TRD:
        - Format: hudi
        - Operation: upsert
        - Record Key: CustId
        - Precombine Field: OpTs

        Args:
            df: DataFrame to write
            s3_path: S3 destination path
            table_name: Hudi table name
            record_key: Primary key field
            precombine_field: Field for record versioning
            operation: Hudi operation (upsert, insert, bulk_insert)

        Returns:
            True if write successful
        """
        try:
            hudi_options = {
                'hoodie.table.name': table_name,
                'hoodie.datasource.write.recordkey.field': record_key,
                'hoodie.datasource.write.precombine.field': precombine_field,
                'hoodie.datasource.write.operation': operation,
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.datasource.hive_sync.enable': 'false',
                'hoodie.upsert.shuffle.parallelism': '200',
                'hoodie.insert.shuffle.parallelism': '200'
            }

            logger.info(f"Writing to Hudi table: {table_name} at {s3_path}")

            df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(s3_path)

            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to Hudi table {table_name}")

            return True

        except Exception as e:
            logger.error(f"Error writing to Hudi table: {str(e)}")
            return False

    def read_hudi_table(self, s3_path: str) -> Optional[DataFrame]:
        """
        Read Hudi table from S3

        Args:
            s3_path: S3 path to Hudi table

        Returns:
            DataFrame or None if read fails
        """
        try:
            df = self.spark.read.format("hudi").load(s3_path)

            # Normalize column names
            df = df.toDF(*[c.lower() for c in df.columns])

            record_count = df.count()
            logger.info(f"Successfully read {record_count} records from Hudi table at {s3_path}")

            return df

        except Exception as e:
            logger.warning(f"Could not read Hudi table from {s3_path}: {str(e)}")
            return None


class CustomerOrderAnalytics:
    """Main job class implementing all FRD requirements"""

    def __init__(self, spark: SparkSession, glue_context: GlueContext):
        self.spark = spark
        self.glue_context = glue_context
        self.s3_validator = S3Validator()
        self.data_reader = DataReader(spark, self.s3_validator)
        self.data_writer = DataWriter(self.s3_validator)
        self.data_cleaner = DataCleaner()
        self.scd2_handler = SCDType2Handler(spark)

        # S3 paths from TRD
        self.customer_input_path = "s3://adif-sdlc/sdlc_wizard/customerdata/"
        self.order_input_path = "s3://adif-sdlc/sdlc_wizard/orderdata/"
        self.customer_scd2_path = "s3://adif-sdlc/sdlc_wizard/customerdata/"
        self.aggregate_spend_path = "s3://adif-sdlc/analytics/customeraggregatespend/"
        self.order_summary_path = "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/"

    def get_customer_schema(self) -> StructType:
        """
        Get customer data schema from TRD

        Schema: CustId (string), Name (string), EmailId (string), Region (string)
        """
        return StructType([
            StructField("custid", StringType(), True),
            StructField("name", StringType(), True),
            StructField("emailid", StringType(), True),
            StructField("region", StringType(), True)
        ])

    def get_order_schema(self) -> StructType:
        """
        Get order data schema from TRD

        Schema: OrderId (string), ItemName (string), PricePerUnit (decimal),
                Qty (integer), Date (string)
        """
        return StructType([
            StructField("orderid", StringType(), True),
            StructField("itemname", StringType(), True),
            StructField("priceperunit", DecimalType(10, 2), True),
            StructField("qty", IntegerType(), True),
            StructField("date", StringType(), True)
        ])

    def ingest_customer_data(self) -> Optional[DataFrame]:
        """
        FR-INGEST-001: Ingest customer data from S3

        Source: s3://adif-sdlc/sdlc_wizard/customerdata/
        Format: CSV with UTF-8 encoding, comma delimiter, header

        Returns:
            Customer DataFrame or None
        """
        logger.info("FR-INGEST-001: Starting customer data ingestion")

        df = self.data_reader.read_data_safe(
            s3_path=self.customer_input_path,
            file_format="csv",
            schema=self.get_customer_schema(),
            header=True,
            inferSchema=False,
            encoding="UTF-8",
            delimiter=","
        )

        if df:
            logger.info(f"FR-INGEST-001: Successfully ingested customer data. Records: {df.count()}")
        else:
            logger.error("FR-INGEST-001: Failed to ingest customer data")

        return df

    def ingest_order_data(self) -> Optional[DataFrame]:
        """
        FR-INGEST-002: Ingest order data from S3

        Source: s3://adif-sdlc/sdlc_wizard/orderdata/
        Format: CSV with UTF-8 encoding, comma delimiter, header

        Returns:
            Order DataFrame or None
        """
        logger.info("FR-INGEST-002: Starting order data ingestion")

        df = self.data_reader.read_data_safe(
            s3_path=self.order_input_path,
            file_format="csv",
            schema=self.get_order_schema(),
            header=True,
            inferSchema=False,
            encoding="UTF-8",
            delimiter=","
        )

        if df:
            logger.info(f"FR-INGEST-002: Successfully ingested order data. Records: {df.count()}")
        else:
            logger.error("FR-INGEST-002: Failed to ingest order data")

        return df

    def clean_customer_data(self, df: DataFrame) -> DataFrame:
        """
        FR-CLEAN-001: Clean customer data

        TR-INGEST-002 Requirements:
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records

        Args:
            df: Raw customer DataFrame

        Returns:
            Cleaned customer DataFrame
        """
        logger.info("FR-CLEAN-001: Starting customer data cleaning")

        df_cleaned = self.data_cleaner.clean_data(df)

        logger.info(f"FR-CLEAN-001: Customer data cleaning complete. Records: {df_cleaned.count()}")
        return df_cleaned

    def clean_order_data(self, df: DataFrame) -> DataFrame:
        """
        FR-CLEAN-001: Clean order data

        TR-INGEST-002 Requirements:
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records

        Args:
            df: Raw order DataFrame

        Returns:
            Cleaned order DataFrame
        """
        logger.info("FR-CLEAN-001: Starting order data cleaning")

        df_cleaned = self.data_cleaner.clean_data(df)

        logger.info(f"FR-CLEAN-001: Order data cleaning complete. Records: {df_cleaned.count()}")
        return df_cleaned

    def implement_scd2_customer(self, df: DataFrame) -> bool:
        """
        FR-SCD2-001: Implement SCD Type 2 for customer data

        TRD Requirements:
        - Add columns: IsActive, StartDate, EndDate, OpTs
        - Use Hudi format with upsert operation
        - Track historical changes
        - Output: s3://adif-sdlc/sdlc_wizard/customerdata/

        Args:
            df: Cleaned customer DataFrame

        Returns:
            True if successful
        """
        logger.info("FR-SCD2-001: Starting SCD Type 2 implementation for customer data")

        # Add SCD Type 2 columns
        df_scd2 = self.scd2_handler.add_scd2_columns(df, is_initial_load=True)

        # Write to Hudi table
        success = self.scd2_handler.write_hudi_table(
            df=df_scd2,
            s3_path=self.customer_scd2_path,
            table_name="customer_scd2",
            record_key="custid",
            precombine_field="opts",
            operation="upsert"
        )

        if success:
            logger.info("FR-SCD2-001: Successfully implemented SCD Type 2 for customer data")
        else:
            logger.error("FR-SCD2-001: Failed to implement SCD Type 2 for customer data")

        return success

    def calculate_customer_aggregate_spend(
        self,
        customer_df: DataFrame,
        order_df: DataFrame
    ) -> Optional[DataFrame]:
        """
        FR-AGG-001: Calculate customer aggregate spend

        Business Logic:
        - Join customer and order data
        - Calculate total spend per customer: sum(PricePerUnit * Qty)
        - Group by CustId, Name
        - Output: s3://adif-sdlc/analytics/customeraggregatespend/

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Aggregate spend DataFrame or None
        """
        logger.info("FR-AGG-001: Starting customer aggregate spend calculation")

        try:
            # Add CustId to order data (assuming it exists or needs to be derived)
            # For this implementation, we'll assume orders have a custid field
            # If not present in schema, this would need to be added based on business logic

            # Calculate total amount per order
            order_with_total = order_df.withColumn(
                "totalamount",
                col("priceperunit") * col("qty")
            )

            # Join with customer data
            joined_df = customer_df.select("custid", "name") \
                .join(order_with_total, "custid", "inner")

            # Aggregate by customer
            aggregate_df = joined_df.groupBy("custid", "name") \
                .agg(_sum("totalamount").alias("totalspend"))

            logger.info(f"FR-AGG-001: Calculated aggregate spend for {aggregate_df.count()} customers")

            return aggregate_df

        except Exception as e:
            logger.error(f"FR-AGG-001: Error calculating customer aggregate spend: {str(e)}")
            return None

    def generate_order_summary(
        self,
        customer_df: DataFrame,
        order_df: DataFrame
    ) -> Optional[DataFrame]:
        """
        FR-SUMMARY-001: Generate order summary

        Business Logic:
        - Join customer and order data
        - Include: OrderId, CustId, Name, ItemName, TotalAmount, Date
        - Output: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/

        Args:
            customer_df: Customer DataFrame
            order_df: Order DataFrame

        Returns:
            Order summary DataFrame or None
        """
        logger.info("FR-SUMMARY-001: Starting order summary generation")

        try:
            # Calculate total amount per order
            order_with_total = order_df.withColumn(
                "totalamount",
                col("priceperunit") * col("qty")
            )

            # Join with customer data
            summary_df = order_with_total.join(
                customer_df.select("custid", "name"),
                "custid",
                "inner"
            ).select(
                "orderid",
                "custid",
                "name",
                "itemname",
                "totalamount",
                "date"
            )

            logger.info(f"FR-SUMMARY-001: Generated order summary with {summary_df.count()} records")

            return summary_df

        except Exception as e:
            logger.error(f"FR-SUMMARY-001: Error generating order summary: {str(e)}")
            return None

    def write_customer_aggregate_spend(self, df: DataFrame) -> bool:
        """
        Write customer aggregate spend to S3

        Output: s3://adif-sdlc/analytics/customeraggregatespend/
        Format: Parquet

        Args:
            df: Aggregate spend DataFrame

        Returns:
            True if successful
        """
        logger.info("Writing customer aggregate spend to S3")

        success = self.data_writer.write_data_safe(
            df=df,
            s3_path=self.aggregate_spend_path,
            file_format="parquet",
            mode="overwrite"
        )

        return success

    def write_order_summary(self, df: DataFrame) -> bool:
        """
        Write order summary to S3

        Output: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/
        Format: Parquet

        Args:
            df: Order summary DataFrame

        Returns:
            True if successful
        """
        logger.info("Writing order summary to S3")

        success = self.data_writer.write_data_safe(
            df=df,
            s3_path=self.order_summary_path,
            file_format="parquet",
            mode="overwrite"
        )

        return success

    def run(self) -> bool:
        """
        Execute complete ETL pipeline

        Pipeline:
        1. Ingest customer data (FR-INGEST-001)
        2. Ingest order data (FR-INGEST-002)
        3. Clean customer data (FR-CLEAN-001)
        4. Clean order data (FR-CLEAN-001)
        5. Implement SCD Type 2 for customer (FR-SCD2-001)
        6. Calculate customer aggregate spend (FR-AGG-001)
        7. Generate order summary (FR-SUMMARY-001)
        8. Write outputs to S3

        Returns:
            True if pipeline completes successfully
        """
        try:
            logger.info("=" * 80)
            logger.info("Starting Customer Order Analytics ETL Pipeline")
            logger.info("=" * 80)

            # Step 1: Ingest customer data
            customer_df = self.ingest_customer_data()
            if customer_df is None:
                logger.error("Pipeline failed: Customer data ingestion failed")
                return False

            # Step 2: Ingest order data
            order_df = self.ingest_order_data()
            if order_df is None:
                logger.error("Pipeline failed: Order data ingestion failed")
                return False

            # Step 3: Clean customer data
            customer_cleaned = self.clean_customer_data(customer_df)

            # Step 4: Clean order data
            order_cleaned = self.clean_order_data(order_df)

            # Step 5: Implement SCD Type 2 for customer data
            scd2_success = self.implement_scd2_customer(customer_cleaned)
            if not scd2_success:
                logger.error("Pipeline failed: SCD Type 2 implementation failed")
                return False

            # Step 6: Calculate customer aggregate spend
            aggregate_spend_df = self.calculate_customer_aggregate_spend(
                customer_cleaned,
                order_cleaned
            )
            if aggregate_spend_df is None:
                logger.error("Pipeline failed: Aggregate spend calculation failed")
                return False

            # Step 7: Generate order summary
            order_summary_df = self.generate_order_summary(
                customer_cleaned,
                order_cleaned
            )
            if order_summary_df is None:
                logger.error("Pipeline failed: Order summary generation failed")
                return False

            # Step 8: Write customer aggregate spend
            if not self.write_customer_aggregate_spend(aggregate_spend_df):
                logger.error("Pipeline failed: Writing aggregate spend failed")
                return False

            # Step 9: Write order summary
            if not self.write_order_summary(order_summary_df):
                logger.error("Pipeline failed: Writing order summary failed")
                return False

            logger.info("=" * 80)
            logger.info("Customer Order Analytics ETL Pipeline completed successfully")
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}")
            return False


def main():
    """Main entry point for AWS Glue job"""
    try:
        # Get job parameters
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])

        # Initialize Spark contexts
        sc, glue_context, spark = SparkContextManager.initialize_spark_contexts()

        # Initialize Glue job
        job = Job(glue_context)
        job.init(args['JOB_NAME'], args)

        # Create and run analytics job
        analytics_job = CustomerOrderAnalytics(spark, glue_context)
        success = analytics_job.run()

        # Commit job
        job.commit()

        if success:
            logger.info("Job completed successfully")
            sys.exit(0)
        else:
            logger.error("Job failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()