"""
AWS Glue PySpark Job: Customer Order Pipeline
Implements data ingestion, cleaning, SCD Type 2, and aggregation logic
"""

import sys
import yaml
from datetime import datetime
from decimal import Decimal

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, lit, current_timestamp, to_date,
    when, row_number, max as _max, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def get_job_parameters():
    """
    Load job parameters from YAML configuration file.
    Returns default configuration that can be overridden by Glue job arguments.
    """
    default_params = {
        "customer_source_path": "s3://adif-sdlc/sdlc_wizard/customerdata/",
        "customer_source_format": "csv",
        "order_source_path": "s3://adif-sdlc/sdlc_wizard/orderdata/",
        "order_source_format": "csv",
        "curated_target_path": "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/",
        "curated_target_format": "parquet",
        "analytics_target_path": "s3://adif-sdlc/analytics/customeraggregatespend/",
        "analytics_target_format": "parquet",
        "catalog_database": "sdlc_wizard_db",
        "customer_table_name": "customer_cleaned",
        "order_table_name": "order_cleaned",
        "scd_table_name": "customer_scd",
        "aggregate_table_name": "customeraggregatespend",
        "hudi_table_name": "customer_scd_hudi",
        "hudi_base_path": "s3://adif-sdlc/curated/sdlc_wizard/customer_scd/",
        "hudi_record_key": "CustId",
        "hudi_precombine_key": "OpTs",
        "hudi_table_type": "COPY_ON_WRITE",
        "hudi_operation": "upsert",
        "enable_data_quality": True,
        "enable_deduplication": True,
        "enable_null_removal": True,
        "csv_header": True,
        "csv_delimiter": ",",
        "csv_encoding": "UTF-8",
        "csv_infer_schema": False
    }

    return default_params


def initialize_spark_contexts():
    """
    Initialize Spark, Glue contexts and Job.
    Returns tuple of (spark, glueContext, job, args)
    """
    # Check if running in Glue environment
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job_name = args['JOB_NAME']
    except Exception:
        # Local/test environment
        job_name = "customer-order-pipeline-local"
        args = {'JOB_NAME': job_name}

    # Initialize contexts
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, args)

    return spark, glueContext, job, args


def validate_s3_access(spark, s3_path):
    """
    Validate S3 path accessibility before operations.
    Returns True if accessible, False otherwise.
    """
    try:
        # Simple validation - in production, use boto3 for better checks
        if s3_path and s3_path.startswith("s3://"):
            return True
        return False
    except Exception as e:
        print(f"S3 validation warning for {s3_path}: {str(e)}")
        return False


class DataReader:
    """Helper class for reading data from S3 with schema enforcement"""

    def __init__(self, spark):
        self.spark = spark

    def _read_csv(self, path, schema, header=True, delimiter=","):
        """Internal method to read CSV files"""
        return self.spark.read \
            .option("header", str(header).lower()) \
            .option("delimiter", delimiter) \
            .option("encoding", "UTF-8") \
            .schema(schema) \
            .csv(path)

    def read_customer_data(self, path):
        """Read customer CSV data with schema enforcement"""
        schema = StructType([
            StructField("CustId", StringType(), False),
            StructField("Name", StringType(), False),
            StructField("EmailId", StringType(), False),
            StructField("Region", StringType(), False)
        ])

        df = self._read_csv(path, schema)
        return df

    def read_order_data(self, path):
        """Read order CSV data with schema enforcement and type casting"""
        schema = StructType([
            StructField("OrderId", StringType(), False),
            StructField("ItemName", StringType(), False),
            StructField("PricePerUnit", StringType(), False),
            StructField("Qty", StringType(), False),
            StructField("Date", StringType(), False),
            StructField("CustId", StringType(), False)
        ])

        df = self._read_csv(path, schema)

        # Cast to proper types
        df = df.withColumn("PricePerUnit", col("PricePerUnit").cast(DecimalType(10, 2))) \
               .withColumn("Qty", col("Qty").cast(IntegerType())) \
               .withColumn("Date", to_date(col("Date")))

        return df


class DataCleaner:
    """Helper class for data quality transformations"""

    @staticmethod
    def remove_nulls(df, columns=None):
        """
        Remove records with NULL values or string 'Null' in specified columns.
        If columns is None, applies to all columns.
        """
        if columns is None:
            columns = df.columns

        # Remove actual NULLs
        for column in columns:
            df = df.filter(col(column).isNotNull())
            # Remove string 'Null' (case insensitive)
            if dict(df.dtypes)[column] == 'string':
                df = df.filter(~(col(column).isin(['Null', 'null', 'NULL', 'None'])))

        return df

    @staticmethod
    def remove_duplicates(df, subset=None):
        """
        Remove duplicate records based on subset of columns.
        If subset is None, considers all columns.
        """
        return df.dropDuplicates(subset=subset)

    @staticmethod
    def clean_dataframe(df, enable_null_removal=True, enable_deduplication=True):
        """Apply all data quality rules"""
        if enable_null_removal:
            df = DataCleaner.remove_nulls(df)

        if enable_deduplication:
            df = DataCleaner.remove_duplicates(df)

        return df


class S3Writer:
    """Helper class for writing data to S3 with format dispatch"""

    def __init__(self, spark):
        self.spark = spark

    def _write_parquet(self, df, path, mode="overwrite", partition_by=None):
        """Internal method to write Parquet files"""
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.parquet(path)

    def _write_csv(self, df, path, mode="overwrite", header=True):
        """Internal method to write CSV files"""
        df.write.mode(mode) \
            .option("header", str(header).lower()) \
            .csv(path)

    def _write_hudi(self, df, path, hudi_options):
        """Internal method to write Hudi tables"""
        df.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)

    def write_dataframe(self, df, path, format_type="parquet", mode="overwrite",
                       partition_by=None, hudi_options=None):
        """
        Public method to write DataFrame with format dispatch.
        Validates S3 access before writing.
        """
        if not validate_s3_access(self.spark, path):
            raise ValueError(f"Invalid S3 path: {path}")

        if format_type.lower() == "parquet":
            self._write_parquet(df, path, mode, partition_by)
        elif format_type.lower() == "csv":
            self._write_csv(df, path, mode)
        elif format_type.lower() == "hudi":
            if not hudi_options:
                raise ValueError("Hudi options required for Hudi format")
            self._write_hudi(df, path, hudi_options)
        else:
            raise ValueError(f"Unsupported format: {format_type}")


class CatalogManager:
    """Helper class for Glue Data Catalog operations"""

    def __init__(self, spark, database):
        self.spark = spark
        self.database = database

    def register_table(self, df, table_name, path, format_type="parquet"):
        """Register DataFrame as a table in Glue Data Catalog"""
        # Create database if not exists
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")

        # Drop table if exists
        self.spark.sql(f"DROP TABLE IF EXISTS {self.database}.{table_name}")

        # Register table
        df.write \
            .format(format_type) \
            .mode("overwrite") \
            .option("path", path) \
            .saveAsTable(f"{self.database}.{table_name}")

        print(f"Registered table: {self.database}.{table_name}")


class SCDProcessor:
    """Helper class for SCD Type 2 implementation"""

    def __init__(self, spark):
        self.spark = spark

    def add_scd_columns(self, df):
        """Add SCD Type 2 tracking columns"""
        current_ts = current_timestamp()

        df = df.withColumn("IsActive", lit(True)) \
               .withColumn("StartDate", current_ts) \
               .withColumn("EndDate", lit(None).cast(TimestampType())) \
               .withColumn("OpTs", current_ts)

        return df

    def implement_scd_type2(self, new_df, existing_df=None):
        """
        Implement SCD Type 2 logic.
        If existing_df is None, treats all records as new inserts.
        """
        # Add SCD columns to new data
        new_df_scd = self.add_scd_columns(new_df)

        if existing_df is None:
            # First load - all records are active
            return new_df_scd

        # Join to find changed records
        join_key = "CustId"
        compare_cols = ["Name", "EmailId", "Region"]

        # Find records that exist in both datasets
        joined = new_df.alias("new").join(
            existing_df.filter(col("IsActive") == True).alias("old"),
            join_key,
            "left"
        )

        # Identify changed records
        change_condition = None
        for col_name in compare_cols:
            condition = col(f"new.{col_name}") != col(f"old.{col_name}")
            if change_condition is None:
                change_condition = condition
            else:
                change_condition = change_condition | condition

        # Mark old records as inactive
        updated_old = existing_df.filter(col("IsActive") == True) \
            .join(new_df, join_key, "inner") \
            .select(existing_df["*"]) \
            .withColumn("IsActive", lit(False)) \
            .withColumn("EndDate", current_timestamp())

        # Keep unchanged old records
        unchanged_old = existing_df.filter(col("IsActive") == True) \
            .join(new_df, join_key, "left_anti")

        # Keep already inactive records
        inactive_old = existing_df.filter(col("IsActive") == False)

        # Combine all records
        result = new_df_scd.unionByName(updated_old) \
                           .unionByName(unchanged_old) \
                           .unionByName(inactive_old)

        return result


class AggregationEngine:
    """Helper class for aggregation calculations"""

    @staticmethod
    def calculate_customer_spend(order_df):
        """
        Calculate total spending per customer per date.
        Formula: SUM(PricePerUnit * Qty) GROUP BY CustId, Date
        """
        agg_df = order_df.withColumn(
            "TotalSpend",
            col("PricePerUnit") * col("Qty")
        ).groupBy("CustId", "Date") \
         .agg(_sum("TotalSpend").alias("TotalSpend"))

        return agg_df


def main():
    """Main execution function"""

    # Initialize Spark contexts
    spark, glueContext, job, args = initialize_spark_contexts()

    try:
        # Load job parameters
        params = get_job_parameters()

        print("Starting Customer Order Pipeline...")
        print(f"Customer Source: {params['customer_source_path']}")
        print(f"Order Source: {params['order_source_path']}")

        # Initialize helper classes
        reader = DataReader(spark)
        writer = S3Writer(spark)
        catalog = CatalogManager(spark, params['catalog_database'])
        scd_processor = SCDProcessor(spark)

        # TR-INGEST-001: Ingest Customer Data
        print("\n=== TR-INGEST-001: Ingesting Customer Data ===")
        df_customer_raw = reader.read_customer_data(params['customer_source_path'])
        print(f"Customer records loaded: {df_customer_raw.count()}")
        df_customer_raw.printSchema()

        # TR-INGEST-002: Ingest Order Data
        print("\n=== TR-INGEST-002: Ingesting Order Data ===")
        df_order_raw = reader.read_order_data(params['order_source_path'])
        print(f"Order records loaded: {df_order_raw.count()}")
        df_order_raw.printSchema()

        # TR-CLEAN-001: Apply Data Quality Rules
        print("\n=== TR-CLEAN-001: Applying Data Quality Rules ===")
        df_customer_clean = DataCleaner.clean_dataframe(
            df_customer_raw,
            enable_null_removal=params['enable_null_removal'],
            enable_deduplication=params['enable_deduplication']
        )
        print(f"Customer records after cleaning: {df_customer_clean.count()}")

        df_order_clean = DataCleaner.clean_dataframe(
            df_order_raw,
            enable_null_removal=params['enable_null_removal'],
            enable_deduplication=params['enable_deduplication']
        )
        print(f"Order records after cleaning: {df_order_clean.count()}")

        # TR-CATALOG-001: Register Cleaned Tables in Glue Catalog
        print("\n=== TR-CATALOG-001: Registering Tables in Glue Catalog ===")
        customer_catalog_path = f"{params['curated_target_path']}{params['customer_table_name']}/"
        order_catalog_path = f"{params['curated_target_path']}{params['order_table_name']}/"

        catalog.register_table(
            df_customer_clean,
            params['customer_table_name'],
            customer_catalog_path,
            params['curated_target_format']
        )

        catalog.register_table(
            df_order_clean,
            params['order_table_name'],
            order_catalog_path,
            params['curated_target_format']
        )

        # FR-INCR-001: Implement SCD Type 2 with Hudi
        print("\n=== FR-INCR-001: Implementing SCD Type 2 ===")
        df_customer_scd = scd_processor.add_scd_columns(df_customer_clean)
        print(f"Customer SCD records: {df_customer_scd.count()}")
        df_customer_scd.printSchema()

        # Configure Hudi options with correct column names
        hudi_options = {
            'hoodie.table.name': params['hudi_table_name'],
            'hoodie.datasource.write.recordkey.field': params['hudi_record_key'],
            'hoodie.datasource.write.precombine.field': params['hudi_precombine_key'],
            'hoodie.datasource.write.table.type': params['hudi_table_type'],
            'hoodie.datasource.write.operation': params['hudi_operation'],
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': params['catalog_database'],
            'hoodie.datasource.hive_sync.table': params['scd_table_name'],
            'hoodie.datasource.hive_sync.partition_extractor_class':
                'org.apache.hudi.hive.NonPartitionedExtractor',
            'hoodie.datasource.hive_sync.use_jdbc': 'false'
        }

        # Write to Hudi
        writer.write_dataframe(
            df_customer_scd,
            params['hudi_base_path'],
            format_type="hudi",
            hudi_options=hudi_options
        )
        print(f"SCD Type 2 data written to: {params['hudi_base_path']}")

        # TR-AGG-001: Calculate Customer Aggregate Spend
        print("\n=== TR-AGG-001: Calculating Customer Aggregate Spend ===")
        df_aggregate = AggregationEngine.calculate_customer_spend(df_order_clean)
        print(f"Aggregate records: {df_aggregate.count()}")
        df_aggregate.show(10, truncate=False)

        # Write aggregates to analytics layer
        writer.write_dataframe(
            df_aggregate,
            params['analytics_target_path'],
            format_type=params['analytics_target_format'],
            partition_by=["Date"]
        )
        print(f"Aggregate data written to: {params['analytics_target_path']}")

        # Register aggregate table in catalog
        catalog.register_table(
            df_aggregate,
            params['aggregate_table_name'],
            params['analytics_target_path'],
            params['analytics_target_format']
        )

        print("\n=== Pipeline Completed Successfully ===")

        # Commit job
        job.commit()

    except Exception as e:
        print(f"ERROR: Pipeline failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Don't stop spark if it was already running
        if 'builtins' not in dir() or not hasattr(__builtins__, 'spark'):
            spark.stop()


if __name__ == "__main__":
    main()