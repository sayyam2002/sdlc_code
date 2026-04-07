"""
AWS Glue PySpark Job - Customer & Order Data Processing with SCD Type 2
Implements FRD/TRD requirements for data ingestion, cleaning, and historical tracking
"""

import sys
import yaml
from datetime import datetime
from typing import Dict, List, Tuple

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    when, trim, upper, sum as _sum, count, max as _max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


class CustomerOrderProcessor:
    """
    Main processor class for customer and order data with SCD Type 2 implementation.

    Implements:
    - FR-INGEST-001: Data ingestion from S3
    - FR-CLEAN-001: Data cleaning (NULL removal, deduplication)
    - FR-SCD2-001: SCD Type 2 with Hudi format
    - FR-AGGREGATE-001: Customer spend aggregation
    """

    def __init__(self, spark: SparkSession, glue_context: GlueContext, config: Dict):
        """
        Initialize processor with Spark session and configuration.

        Args:
            spark: SparkSession instance
            glue_context: GlueContext instance
            config: Configuration dictionary from glue_params.yaml
        """
        self.spark = spark
        self.glue_context = glue_context
        self.config = config
        self.database = config['glue_catalog']['database']

        # S3 paths from TRD
        self.customer_data_path = config['data_sources']['customer_data_path']
        self.order_data_path = config['data_sources']['order_data_path']
        self.customer_aggregate_path = config['data_sources']['customer_aggregate_spend_path']
        self.order_summary_path = config['data_sources']['order_summary_path']
        self.customer_catalog_path = config['catalog_paths']['customer_catalog_path']
        self.order_catalog_path = config['catalog_paths']['order_catalog_path']

        # SCD Type 2 configuration
        self.scd_config = config['scd_type2']
        self.hudi_config = config['hudi']

        print(f"[INFO] Initialized CustomerOrderProcessor")
        print(f"[INFO] Glue Database: {self.database}")
        print(f"[INFO] Customer Data Path: {self.customer_data_path}")
        print(f"[INFO] Order Data Path: {self.order_data_path}")

    def get_customer_schema(self) -> StructType:
        """
        Define customer schema based on TRD specifications.

        Schema from TRD:
        - CustId (string)
        - Name (string)
        - EmailId (string)
        - Region (string)

        Returns:
            StructType: Customer schema
        """
        return StructType([
            StructField("CustId", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("EmailId", StringType(), True),
            StructField("Region", StringType(), True)
        ])

    def get_order_schema(self) -> StructType:
        """
        Define order schema based on TRD specifications.

        Schema from TRD:
        - OrderId (string)
        - CustId (string)
        - OrderDate (string)
        - Amount (double)

        Returns:
            StructType: Order schema
        """
        return StructType([
            StructField("OrderId", StringType(), True),
            StructField("CustId", StringType(), True),
            StructField("OrderDate", StringType(), True),
            StructField("Amount", DoubleType(), True)
        ])

    def ingest_customer_data(self) -> DataFrame:
        """
        Ingest customer data from S3 source.
        Implements FR-INGEST-001.

        Source: s3://adif-sdlc/sdlc_wizard/customerdata/

        Returns:
            DataFrame: Raw customer data
        """
        print(f"[INFO] Ingesting customer data from: {self.customer_data_path}")

        try:
            customer_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(self.get_customer_schema()) \
                .csv(self.customer_data_path)

            record_count = customer_df.count()
            print(f"[INFO] Successfully ingested {record_count} customer records")

            return customer_df

        except Exception as e:
            print(f"[ERROR] Failed to ingest customer data: {str(e)}")
            raise

    def ingest_order_data(self) -> DataFrame:
        """
        Ingest order data from S3 source.
        Implements FR-INGEST-001.

        Source: s3://adif-sdlc/sdlc_wizard/orderdata/

        Returns:
            DataFrame: Raw order data
        """
        print(f"[INFO] Ingesting order data from: {self.order_data_path}")

        try:
            order_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(self.get_order_schema()) \
                .csv(self.order_data_path)

            record_count = order_df.count()
            print(f"[INFO] Successfully ingested {record_count} order records")

            return order_df

        except Exception as e:
            print(f"[ERROR] Failed to ingest order data: {str(e)}")
            raise

    def clean_data(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Clean data by removing NULLs, 'Null' strings, and duplicates.
        Implements FR-CLEAN-001.

        Cleaning rules from TRD:
        - Remove NULL values
        - Remove 'Null' string values
        - Remove duplicate records

        Args:
            df: Input DataFrame
            dataset_name: Name of dataset for logging

        Returns:
            DataFrame: Cleaned data
        """
        print(f"[INFO] Cleaning {dataset_name} data")

        initial_count = df.count()
        print(f"[INFO] Initial record count: {initial_count}")

        # Step 1: Remove rows with NULL values in any column
        cleaned_df = df.dropna(how='any')
        after_null_removal = cleaned_df.count()
        print(f"[INFO] After NULL removal: {after_null_removal} records")

        # Step 2: Remove rows with 'Null' string values
        null_strings = self.config['data_cleaning']['null_string_values']

        for column in cleaned_df.columns:
            for null_str in null_strings:
                cleaned_df = cleaned_df.filter(
                    ~(trim(upper(col(column))) == null_str.upper())
                )

        after_null_string_removal = cleaned_df.count()
        print(f"[INFO] After 'Null' string removal: {after_null_string_removal} records")

        # Step 3: Remove duplicate records
        cleaned_df = cleaned_df.dropDuplicates()
        final_count = cleaned_df.count()
        print(f"[INFO] After duplicate removal: {final_count} records")

        removed_count = initial_count - final_count
        print(f"[INFO] Total records removed: {removed_count}")

        return cleaned_df

    def add_scd_type2_columns(self, df: DataFrame, is_new_record: bool = True) -> DataFrame:
        """
        Add SCD Type 2 columns to DataFrame.
        Implements FR-SCD2-001.

        SCD Type 2 columns from TRD:
        - IsActive (boolean): True for current records
        - StartDate (timestamp): Record effective start date
        - EndDate (timestamp): Record effective end date (9999-12-31 for active)
        - OpTs (timestamp): Operation timestamp

        Args:
            df: Input DataFrame
            is_new_record: Whether this is a new record (True) or historical (False)

        Returns:
            DataFrame: DataFrame with SCD Type 2 columns
        """
        print(f"[INFO] Adding SCD Type 2 columns")

        current_ts = current_timestamp()
        default_end_date = self.scd_config['default_end_date']

        scd_df = df \
            .withColumn(self.scd_config['columns']['is_active'], lit(is_new_record)) \
            .withColumn(self.scd_config['columns']['start_date'], current_ts) \
            .withColumn(
                self.scd_config['columns']['end_date'],
                to_timestamp(lit(default_end_date), 'yyyy-MM-dd HH:mm:ss')
            ) \
            .withColumn(self.scd_config['columns']['operation_timestamp'], current_ts)

        print(f"[INFO] SCD Type 2 columns added successfully")

        return scd_df

    def write_to_hudi(self, df: DataFrame, table_config: Dict, catalog_path: str,
                      table_name: str) -> None:
        """
        Write DataFrame to S3 using Hudi format with upsert operation.
        Implements FR-SCD2-001 with Hudi.

        Hudi configuration from TRD:
        - Format: hudi
        - Operation: upsert
        - Record key: CustId/OrderId
        - Precombine field: OpTs

        Args:
            df: DataFrame to write
            table_config: Hudi table configuration
            catalog_path: S3 catalog path
            table_name: Glue catalog table name
        """
        print(f"[INFO] Writing to Hudi table: {table_config['table_name']}")
        print(f"[INFO] Catalog path: {catalog_path}")

        hudi_options = {
            'hoodie.table.name': table_config['table_name'],
            'hoodie.datasource.write.recordkey.field': table_config['record_key'],
            'hoodie.datasource.write.precombine.field': table_config['precombine_field'],
            'hoodie.datasource.write.partitionpath.field': table_config['partition_path'],
            'hoodie.datasource.write.operation': table_config['operation'],
            'hoodie.datasource.write.table.type': table_config['table_type'],
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': self.database,
            'hoodie.datasource.hive_sync.table': table_name,
            'hoodie.datasource.hive_sync.partition_fields': table_config['partition_path'],
            'hoodie.datasource.hive_sync.partition_extractor_class':
                'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.hive_sync.mode': 'hms'
        }

        try:
            df.write \
                .format('hudi') \
                .options(**hudi_options) \
                .mode('append') \
                .save(catalog_path)

            record_count = df.count()
            print(f"[INFO] Successfully wrote {record_count} records to Hudi table")

        except Exception as e:
            print(f"[ERROR] Failed to write to Hudi: {str(e)}")
            raise

    def register_glue_catalog_table(self, table_name: str, catalog_path: str,
                                     schema: StructType) -> None:
        """
        Register table in AWS Glue Catalog.

        Glue Catalog from TRD:
        - Database: gen_ai_poc_databrickscoe
        - Tables: sdlc_wizard_customer, sdlc_wizard_order

        Args:
            table_name: Name of the table
            catalog_path: S3 path for table data
            schema: Table schema
        """
        print(f"[INFO] Registering table in Glue Catalog: {self.database}.{table_name}")

        try:
            # Create empty DataFrame with schema for catalog registration
            empty_df = self.spark.createDataFrame([], schema)

            # Write to catalog location (creates table if not exists)
            empty_df.write \
                .format('parquet') \
                .mode('append') \
                .option('path', catalog_path) \
                .saveAsTable(f"{self.database}.{table_name}")

            print(f"[INFO] Table registered successfully: {self.database}.{table_name}")

        except Exception as e:
            print(f"[WARN] Table may already exist or registration failed: {str(e)}")

    def aggregate_customer_spend(self, customer_df: DataFrame,
                                  order_df: DataFrame) -> DataFrame:
        """
        Aggregate customer spend data.
        Implements FR-AGGREGATE-001.

        Aggregation from TRD:
        - Join customer and order data
        - Calculate total spend per customer
        - Calculate order count per customer

        Args:
            customer_df: Cleaned customer DataFrame
            order_df: Cleaned order DataFrame

        Returns:
            DataFrame: Aggregated customer spend data
        """
        print(f"[INFO] Aggregating customer spend data")

        # Join customer and order data
        joined_df = customer_df.join(
            order_df,
            customer_df.CustId == order_df.CustId,
            'inner'
        )

        # Aggregate spend by customer
        aggregate_df = joined_df.groupBy(
            customer_df.CustId,
            customer_df.Name,
            customer_df.EmailId,
            customer_df.Region
        ).agg(
            _sum(order_df.Amount).alias('TotalSpend'),
            count(order_df.OrderId).alias('OrderCount'),
            _max(order_df.OrderDate).alias('LastOrderDate')
        )

        record_count = aggregate_df.count()
        print(f"[INFO] Aggregated data for {record_count} customers")

        return aggregate_df

    def write_aggregate_to_s3(self, aggregate_df: DataFrame) -> None:
        """
        Write aggregated customer spend data to S3.

        Output path from TRD: s3://adif-sdlc/analytics/customeraggregatespend/

        Args:
            aggregate_df: Aggregated customer spend DataFrame
        """
        print(f"[INFO] Writing aggregate data to: {self.customer_aggregate_path}")

        try:
            aggregate_df.write \
                .mode('overwrite') \
                .parquet(self.customer_aggregate_path)

            record_count = aggregate_df.count()
            print(f"[INFO] Successfully wrote {record_count} aggregate records")

        except Exception as e:
            print(f"[ERROR] Failed to write aggregate data: {str(e)}")
            raise

    def create_order_summary(self, order_df: DataFrame) -> DataFrame:
        """
        Create order summary data.

        Summary includes:
        - Total orders by region
        - Total amount by region

        Args:
            order_df: Cleaned order DataFrame

        Returns:
            DataFrame: Order summary data
        """
        print(f"[INFO] Creating order summary")

        # Join with customer to get region
        customer_df = self.spark.read.parquet(self.customer_catalog_path)

        joined_df = order_df.join(
            customer_df.select('CustId', 'Region'),
            'CustId',
            'inner'
        )

        summary_df = joined_df.groupBy('Region').agg(
            count('OrderId').alias('TotalOrders'),
            _sum('Amount').alias('TotalAmount')
        )

        return summary_df

    def write_order_summary_to_s3(self, summary_df: DataFrame) -> None:
        """
        Write order summary data to S3.

        Output path from TRD: s3://adif-sdlc/curated/sdlc_wizard/ordersummary/

        Args:
            summary_df: Order summary DataFrame
        """
        print(f"[INFO] Writing order summary to: {self.order_summary_path}")

        try:
            summary_df.write \
                .mode('overwrite') \
                .parquet(self.order_summary_path)

            record_count = summary_df.count()
            print(f"[INFO] Successfully wrote {record_count} summary records")

        except Exception as e:
            print(f"[ERROR] Failed to write order summary: {str(e)}")
            raise

    def process(self) -> Tuple[DataFrame, DataFrame]:
        """
        Main processing pipeline.

        Pipeline stages:
        1. Ingest customer and order data (FR-INGEST-001)
        2. Clean data (FR-CLEAN-001)
        3. Add SCD Type 2 columns (FR-SCD2-001)
        4. Write to Hudi format (FR-SCD2-001)
        5. Register in Glue Catalog
        6. Aggregate customer spend (FR-AGGREGATE-001)
        7. Create order summary

        Returns:
            Tuple[DataFrame, DataFrame]: Cleaned customer and order DataFrames
        """
        print("[INFO] Starting data processing pipeline")

        # Stage 1: Ingest data
        print("\n=== STAGE 1: DATA INGESTION ===")
        customer_df = self.ingest_customer_data()
        order_df = self.ingest_order_data()

        # Stage 2: Clean data
        print("\n=== STAGE 2: DATA CLEANING ===")
        customer_clean_df = self.clean_data(customer_df, "customer")
        order_clean_df = self.clean_data(order_df, "order")

        # Stage 3: Add SCD Type 2 columns
        print("\n=== STAGE 3: SCD TYPE 2 COLUMNS ===")
        customer_scd_df = self.add_scd_type2_columns(customer_clean_df)
        order_scd_df = self.add_scd_type2_columns(order_clean_df)

        # Stage 4: Write to Hudi
        print("\n=== STAGE 4: WRITE TO HUDI ===")
        self.write_to_hudi(
            customer_scd_df,
            self.hudi_config['customer'],
            self.customer_catalog_path,
            self.config['glue_catalog']['customer_table']
        )

        self.write_to_hudi(
            order_scd_df,
            self.hudi_config['order'],
            self.order_catalog_path,
            self.config['glue_catalog']['order_table']
        )

        # Stage 5: Register in Glue Catalog
        print("\n=== STAGE 5: GLUE CATALOG REGISTRATION ===")
        customer_schema_with_scd = self.get_customer_schema()
        order_schema_with_scd = self.get_order_schema()

        self.register_glue_catalog_table(
            self.config['glue_catalog']['customer_table'],
            self.customer_catalog_path,
            customer_schema_with_scd
        )

        self.register_glue_catalog_table(
            self.config['glue_catalog']['order_table'],
            self.order_catalog_path,
            order_schema_with_scd
        )

        # Stage 6: Aggregate customer spend
        print("\n=== STAGE 6: CUSTOMER SPEND AGGREGATION ===")
        aggregate_df = self.aggregate_customer_spend(customer_clean_df, order_clean_df)
        self.write_aggregate_to_s3(aggregate_df)

        # Stage 7: Create order summary
        print("\n=== STAGE 7: ORDER SUMMARY ===")
        summary_df = self.create_order_summary(order_clean_df)
        self.write_order_summary_to_s3(summary_df)

        print("\n[INFO] Data processing pipeline completed successfully")

        return customer_clean_df, order_clean_df


def load_config(config_path: str = 'config/glue_params.yaml') -> Dict:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        Dict: Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print(f"[INFO] Configuration loaded from: {config_path}")
        return config
    except Exception as e:
        print(f"[ERROR] Failed to load configuration: {str(e)}")
        raise


def main():
    """
    Main entry point for AWS Glue job.
    """
    print("[INFO] Starting AWS Glue PySpark Job")
    print(f"[INFO] Job start time: {datetime.now()}")

    # Get job parameters
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job_name = args['JOB_NAME']
        print(f"[INFO] Job name: {job_name}")
    except Exception as e:
        print(f"[WARN] Could not resolve job parameters: {str(e)}")
        job_name = "customer-order-scd2-processing"

    # Initialize Spark and Glue contexts
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Initialize Glue job
    job = Job(glue_context)
    job.init(job_name, args if 'args' in locals() else {})

    try:
        # Load configuration
        config = load_config()

        # Initialize processor
        processor = CustomerOrderProcessor(spark, glue_context, config)

        # Execute processing pipeline
        customer_clean_df, order_clean_df = processor.process()

        # Show sample results
        print("\n=== SAMPLE CUSTOMER DATA ===")
        customer_clean_df.show(5, truncate=False)

        print("\n=== SAMPLE ORDER DATA ===")
        order_clean_df.show(5, truncate=False)

        # Commit job
        job.commit()

        print(f"[INFO] Job completed successfully at: {datetime.now()}")

    except Exception as e:
        print(f"[ERROR] Job failed: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()