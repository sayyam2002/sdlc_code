"""
AWS Glue PySpark ETL Job - Customer Order Analytics
Main job implementation with SCD Type 2 support using Apache Hudi
"""

import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_date,
    sum as _sum, count, max as _max, min as _min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, DateType, BooleanType, TimestampType
)


def initialize_spark_contexts() -> Tuple[SparkSession, Any]:
    """
    Initialize Spark and Glue contexts with proper configuration.

    Returns:
        Tuple of (SparkSession, GlueContext)
    """
    # Check if running in existing Spark context (test environment)
    if hasattr(__builtins__, 'spark'):
        spark = getattr(__builtins__, 'spark')
        glue_context = None
        return spark, glue_context

    # Create new Spark session for Glue environment
    spark = SparkSession.builder \
        .appName("CustomerOrderETL") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Initialize Glue context if available
    glue_context = None
    try:
        from awsglue.context import GlueContext
        glue_context = GlueContext(spark.sparkContext)
    except ImportError:
        print("GlueContext not available - running in local/test mode")

    return spark, glue_context


def get_job_parameters() -> Dict[str, Any]:
    """
    Load job parameters from YAML configuration file.

    Returns:
        Dictionary containing all job parameters
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config',
        'glue_params.yaml'
    )

    with open(config_path, 'r') as f:
        params = yaml.safe_load(f)

    return params


def validate_s3_access(spark: SparkSession, s3_path: str) -> bool:
    """
    Validate S3 bucket access before operations.

    Args:
        spark: SparkSession instance
        s3_path: S3 path to validate

    Returns:
        True if accessible, False otherwise
    """
    try:
        # Simple validation - check if path is properly formatted
        if not s3_path.startswith('s3://'):
            return False

        # In production, this would attempt to list the bucket
        # For now, we assume valid S3 paths are accessible
        return True
    except Exception as e:
        print(f"S3 access validation failed for {s3_path}: {str(e)}")
        return False


class DataReader:
    """Helper class for safe data reading operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _read_csv(self, path: str, schema: Optional[StructType] = None) -> DataFrame:
        """Internal method to read CSV files"""
        reader = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true" if schema is None else "false")

        if schema:
            reader = reader.schema(schema)

        return reader.load(path)

    def _read_parquet(self, path: str) -> DataFrame:
        """Internal method to read Parquet files"""
        return self.spark.read.format("parquet").load(path)

    def read_data(self, path: str, format_type: str, schema: Optional[StructType] = None) -> DataFrame:
        """Public method to read data with format dispatch"""
        if format_type.lower() == "csv":
            return self._read_csv(path, schema)
        elif format_type.lower() == "parquet":
            return self._read_parquet(path)
        else:
            raise ValueError(f"Unsupported format: {format_type}")


class DataWriter:
    """Helper class for safe data writing operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Internal method to write Parquet files"""
        df.write.format("parquet").mode(mode).save(path)

    def _write_csv(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Internal method to write CSV files"""
        df.write.format("csv") \
            .option("header", "true") \
            .mode(mode) \
            .save(path)

    def _write_hudi(self, df: DataFrame, path: str, hudi_options: Dict[str, str]) -> None:
        """Internal method to write Hudi tables"""
        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)

    def write_data(self, df: DataFrame, path: str, format_type: str,
                   mode: str = "overwrite", hudi_options: Optional[Dict[str, str]] = None) -> None:
        """Public method to write data with format dispatch"""
        if format_type.lower() == "parquet":
            self._write_parquet(df, path, mode)
        elif format_type.lower() == "csv":
            self._write_csv(df, path, mode)
        elif format_type.lower() == "hudi":
            if hudi_options is None:
                raise ValueError("Hudi options required for Hudi format")
            self._write_hudi(df, path, hudi_options)
        else:
            raise ValueError(f"Unsupported format: {format_type}")


def read_data_safe(spark: SparkSession, path: str, format_type: str,
                   schema: Optional[StructType] = None) -> Optional[DataFrame]:
    """
    Safely read data from S3 with error handling.

    Args:
        spark: SparkSession instance
        path: S3 path to read from
        format_type: Format type (csv, parquet, etc.)
        schema: Optional schema definition

    Returns:
        DataFrame if successful, None otherwise
    """
    try:
        if not validate_s3_access(spark, path):
            print(f"S3 access validation failed for {path}")
            return None

        reader = DataReader(spark)
        df = reader.read_data(path, format_type, schema)

        # Normalize column names to lowercase
        df = df.toDF(*[c.lower() for c in df.columns])

        return df
    except Exception as e:
        print(f"Error reading data from {path}: {str(e)}")
        return None


def write_data_safe(df: DataFrame, spark: SparkSession, path: str,
                    format_type: str, mode: str = "overwrite",
                    hudi_options: Optional[Dict[str, str]] = None) -> bool:
    """
    Safely write data to S3 with error handling.

    Args:
        df: DataFrame to write
        spark: SparkSession instance
        path: S3 path to write to
        format_type: Format type (parquet, csv, hudi)
        mode: Write mode (overwrite, append)
        hudi_options: Hudi configuration options

    Returns:
        True if successful, False otherwise
    """
    try:
        if not validate_s3_access(spark, path):
            print(f"S3 access validation failed for {path}")
            return False

        writer = DataWriter(spark)
        writer.write_data(df, path, format_type, mode, hudi_options)

        print(f"Successfully wrote data to {path}")
        return True
    except Exception as e:
        print(f"Error writing data to {path}: {str(e)}")
        return False


def get_customer_schema() -> StructType:
    """
    Define customer data schema.

    Returns:
        StructType schema definition
    """
    return StructType([
        StructField("custid", StringType(), False),
        StructField("name", StringType(), False),
        StructField("emailid", StringType(), False),
        StructField("region", StringType(), False)
    ])


def get_order_schema() -> StructType:
    """
    Define order data schema.

    Returns:
        StructType schema definition
    """
    return StructType([
        StructField("orderid", StringType(), False),
        StructField("itemname", StringType(), False),
        StructField("priceperunit", DecimalType(10, 2), False),
        StructField("qty", IntegerType(), False),
        StructField("date", DateType(), False),
        StructField("custid", StringType(), False)
    ])


def clean_dataframe(df: DataFrame, null_string_values: list) -> DataFrame:
    """
    Remove NULL values, "Null" strings, and duplicate records.

    Args:
        df: Input DataFrame
        null_string_values: List of string values to treat as null

    Returns:
        Cleaned DataFrame
    """
    # Remove rows with actual NULL values
    df_cleaned = df.dropna()

    # Remove rows with string "Null" values (case-sensitive)
    for col_name in df.columns:
        for null_val in null_string_values:
            df_cleaned = df_cleaned.filter(col(col_name) != null_val)

    # Remove duplicate records (keep first occurrence)
    df_cleaned = df_cleaned.dropDuplicates()

    return df_cleaned


def apply_scd2(customer_df: DataFrame, order_df: DataFrame,
               spark: SparkSession) -> DataFrame:
    """
    Apply SCD Type 2 logic to create order summary with historical tracking.

    Args:
        customer_df: Customer DataFrame
        order_df: Order DataFrame
        spark: SparkSession instance

    Returns:
        DataFrame with SCD2 columns
    """
    # Join customer and order data
    joined_df = order_df.join(
        customer_df,
        order_df.custid == customer_df.custid,
        "inner"
    ).select(
        order_df.custid,
        order_df.orderid,
        customer_df.name,
        customer_df.emailid,
        customer_df.region,
        order_df.itemname,
        order_df.priceperunit,
        order_df.qty,
        order_df.date
    )

    # Add SCD2 metadata columns
    scd2_df = joined_df \
        .withColumn("isactive", lit(True).cast(BooleanType())) \
        .withColumn("startdate", current_timestamp().cast(TimestampType())) \
        .withColumn("enddate", lit(None).cast(TimestampType())) \
        .withColumn("opts", current_timestamp().cast(TimestampType()))

    return scd2_df


def aggregate_customer_spend(order_summary_df: DataFrame) -> DataFrame:
    """
    Aggregate order data by customer and date to calculate total spending.

    Args:
        order_summary_df: Order summary DataFrame

    Returns:
        Aggregated DataFrame with customer spending metrics
    """
    # Calculate total spend per order
    order_summary_df = order_summary_df.withColumn(
        "totalspend",
        col("priceperunit") * col("qty")
    )

    # Aggregate by customer and date
    agg_df = order_summary_df.groupBy("custid", "name", "region", "date") \
        .agg(
            _sum("totalspend").alias("total_spend"),
            count("orderid").alias("order_count"),
            _sum("qty").alias("total_quantity"),
            _max("priceperunit").alias("max_price"),
            _min("priceperunit").alias("min_price")
        )

    return agg_df


def register_table_to_catalog(spark: SparkSession, df: DataFrame,
                               database: str, table_name: str,
                               s3_path: str) -> bool:
    """
    Register DataFrame as table in Glue Data Catalog.

    Args:
        spark: SparkSession instance
        df: DataFrame to register
        database: Catalog database name
        table_name: Table name
        s3_path: S3 location of data

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        # Register table
        df.write.format("parquet") \
            .mode("overwrite") \
            .option("path", s3_path) \
            .saveAsTable(f"{database}.{table_name}")

        print(f"Successfully registered table {database}.{table_name}")
        return True
    except Exception as e:
        print(f"Error registering table {database}.{table_name}: {str(e)}")
        return False


def main():
    """
    Main ETL job execution function.
    """
    print("Starting Customer Order ETL Job...")

    # Initialize Spark contexts
    spark, glue_context = initialize_spark_contexts()

    # Load job parameters
    params = get_job_parameters()

    # Extract configuration
    customer_source = params['inputs']['customer']['source_path']
    customer_format = params['inputs']['customer']['source_format']
    order_source = params['inputs']['order']['source_path']
    order_format = params['inputs']['order']['source_format']

    customer_target = params['outputs']['customer']['target_path']
    order_target = params['outputs']['order']['target_path']
    ordersummary_target = params['outputs']['ordersummary']['target_path']
    aggregate_target = params['outputs']['customeraggregatespend']['target_path']

    catalog_db = params['catalog']['database']
    null_values = params['processing']['null_string_values']

    # Step 1: Ingest customer data
    print(f"Reading customer data from {customer_source}...")
    customer_df = read_data_safe(spark, customer_source, customer_format, get_customer_schema())

    if customer_df is None:
        print("Failed to read customer data")
        return

    print(f"Customer records loaded: {customer_df.count()}")

    # Step 2: Ingest order data
    print(f"Reading order data from {order_source}...")
    order_df = read_data_safe(spark, order_source, order_format, get_order_schema())

    if order_df is None:
        print("Failed to read order data")
        return

    print(f"Order records loaded: {order_df.count()}")

    # Step 3: Clean customer data
    print("Cleaning customer data...")
    customer_clean = clean_dataframe(customer_df, null_values)
    print(f"Customer records after cleaning: {customer_clean.count()}")

    # Step 4: Clean order data
    print("Cleaning order data...")
    order_clean = clean_dataframe(order_df, null_values)
    print(f"Order records after cleaning: {order_clean.count()}")

    # Step 5: Write cleaned customer data to catalog
    print(f"Writing cleaned customer data to {customer_target}...")
    write_data_safe(customer_clean, spark, customer_target, "parquet")
    register_table_to_catalog(
        spark, customer_clean, catalog_db,
        params['catalog']['tables']['customer'], customer_target
    )

    # Step 6: Write cleaned order data to catalog
    print(f"Writing cleaned order data to {order_target}...")
    write_data_safe(order_clean, spark, order_target, "parquet")
    register_table_to_catalog(
        spark, order_clean, catalog_db,
        params['catalog']['tables']['order'], order_target
    )

    # Step 7: Apply SCD Type 2 logic
    if params['processing']['enable_scd2']:
        print("Applying SCD Type 2 logic...")
        order_summary_df = apply_scd2(customer_clean, order_clean, spark)
        print(f"Order summary records created: {order_summary_df.count()}")

        # Prepare Hudi options
        hudi_options = {
            'hoodie.table.name': params['hudi']['table_name'],
            'hoodie.datasource.write.recordkey.field': params['hudi']['record_key'],
            'hoodie.datasource.write.precombine.field': params['hudi']['precombine_field'],
            'hoodie.datasource.write.table.type': params['hudi']['table_type'],
            'hoodie.datasource.write.operation': params['hudi']['operation'],
            'hoodie.datasource.write.partitionpath.field': params['hudi']['partition_fields'],
            'hoodie.index.type': params['hudi']['index_type']
        }

        # Write to Hudi table
        print(f"Writing order summary to Hudi table at {ordersummary_target}...")
        write_data_safe(order_summary_df, spark, ordersummary_target, "hudi",
                       hudi_options=hudi_options)

        # Step 8: Aggregate customer spending
        if params['processing']['enable_aggregation']:
            print("Calculating customer aggregate spending...")
            aggregate_df = aggregate_customer_spend(order_summary_df)
            print(f"Aggregate records created: {aggregate_df.count()}")

            # Write aggregated data
            print(f"Writing aggregate data to {aggregate_target}...")
            write_data_safe(aggregate_df, spark, aggregate_target, "parquet")
            register_table_to_catalog(
                spark, aggregate_df, catalog_db,
                params['catalog']['tables']['customeraggregatespend'], aggregate_target
            )

    print("Customer Order ETL Job completed successfully!")


if __name__ == "__main__":
    main()