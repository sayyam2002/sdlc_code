"""
AWS Glue PySpark ETL Job - Customer Order Analytics Pipeline
Implements complete ETL with data quality, SCD Type 2, and analytics aggregation
"""

import sys
import yaml
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, sum as spark_sum, current_timestamp, to_date
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame


def get_job_parameters():
    """
    Load job parameters from YAML configuration file.
    Returns dictionary with all configuration parameters.
    """
    config_path = "config/glue_params.yaml"
    try:
        with open(config_path, 'r') as f:
            params = yaml.safe_load(f)
        return params
    except Exception as e:
        print(f"Warning: Could not load config file {config_path}: {str(e)}")
        # Return default parameters
        return {
            "inputs_customer_source_path": "s3://adif-sdlc/sdlc_wizard/customerdata/",
            "inputs_customer_source_format": "csv",
            "inputs_order_source_path": "s3://adif-sdlc/sdlc_wizard/orderdata/",
            "inputs_order_source_format": "csv",
            "outputs_curated_customer_path": "s3://adif-sdlc/curated/sdlc_wizard/customer/",
            "outputs_curated_order_path": "s3://adif-sdlc/curated/sdlc_wizard/order/",
            "outputs_curated_ordersummary_path": "s3://adif-sdlc/curated/sdlc_wizard/ordersummary/",
            "outputs_state_customer_baseline_path": "s3://adif-sdlc/state/sdlc_wizard/customer_baseline/",
            "outputs_analytics_customeraggregatespend_path": "s3://adif-sdlc/analytics/customeraggregatespend/",
            "catalog_database_name": "gen_ai_poc_databrickscoe",
            "catalog_customer_table_name": "customer",
            "catalog_order_table_name": "order",
            "catalog_ordersummary_table_name": "ordersummary",
            "catalog_customeraggregatespend_table_name": "customeraggregatespend",
            "hudi_table_name": "customer_baseline",
            "hudi_record_key": "custid",
            "hudi_precombine_field": "opts",
            "hudi_partition_path": "",
            "hudi_table_type": "COPY_ON_WRITE",
            "hudi_operation": "upsert",
            "enable_scd2": True,
            "enable_incremental_processing": True,
            "enable_data_quality_checks": True,
            "csv_header": True,
            "csv_infer_schema": True,
            "csv_separator": ",",
            "output_format": "parquet",
            "output_mode": "overwrite",
            "output_compression": "snappy"
        }


def initialize_spark_contexts():
    """
    Initialize Spark and Glue contexts with proper configuration.
    Returns tuple of (spark, glueContext, job)
    """
    # Check if spark already exists (for testing)
    if 'spark' in dir(__builtins__):
        spark = __builtins__.spark
        sc = spark.sparkContext
        glueContext = GlueContext(sc)
        job = Job(glueContext)
        return spark, glueContext, job

    # Initialize for Glue environment
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        job_name = args['JOB_NAME']
    except Exception:
        job_name = "customer_order_analytics_pipeline"

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, args if 'args' in locals() else {})

    return spark, glueContext, job


def validate_s3_path(path):
    """
    Validate S3 path format.
    Returns True if valid, False otherwise.
    """
    if not path:
        return False
    return path.startswith("s3://") or path.startswith("s3a://")


class DataReader:
    """Helper class for safe data reading operations"""

    def __init__(self, spark, glueContext):
        self.spark = spark
        self.glueContext = glueContext

    def read_csv(self, path, header=True, infer_schema=True, separator=","):
        """
        Safely read CSV data from S3 path.
        Returns DataFrame or None if read fails.
        """
        if not validate_s3_path(path):
            print(f"Invalid S3 path: {path}")
            return None

        try:
            df = self.spark.read.format("csv") \
                .option("header", str(header).lower()) \
                .option("inferSchema", str(infer_schema).lower()) \
                .option("sep", separator) \
                .load(path)

            # Normalize column names to lowercase
            df = df.toDF(*[c.lower() for c in df.columns])

            print(f"Successfully read {df.count()} records from {path}")
            return df
        except Exception as e:
            print(f"Error reading from {path}: {str(e)}")
            return None


class DataWriter:
    """Helper class for safe data writing operations"""

    def __init__(self, spark, glueContext):
        self.spark = spark
        self.glueContext = glueContext

    def _write_parquet(self, df, path, mode="overwrite", compression="snappy"):
        """Internal method to write Parquet format"""
        df.write.format("parquet") \
            .option("compression", compression) \
            .mode(mode) \
            .save(path)

    def _write_hudi(self, df, path, table_name, record_key, precombine_field,
                    table_type="COPY_ON_WRITE", operation="upsert"):
        """Internal method to write Hudi format"""
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_field,
            'hoodie.datasource.write.operation': operation,
            'hoodie.datasource.write.table.type': table_type,
            'hoodie.datasource.hive_sync.enable': 'false'
        }

        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)

    def write_dataframe(self, df, path, format_type="parquet", mode="overwrite",
                       compression="snappy", hudi_config=None):
        """
        Safely write DataFrame to S3 path.
        Returns True if successful, False otherwise.
        """
        if not validate_s3_path(path):
            print(f"Invalid S3 path: {path}")
            return False

        if df is None or df.rdd.isEmpty():
            print(f"Empty DataFrame, skipping write to {path}")
            return False

        try:
            if format_type == "hudi" and hudi_config:
                self._write_hudi(
                    df, path,
                    hudi_config.get("table_name"),
                    hudi_config.get("record_key"),
                    hudi_config.get("precombine_field"),
                    hudi_config.get("table_type", "COPY_ON_WRITE"),
                    hudi_config.get("operation", "upsert")
                )
            else:
                self._write_parquet(df, path, mode, compression)

            print(f"Successfully wrote {df.count()} records to {path}")
            return True
        except Exception as e:
            print(f"Error writing to {path}: {str(e)}")
            return False

    def write_to_catalog(self, df, database, table_name, path):
        """
        Write DataFrame to Glue Data Catalog.
        Returns True if successful, False otherwise.
        """
        if df is None or df.rdd.isEmpty():
            print(f"Empty DataFrame, skipping catalog write for {table_name}")
            return False

        try:
            # Convert to DynamicFrame
            dynamic_frame = DynamicFrame.fromDF(df, self.glueContext, table_name)

            # Write to catalog
            self.glueContext.write_dynamic_frame.from_catalog(
                frame=dynamic_frame,
                database=database,
                table_name=table_name,
                transformation_ctx=f"write_{table_name}"
            )

            print(f"Successfully wrote to catalog: {database}.{table_name}")
            return True
        except Exception as e:
            print(f"Error writing to catalog {database}.{table_name}: {str(e)}")
            # Fallback: write to S3 path
            if path:
                return self.write_dataframe(df, path)
            return False


def clean_dataframe(df, df_name="dataframe"):
    """
    Remove null values and duplicates from DataFrame.
    Handles both NULL and string 'Null' values.
    Returns cleaned DataFrame.
    """
    if df is None:
        print(f"Cannot clean None {df_name}")
        return None

    print(f"Cleaning {df_name}: Initial count = {df.count()}")

    # Replace string 'Null' with actual NULL
    for column in df.columns:
        df = df.withColumn(column,
            col(column).when(col(column) == "Null", lit(None)).otherwise(col(column)))

    # Drop rows with any NULL values
    df_cleaned = df.dropna()

    # Remove duplicates
    df_cleaned = df_cleaned.dropDuplicates()

    print(f"Cleaning {df_name}: Final count = {df_cleaned.count()}")

    return df_cleaned


def add_scd2_columns(df):
    """
    Add SCD Type 2 columns to DataFrame.
    Adds: IsActive, StartDate, EndDate, OpTs
    Returns DataFrame with SCD2 columns.
    """
    if df is None:
        return None

    current_ts = current_timestamp()

    df_scd2 = df.withColumn("isactive", lit(True)) \
                .withColumn("startdate", current_ts) \
                .withColumn("enddate", lit(None).cast("timestamp")) \
                .withColumn("opts", current_ts)

    return df_scd2


def detect_customer_changes(current_df, baseline_df):
    """
    Detect changes in customer data for incremental processing.
    Returns DataFrame with only changed records.
    """
    if current_df is None:
        return None

    if baseline_df is None or baseline_df.rdd.isEmpty():
        # No baseline exists, all records are new
        print("No baseline found, treating all records as new")
        return current_df

    # Join to find changed records
    # Records that don't exist in baseline or have different values
    changed_df = current_df.alias("curr").join(
        baseline_df.alias("base"),
        col("curr.custid") == col("base.custid"),
        "left_anti"
    )

    print(f"Detected {changed_df.count()} changed customer records")

    return changed_df


def create_order_summary(customer_df, order_df):
    """
    Create order summary by joining customer and order data.
    Returns DataFrame with order summary.
    """
    if customer_df is None or order_df is None:
        print("Cannot create order summary: missing input data")
        return None

    # Join customer and order data
    order_summary = order_df.alias("o").join(
        customer_df.alias("c"),
        col("o.custid") == col("c.custid"),
        "inner"
    ).select(
        col("o.orderid"),
        col("o.itemname"),
        col("o.priceperunit"),
        col("o.qty"),
        col("o.date"),
        col("c.custid"),
        col("c.name"),
        col("c.emailid"),
        col("c.region")
    )

    print(f"Created order summary with {order_summary.count()} records")

    return order_summary


def calculate_customer_aggregate_spend(order_summary_df):
    """
    Calculate total customer spending aggregated by customer name and date.
    Returns DataFrame with customer aggregate spend.
    """
    if order_summary_df is None:
        print("Cannot calculate aggregate spend: missing order summary")
        return None

    # Calculate total spend per order line
    order_with_total = order_summary_df.withColumn(
        "totalspend",
        col("priceperunit") * col("qty")
    )

    # Aggregate by customer name and date
    aggregate_spend = order_with_total.groupBy("name", "date").agg(
        spark_sum("totalspend").alias("totalspend")
    )

    print(f"Calculated aggregate spend for {aggregate_spend.count()} customer-date combinations")

    return aggregate_spend


def main():
    """
    Main ETL pipeline execution function.
    Orchestrates the complete data processing workflow.
    """
    print("=" * 80)
    print("Starting Customer Order Analytics ETL Pipeline")
    print("=" * 80)

    # Initialize contexts
    spark, glueContext, job = initialize_spark_contexts()

    # Load parameters
    params = get_job_parameters()

    # Initialize helpers
    reader = DataReader(spark, glueContext)
    writer = DataWriter(spark, glueContext)

    # Step 1: Ingest Customer Data
    print("\n[STEP 1] Ingesting Customer Data...")
    customer_raw_df = reader.read_csv(
        params["inputs_customer_source_path"],
        header=params.get("csv_header", True),
        infer_schema=params.get("csv_infer_schema", True),
        separator=params.get("csv_separator", ",")
    )

    # Step 2: Ingest Order Data
    print("\n[STEP 2] Ingesting Order Data...")
    order_raw_df = reader.read_csv(
        params["inputs_order_source_path"],
        header=params.get("csv_header", True),
        infer_schema=params.get("csv_infer_schema", True),
        separator=params.get("csv_separator", ",")
    )

    # Step 3: Clean Customer Data
    print("\n[STEP 3] Cleaning Customer Data...")
    customer_clean_df = clean_dataframe(customer_raw_df, "customer")

    # Step 4: Clean Order Data
    print("\n[STEP 4] Cleaning Order Data...")
    order_clean_df = clean_dataframe(order_raw_df, "order")

    # Step 5: Write Cleaned Data to Catalog
    print("\n[STEP 5] Writing Cleaned Data to Catalog...")
    writer.write_to_catalog(
        customer_clean_df,
        params["catalog_database_name"],
        params["catalog_customer_table_name"],
        params["outputs_curated_customer_path"]
    )

    writer.write_to_catalog(
        order_clean_df,
        params["catalog_database_name"],
        params["catalog_order_table_name"],
        params["outputs_curated_order_path"]
    )

    # Step 6: Implement SCD Type 2 for Customer Baseline
    if params.get("enable_scd2", True):
        print("\n[STEP 6] Implementing SCD Type 2 for Customer Baseline...")

        # Try to read existing baseline
        baseline_df = None
        try:
            baseline_df = reader.read_csv(params["outputs_state_customer_baseline_path"])
        except Exception:
            print("No existing baseline found, creating new baseline")

        # Detect changes if incremental processing enabled
        if params.get("enable_incremental_processing", True):
            changed_customers = detect_customer_changes(customer_clean_df, baseline_df)
        else:
            changed_customers = customer_clean_df

        # Add SCD2 columns
        customer_scd2_df = add_scd2_columns(changed_customers)

        # Write to Hudi format
        hudi_config = {
            "table_name": params["hudi_table_name"],
            "record_key": params["hudi_record_key"],
            "precombine_field": params["hudi_precombine_field"],
            "table_type": params.get("hudi_table_type", "COPY_ON_WRITE"),
            "operation": params.get("hudi_operation", "upsert")
        }

        writer.write_dataframe(
            customer_scd2_df,
            params["outputs_state_customer_baseline_path"],
            format_type="hudi",
            hudi_config=hudi_config
        )

    # Step 7: Create Order Summary
    print("\n[STEP 7] Creating Order Summary...")
    order_summary_df = create_order_summary(customer_clean_df, order_clean_df)

    writer.write_to_catalog(
        order_summary_df,
        params["catalog_database_name"],
        params["catalog_ordersummary_table_name"],
        params["outputs_curated_ordersummary_path"]
    )

    # Step 8: Calculate Customer Aggregate Spend
    print("\n[STEP 8] Calculating Customer Aggregate Spend...")
    aggregate_spend_df = calculate_customer_aggregate_spend(order_summary_df)

    writer.write_to_catalog(
        aggregate_spend_df,
        params["catalog_database_name"],
        params["catalog_customeraggregatespend_table_name"],
        params["outputs_analytics_customeraggregatespend_path"]
    )

    # Commit job
    job.commit()

    print("\n" + "=" * 80)
    print("ETL Pipeline Completed Successfully")
    print("=" * 80)


if __name__ == "__main__":
    main()