import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, current_timestamp, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_customer_schema():
    return StructType([
        StructField("CustId", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("EmailId", StringType(), False),
        StructField("Region", StringType(), False)
    ])


def get_order_schema():
    return StructType([
        StructField("OrderId", StringType(), False),
        StructField("ItemName", StringType(), False),
        StructField("PricePerUnit", DoubleType(), False),
        StructField("Qty", LongType(), False),
        StructField("Date", StringType(), False),
        StructField("CustId", StringType(), False)
    ])


def ingest_customer_data(spark, source_path):
    logger.info(f"Starting customer data ingestion from {source_path}")

    try:
        customer_schema = get_customer_schema()
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(customer_schema) \
            .load(source_path)

        count = df.count()
        logger.info(f"Customer data ingestion completed. Row count: {count}")
        return df
    except Exception as e:
        logger.error(f"Failed to access S3 path: {source_path}")
        raise Exception(f"Failed to access S3 path: {source_path}") from e


def ingest_order_data(spark, source_path):
    logger.info(f"Starting order data ingestion from {source_path}")

    try:
        order_schema = get_order_schema()
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(order_schema) \
            .load(source_path)

        count = df.count()
        logger.info(f"Order data ingestion completed. Row count: {count}")
        return df
    except Exception as e:
        logger.error(f"Failed to access S3 path: {source_path}")
        raise Exception(f"Failed to access S3 path: {source_path}") from e


def clean_data(df, data_type):
    logger.info(f"Starting data cleaning for {data_type}")

    initial_count = df.count()

    for column in df.columns:
        df = df.filter(
            (col(column).isNotNull()) &
            (col(column) != "Null") &
            (col(column) != "null")
        )

    df = df.dropDuplicates()

    final_count = df.count()
    logger.info(f"Data cleaning completed for {data_type}. Rows before: {initial_count}, after: {final_count}")

    return df


def write_to_catalog(df, output_path, database_name, table_name, spark):
    logger.info(f"Writing {table_name} to catalog at {output_path}")

    df.write.mode("overwrite") \
        .format("parquet") \
        .save(output_path)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")

    spark.sql(f"""
        CREATE EXTERNAL TABLE {database_name}.{table_name}
        STORED AS PARQUET
        LOCATION '{output_path}'
    """)

    logger.info(f"Successfully registered {table_name} in catalog")


def load_previous_snapshot(spark, snapshot_path):
    try:
        logger.info(f"Loading previous customer snapshot from {snapshot_path}")
        df = spark.read.format("parquet").load(snapshot_path)
        logger.info(f"Previous snapshot loaded. Row count: {df.count()}")
        return df
    except Exception as e:
        logger.info(f"No previous snapshot found at {snapshot_path}. This is the first run.")
        return None


def detect_changed_customers(current_df, previous_df):
    if previous_df is None:
        logger.info("First run detected. All customers are considered new.")
        return current_df

    logger.info("Detecting changed customers")

    changed_customers = current_df.join(
        previous_df,
        on="CustId",
        how="left_anti"
    ).union(
        current_df.alias("curr").join(
            previous_df.alias("prev"),
            on="CustId",
            how="inner"
        ).filter(
            (col("curr.Name") != col("prev.Name")) |
            (col("curr.EmailId") != col("prev.EmailId")) |
            (col("curr.Region") != col("prev.Region"))
        ).select("curr.*")
    ).distinct()

    changed_count = changed_customers.count()
    logger.info(f"Changed customers detected: {changed_count}")

    return changed_customers


def save_customer_snapshot(df, snapshot_path):
    logger.info(f"Saving customer snapshot to {snapshot_path}")
    df.write.mode("overwrite").format("parquet").save(snapshot_path)
    logger.info("Customer snapshot saved successfully")


def join_customer_order(customer_df, order_df):
    logger.info("Joining customer and order data")

    joined_df = order_df.join(customer_df, on="CustId", how="inner")

    joined_df = joined_df.withColumn(
        "TotalAmount",
        col("PricePerUnit") * col("Qty")
    )

    logger.info(f"Join completed. Row count: {joined_df.count()}")
    return joined_df


def write_hudi_scd2(df, output_path, table_name, database_name):
    logger.info(f"Writing SCD Type 2 data to Hudi table at {output_path}")

    current_ts = datetime.now()

    df_with_scd = df.withColumn("IsActive", lit(True)) \
        .withColumn("StartDate", lit(current_ts).cast(TimestampType())) \
        .withColumn("EndDate", lit(None).cast(TimestampType())) \
        .withColumn("OpTs", lit(current_ts).cast(TimestampType()))

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'CustId,OrderId',
        'hoodie.datasource.write.precombine.field': 'OpTs',
        'hoodie.datasource.write.partitionpath.field': '',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2,
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': database_name,
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
        'hoodie.datasource.hive_sync.use_jdbc': 'false',
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.table.type': 'COPY_ON_WRITE'
    }

    df_with_scd.write.format('hudi') \
        .options(**hudi_options) \
        .mode('append') \
        .save(output_path)

    logger.info(f"Successfully wrote {df_with_scd.count()} records to Hudi table")


def process_incremental_ordersummary(changed_customers_df, order_df, customer_df, output_path, table_name, database_name):
    logger.info("Processing incremental order summary")

    if changed_customers_df.count() == 0:
        logger.info("No changed customers. Skipping incremental processing.")
        return

    affected_orders = order_df.join(
        changed_customers_df.select("CustId"),
        on="CustId",
        how="inner"
    )

    affected_orders_with_customer = join_customer_order(customer_df, affected_orders)

    write_hudi_scd2(affected_orders_with_customer, output_path, table_name, database_name)

    logger.info("Incremental order summary processing completed")


def calculate_aggregate_spend(spark, ordersummary_path, output_path, database_name, table_name):
    logger.info("Calculating customer aggregate spend")

    try:
        ordersummary_df = spark.read.format("hudi").load(ordersummary_path)

        active_orders = ordersummary_df.filter(col("IsActive") == True)

        aggregate_df = active_orders.groupBy("CustId", "Name", "EmailId", "Region") \
            .agg(_sum("TotalAmount").alias("TotalSpend"))

        aggregate_df.write.mode("overwrite") \
            .format("parquet") \
            .save(output_path)

        spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")

        spark.sql(f"""
            CREATE EXTERNAL TABLE {database_name}.{table_name}
            STORED AS PARQUET
            LOCATION '{output_path}'
        """)

        logger.info(f"Aggregate spend calculation completed. Row count: {aggregate_df.count()}")
    except Exception as e:
        logger.warning(f"Could not calculate aggregate spend. Ordersummary table may not exist yet: {e}")


def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'database_name',
        'customer_source_path',
        'order_source_path',
        'customer_output_path',
        'order_output_path',
        'ordersummary_output_path',
        'customer_snapshot_path',
        'aggregate_output_path',
        'customer_table_name',
        'order_table_name',
        'ordersummary_table_name',
        'aggregate_table_name'
    ])

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    logger.info("Starting AWS Glue job execution")

    customer_source_path = args['customer_source_path']
    order_source_path = args['order_source_path']
    customer_output_path = args['customer_output_path']
    order_output_path = args['order_output_path']
    ordersummary_output_path = args['ordersummary_output_path']
    customer_snapshot_path = args['customer_snapshot_path']
    aggregate_output_path = args['aggregate_output_path']
    database_name = args['database_name']
    customer_table_name = args['customer_table_name']
    order_table_name = args['order_table_name']
    ordersummary_table_name = args['ordersummary_table_name']
    aggregate_table_name = args['aggregate_table_name']

    customer_df = ingest_customer_data(spark, customer_source_path)
    order_df = ingest_order_data(spark, order_source_path)

    cleaned_customer_df = clean_data(customer_df, "customer")
    cleaned_order_df = clean_data(order_df, "order")

    write_to_catalog(cleaned_customer_df, customer_output_path, database_name, customer_table_name, spark)
    write_to_catalog(cleaned_order_df, order_output_path, database_name, order_table_name, spark)

    previous_snapshot = load_previous_snapshot(spark, customer_snapshot_path)
    changed_customers = detect_changed_customers(cleaned_customer_df, previous_snapshot)

    if previous_snapshot is None:
        logger.info("First run: Processing all orders")
        joined_df = join_customer_order(cleaned_customer_df, cleaned_order_df)
        write_hudi_scd2(joined_df, ordersummary_output_path, ordersummary_table_name, database_name)
    else:
        process_incremental_ordersummary(
            changed_customers,
            cleaned_order_df,
            cleaned_customer_df,
            ordersummary_output_path,
            ordersummary_table_name,
            database_name
        )

    save_customer_snapshot(cleaned_customer_df, customer_snapshot_path)

    calculate_aggregate_spend(
        spark,
        ordersummary_output_path,
        aggregate_output_path,
        database_name,
        aggregate_table_name
    )

    logger.info("AWS Glue job execution completed successfully")

    job.commit()


if __name__ == "__main__":
    main()