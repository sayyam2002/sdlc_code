"""
AWS Glue ETL Job - Use Case 1
Complete data processing pipeline for customer and order data with SCD Type 2 implementation
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType


class S3Reader:
    """Helper class for reading data from S3"""
    
    def __init__(self, spark, glue_context):
        self.spark = spark
        self.glue_context = glue_context
    
    def read_csv(self, path, header=True, infer_schema=True):
        """Read CSV files from S3 path"""
        return self.spark.read.format("csv") \
            .option("header", str(header).lower()) \
            .option("inferSchema", str(infer_schema).lower()) \
            .load(path)


class S3Writer:
    """Helper class for writing data to S3"""
    
    def __init__(self, spark, glue_context):
        self.spark = spark
        self.glue_context = glue_context
    
    def _write_parquet(self, df, path, mode="overwrite"):
        """Internal method to write Parquet format"""
        df.write.format("parquet") \
            .option("compression", "snappy") \
            .mode(mode) \
            .save(path)
    
    def _write_hudi(self, df, path, table_name, record_key, precombine_field, operation="upsert"):
        """Internal method to write Hudi format"""
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_field,
            'hoodie.datasource.write.operation': operation,
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.hive_sync.enable': 'false'
        }
        
        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)
    
    def write_dataframe(self, df, path, format_type="parquet", mode="overwrite", **kwargs):
        """Public method to write DataFrame with format dispatch"""
        if format_type == "parquet":
            self._write_parquet(df, path, mode)
        elif format_type == "hudi":
            self._write_hudi(
                df, path,
                kwargs.get('table_name'),
                kwargs.get('record_key'),
                kwargs.get('precombine_field'),
                kwargs.get('operation', 'upsert')
            )
        else:
            raise ValueError(f"Unsupported format: {format_type}")


class DataCleaner:
    """Data cleaning operations"""
    
    @staticmethod
    def remove_nulls(df, null_string="Null"):
        """Remove records with NULL values or string 'Null'"""
        for column in df.columns:
            df = df.filter(
                (col(column).isNotNull()) & 
                (col(column) != null_string)
            )
        return df
    
    @staticmethod
    def remove_duplicates(df):
        """Remove duplicate records"""
        return df.dropDuplicates()


class CatalogManager:
    """AWS Glue Catalog operations"""
    
    def __init__(self, glue_context, database):
        self.glue_context = glue_context
        self.database = database
    
    def register_table(self, df, table_name, s3_path):
        """Register DataFrame as Glue Catalog table"""
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, table_name)
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={"path": s3_path},
            format="parquet",
            transformation_ctx=f"write_{table_name}"
        )


class SCD2Processor:
    """SCD Type 2 processing logic"""
    
    @staticmethod
    def add_scd2_columns(df, is_active=True):
        """Add SCD Type 2 tracking columns"""
        return df \
            .withColumn("IsActive", lit(is_active)) \
            .withColumn("StartDate", current_timestamp()) \
            .withColumn("EndDate", lit(None).cast(TimestampType())) \
            .withColumn("OpTs", current_timestamp())
    
    @staticmethod
    def join_customer_order(customer_df, order_df):
        """Join customer and order data"""
        return order_df.join(customer_df, "CustId", "inner")


class AggregationProcessor:
    """Data aggregation operations"""
    
    @staticmethod
    def calculate_customer_spend(customer_df, order_df):
        """Calculate total spending per customer"""
        joined_df = order_df.join(customer_df, "CustId", "inner")
        
        agg_df = joined_df \
            .withColumn("Amount", col("PricePerUnit") * col("Qty")) \
            .groupBy("Name", "Date") \
            .agg(_sum("Amount").alias("TotalAmount"))
        
        return agg_df


class GlueETLJob:
    """Main ETL Job orchestrator"""
    
    def __init__(self):
        self.sc = None
        self.glue_context = None
        self.spark = None
        self.job = None
        self.s3_reader = None
        self.s3_writer = None
        self.catalog_manager = None
        
    def initialize(self, job_name="usecase1_etl_job"):
        """Initialize Glue context and Spark session"""
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(job_name, {})
        
        self.s3_reader = S3Reader(self.spark, self.glue_context)
        self.s3_writer = S3Writer(self.spark, self.glue_context)
        
        self.spark.sparkContext.setLogLevel("INFO")
        
    def ingest_data(self, customer_path, order_path):
        """Ingest customer and order data from S3"""
        print(f"Ingesting customer data from: {customer_path}")
        customer_df = self.s3_reader.read_csv(customer_path)
        print(f"Customer records ingested: {customer_df.count()}")
        
        print(f"Ingesting order data from: {order_path}")
        order_df = self.s3_reader.read_csv(order_path)
        print(f"Order records ingested: {order_df.count()}")
        
        return customer_df, order_df
    
    def clean_data(self, customer_df, order_df, null_string="Null"):
        """Clean customer and order data"""
        print("Cleaning customer data...")
        customer_clean = DataCleaner.remove_nulls(customer_df, null_string)
        customer_clean = DataCleaner.remove_duplicates(customer_clean)
        print(f"Customer records after cleaning: {customer_clean.count()}")
        
        print("Cleaning order data...")
        order_clean = DataCleaner.remove_nulls(order_df, null_string)
        order_clean = DataCleaner.remove_duplicates(order_clean)
        print(f"Order records after cleaning: {order_clean.count()}")
        
        return customer_clean, order_clean
    
    def register_catalog_tables(self, customer_df, order_df, database, 
                                customer_table, order_table,
                                customer_path, order_path):
        """Register cleaned data in Glue Catalog"""
        print(f"Registering catalog tables in database: {database}")
        
        self.catalog_manager = CatalogManager(self.glue_context, database)
        
        print(f"Writing customer data to: {customer_path}")
        self.s3_writer.write_dataframe(customer_df, customer_path, format_type="parquet")
        
        print(f"Writing order data to: {order_path}")
        self.s3_writer.write_dataframe(order_df, order_path, format_type="parquet")
        
        print("Catalog tables registered successfully")
    
    def process_scd2(self, customer_df, order_df, output_path, table_name, 
                     record_key, precombine_field):
        """Process SCD Type 2 for order summary"""
        print("Processing SCD Type 2 for order summary...")
        
        joined_df = SCD2Processor.join_customer_order(customer_df, order_df)
        print(f"Joined records: {joined_df.count()}")
        
        scd2_df = SCD2Processor.add_scd2_columns(joined_df, is_active=True)
        
        print(f"Writing Hudi table to: {output_path}")
        self.s3_writer.write_dataframe(
            scd2_df, output_path, 
            format_type="hudi",
            table_name=table_name,
            record_key=record_key,
            precombine_field=precombine_field,
            operation="upsert"
        )
        
        print("SCD Type 2 processing completed")
        return scd2_df
    
    def calculate_aggregates(self, customer_df, order_df, output_path):
        """Calculate customer aggregate spend"""
        print("Calculating customer aggregate spend...")
        
        agg_df = AggregationProcessor.calculate_customer_spend(customer_df, order_df)
        print(f"Aggregate records: {agg_df.count()}")
        
        print(f"Writing aggregate data to: {output_path}")
        self.s3_writer.write_dataframe(agg_df, output_path, format_type="parquet")
        
        print("Aggregation completed")
        return agg_df
    
    def run(self, config):
        """Execute complete ETL pipeline"""
        try:
            print("=" * 80)
            print("Starting AWS Glue ETL Job - Use Case 1")
            print("=" * 80)
            
            # Step 1: Ingest data
            customer_df, order_df = self.ingest_data(
                config['source']['customer_path'],
                config['source']['order_path']
            )
            
            # Step 2: Clean data
            customer_clean, order_clean = self.clean_data(
                customer_df, order_df,
                config['processing']['null_string_value']
            )
            
            # Step 3: Register catalog tables
            self.register_catalog_tables(
                customer_clean, order_clean,
                config['catalog']['database'],
                config['catalog']['customer_table'],
                config['catalog']['order_table'],
                config['target']['customer_catalog_path'],
                config['target']['order_catalog_path']
            )
            
            # Step 4: Process SCD Type 2
            ordersummary_df = self.process_scd2(
                customer_clean, order_clean,
                config['target']['ordersummary_path'],
                config['hudi']['table_name'],
                config['hudi']['record_key'],
                config['hudi']['precombine_field']
            )
            
            # Step 5: Calculate aggregates
            agg_df = self.calculate_aggregates(
                customer_clean, order_clean,
                config['target']['customeraggregatespend_path']
            )
            
            print("=" * 80)
            print("ETL Job completed successfully")
            print("=" * 80)
            
        except Exception as e:
            print(f"ERROR: ETL Job failed with exception: {str(e)}")
            raise
        finally:
            if self.job:
                self.job.commit()


def main():
    """Main entry point for Glue job"""
    
    # Default configuration
    config = {
        'source': {
            'customer_path': 's3://adif-sdlc/sdlc_wizard/customerdata/',
            'order_path': 's3://adif-sdlc/sdlc_wizard/orderdata/'
        },
        'target': {
            'customer_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/customer/',
            'order_catalog_path': 's3://adif-sdlc/catalog/sdlc_wizard/order/',
            'ordersummary_path': 's3://adif-sdlc/curated/sdlc_wizard/ordersummary/',
            'customeraggregatespend_path': 's3://adif-sdlc/analytics/customeraggregatespend/'
        },
        'catalog': {
            'database': 'gen_ai_poc_databrickscoe',
            'customer_table': 'sdlc_wizard_customer',
            'order_table': 'sdlc_wizard_order',
            'ordersummary_table': 'ordersummary',
            'customeraggregatespend_table': 'customeraggregatespend'
        },
        'processing': {
            'null_string_value': 'Null'
        },
        'hudi': {
            'table_name': 'ordersummary',
            'record_key': 'CustId,OrderId',
            'precombine_field': 'OpTs'
        }
    }
    
    # Initialize and run job
    job = GlueETLJob()
    job.initialize()
    job.run(config)


if __name__ == "__main__":
    main()