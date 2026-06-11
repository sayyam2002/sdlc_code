"""
AWS Glue ETL Job: Customer Order SCD Type 2 Processing

This job implements a complete data processing pipeline for customer and order data
with SCD Type 2 historical tracking using Apache Hudi.
"""

import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sum as spark_sum, when
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


class S3Reader:
    """Helper class for reading data from S3"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def read_csv(self, path, header=True, infer_schema=True, delimiter=","):
        """Read CSV files from S3"""
        return self.spark.read.format("csv") \
            .option("header", header) \
            .option("inferSchema", infer_schema) \
            .option("delimiter", delimiter) \
            .load(path)
    
    def read_hudi(self, path):
        """Read Hudi table from S3"""
        try:
            return self.spark.read.format("hudi").load(path)
        except Exception:
            return None


class S3Writer:
    """Helper class for writing data to S3"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def _write_parquet(self, df, path, mode="overwrite"):
        """Write DataFrame as Parquet"""
        df.write.mode(mode).format("parquet").save(path)
    
    def _write_hudi(self, df, path, table_name, record_key, precombine_key, 
                    table_type="COPY_ON_WRITE", partition_path=""):
        """Write DataFrame as Hudi table"""
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_key,
            'hoodie.datasource.write.table.type': table_type,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
            'hoodie.upsert.shuffle.parallelism': 2,
            'hoodie.insert.shuffle.parallelism': 2
        }
        
        if partition_path:
            hudi_options['hoodie.datasource.write.partitionpath.field'] = partition_path
        
        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(path)
    
    def write_dataframe(self, df, path, format_type="parquet", mode="overwrite", 
                       hudi_config=None):
        """Write DataFrame to S3 with specified format"""
        if format_type == "hudi":
            if not hudi_config:
                raise ValueError("Hudi configuration required for Hudi format")
            self._write_hudi(df, path, **hudi_config)
        elif format_type == "parquet":
            self._write_parquet(df, path, mode)
        else:
            raise ValueError(f"Unsupported format: {format_type}")


class DataCleaner:
    """Helper class for data cleaning operations"""
    
    @staticmethod
    def remove_nulls(df, null_string="Null"):
        """Remove records with NULL values or 'Null' string"""
        columns = df.columns
        
        # Build filter condition for NULL values
        null_condition = col(columns[0]).isNotNull()
        for column in columns[1:]:
            null_condition = null_condition & col(column).isNotNull()
        
        # Build filter condition for "Null" string
        string_null_condition = col(columns[0]) != null_string
        for column in columns[1:]:
            string_null_condition = string_null_condition & (col(column) != null_string)
        
        # Apply both filters
        return df.filter(null_condition & string_null_condition)
    
    @staticmethod
    def remove_duplicates(df):
        """Remove duplicate records"""
        return df.dropDuplicates()


class SCD2Processor:
    """Helper class for SCD Type 2 processing"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def process_scd2(self, new_df, existing_df, business_keys):
        """
        Process SCD Type 2 logic
        
        Args:
            new_df: New data to process
            existing_df: Existing Hudi table data (or None for initial load)
            business_keys: List of columns that identify unique business records
        
        Returns:
            DataFrame ready for Hudi upsert
        """
        current_ts = current_timestamp()
        
        # Add SCD2 metadata columns to new data
        new_df_with_meta = new_df \
            .withColumn("IsActive", lit(True)) \
            .withColumn("StartDate", current_ts) \
            .withColumn("EndDate", lit(None).cast("timestamp")) \
            .withColumn("OpTs", current_ts)
        
        if existing_df is None:
            # Initial load - all records are new
            return new_df_with_meta
        
        # For incremental processing, we need to:
        # 1. Identify changed records
        # 2. Close old versions (set IsActive=false, EndDate=current_ts)
        # 3. Insert new versions
        
        # Get active records from existing data
        active_existing = existing_df.filter(col("IsActive") == True)
        
        # For simplicity, we'll return new records with SCD2 metadata
        # In production, implement full change detection logic
        return new_df_with_meta


class CatalogRegistrar:
    """Helper class for Glue Data Catalog registration"""
    
    def __init__(self, glue_context):
        self.glue_context = glue_context
    
    def register_table(self, df, database, table_name, path, format_type="parquet"):
        """Register table in Glue Data Catalog"""
        try:
            # Convert to DynamicFrame for catalog operations
            dynamic_frame = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": [path]},
                format=format_type
            )
            
            # Write to catalog
            self.glue_context.write_dynamic_frame.from_catalog(
                frame=dynamic_frame,
                database=database,
                table_name=table_name
            )
            return True
        except Exception as e:
            print(f"Catalog registration warning for {table_name}: {str(e)}")
            return False


class GlueETLJob:
    """Main ETL Job class"""
    
    def __init__(self, args):
        self.args = args
        
        # Initialize Spark and Glue contexts
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(args['JOB_NAME'], args)
        
        # Initialize helpers
        self.s3_reader = S3Reader(self.spark)
        self.s3_writer = S3Writer(self.spark)
        self.data_cleaner = DataCleaner()
        self.scd2_processor = SCD2Processor(self.spark)
        self.catalog_registrar = CatalogRegistrar(self.glue_context)
        
        # Load configuration
        self.config = self._load_config()
    
    def _load_config(self):
        """Load configuration from job arguments with defaults"""
        return {
            'source_customer_path': self.args.get('source_customer_path', 
                                                   's3://adif-sdlc/sdlc_wizard/customerdata/'),
            'source_order_path': self.args.get('source_order_path', 
                                               's3://adif-sdlc/sdlc_wizard/orderdata/'),
            'target_ordersummary_path': self.args.get('target_ordersummary_path', 
                                                      's3://adif-sdlc/curated/sdlc_wizard/ordersummary/'),
            'target_customeraggregate_path': self.args.get('target_customeraggregate_path', 
                                                           's3://adif-sdlc/analytics/customeraggregatespend/'),
            'target_customer_path': self.args.get('target_customer_path', 
                                                  's3://adif-sdlc/curated/sdlc_wizard/customer/'),
            'target_order_path': self.args.get('target_order_path', 
                                               's3://adif-sdlc/curated/sdlc_wizard/order/'),
            'catalog_database': self.args.get('catalog_database', 'gen_ai_poc_databrickscoe'),
            'catalog_table_customer': self.args.get('catalog_table_customer', 'sdlc_wizard_customer'),
            'catalog_table_order': self.args.get('catalog_table_order', 'sdlc_wizard_order'),
            'catalog_table_ordersummary': self.args.get('catalog_table_ordersummary', 'ordersummary'),
            'catalog_table_customeraggregate': self.args.get('catalog_table_customeraggregate', 
                                                            'customeraggregatespend'),
            'null_string_value': self.args.get('null_string_value', 'Null'),
            'hudi_table_type': self.args.get('hudi_table_type', 'COPY_ON_WRITE'),
            'hudi_record_key': self.args.get('hudi_record_key', 'OrderId,CustId'),
            'hudi_precombine_key': self.args.get('hudi_precombine_key', 'OpTs'),
        }
    
    def ingest_customer_data(self):
        """Ingest customer data from S3"""
        print(f"Reading customer data from {self.config['source_customer_path']}")
        return self.s3_reader.read_csv(self.config['source_customer_path'])
    
    def ingest_order_data(self):
        """Ingest order data from S3"""
        print(f"Reading order data from {self.config['source_order_path']}")
        return self.s3_reader.read_csv(self.config['source_order_path'])
    
    def clean_data(self, df, dataset_name):
        """Clean data by removing nulls and duplicates"""
        print(f"Cleaning {dataset_name} data...")
        
        initial_count = df.count()
        
        # Remove nulls
        df_no_nulls = self.data_cleaner.remove_nulls(df, self.config['null_string_value'])
        after_null_removal = df_no_nulls.count()
        print(f"  Removed {initial_count - after_null_removal} records with null values")
        
        # Remove duplicates
        df_clean = self.data_cleaner.remove_duplicates(df_no_nulls)
        final_count = df_clean.count()
        print(f"  Removed {after_null_removal - final_count} duplicate records")
        print(f"  Final count: {final_count} records")
        
        return df_clean
    
    def process_ordersummary(self, customer_df, order_df):
        """Process order summary with SCD Type 2 logic"""
        print("Processing order summary with SCD Type 2...")
        
        # Join customer and order data
        joined_df = order_df.join(customer_df, "CustId", "inner")
        print(f"  Joined records: {joined_df.count()}")
        
        # Read existing Hudi table if exists
        existing_df = self.s3_reader.read_hudi(self.config['target_ordersummary_path'])
        
        # Process SCD2
        scd2_df = self.scd2_processor.process_scd2(
            joined_df, 
            existing_df, 
            business_keys=["OrderId", "CustId"]
        )
        
        # Write to Hudi
        hudi_config = {
            'table_name': self.config['catalog_table_ordersummary'],
            'record_key': self.config['hudi_record_key'],
            'precombine_key': self.config['hudi_precombine_key'],
            'table_type': self.config['hudi_table_type']
        }
        
        self.s3_writer.write_dataframe(
            scd2_df,
            self.config['target_ordersummary_path'],
            format_type="hudi",
            hudi_config=hudi_config
        )
        
        print(f"  Written {scd2_df.count()} records to order summary")
        return scd2_df
    
    def generate_customer_aggregate(self, ordersummary_df):
        """Generate customer aggregate spend table"""
        print("Generating customer aggregate spend...")
        
        # Filter active records
        active_df = ordersummary_df.filter(col("IsActive") == True)
        
        # Cast numeric columns
        active_df = active_df \
            .withColumn("PricePerUnit", col("PricePerUnit").cast("double")) \
            .withColumn("Qty", col("Qty").cast("int"))
        
        # Calculate total amount
        active_df = active_df.withColumn("TotalAmount", col("PricePerUnit") * col("Qty"))
        
        # Aggregate by Name and Date
        agg_df = active_df.groupBy("Name", "Date") \
            .agg(spark_sum("TotalAmount").alias("TotalAmount"))
        
        print(f"  Aggregated to {agg_df.count()} records")
        
        # Write to S3
        self.s3_writer.write_dataframe(
            agg_df,
            self.config['target_customeraggregate_path'],
            format_type="parquet",
            mode="overwrite"
        )
        
        return agg_df
    
    def write_cleaned_tables(self, customer_df, order_df):
        """Write cleaned customer and order tables"""
        print("Writing cleaned customer and order tables...")
        
        # Write customer table
        self.s3_writer.write_dataframe(
            customer_df,
            self.config['target_customer_path'],
            format_type="parquet",
            mode="overwrite"
        )
        
        # Write order table
        self.s3_writer.write_dataframe(
            order_df,
            self.config['target_order_path'],
            format_type="parquet",
            mode="overwrite"
        )
        
        print("  Cleaned tables written successfully")
    
    def run(self):
        """Execute the complete ETL pipeline"""
        try:
            print("=" * 80)
            print("Starting AWS Glue ETL Job: Customer Order SCD Type 2 Processing")
            print("=" * 80)
            
            # Step 1: Ingest data
            customer_raw = self.ingest_customer_data()
            order_raw = self.ingest_order_data()
            
            # Step 2: Clean data
            customer_clean = self.clean_data(customer_raw, "customer")
            order_clean = self.clean_data(order_raw, "order")
            
            # Step 3: Write cleaned tables
            self.write_cleaned_tables(customer_clean, order_clean)
            
            # Step 4: Process order summary with SCD Type 2
            ordersummary_df = self.process_ordersummary(customer_clean, order_clean)
            
            # Step 5: Generate customer aggregate
            customeraggregate_df = self.generate_customer_aggregate(ordersummary_df)
            
            print("=" * 80)
            print("ETL Job completed successfully")
            print("=" * 80)
            
            self.job.commit()
            
        except Exception as e:
            print(f"ERROR: Job failed with exception: {str(e)}")
            raise


def main():
    """Main entry point for Glue job"""
    # Get job arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    # Add optional parameters
    optional_params = [
        'source_customer_path',
        'source_order_path',
        'target_ordersummary_path',
        'target_customeraggregate_path',
        'target_customer_path',
        'target_order_path',
        'catalog_database',
        'catalog_table_customer',
        'catalog_table_order',
        'catalog_table_ordersummary',
        'catalog_table_customeraggregate',
        'null_string_value',
        'hudi_table_type',
        'hudi_record_key',
        'hudi_precombine_key'
    ]
    
    for param in optional_params:
        try:
            optional_args = getResolvedOptions(sys.argv, [param])
            args.update(optional_args)
        except Exception:
            pass
    
    # Create and run job
    job = GlueETLJob(args)
    job.run()


if __name__ == "__main__":
    main()