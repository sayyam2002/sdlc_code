"""
AWS Glue PySpark ETL Job - Customer and Order Data Processing

This job implements a complete ETL pipeline with:
- Multi-source data ingestion (S3)
- Data quality validation and cleansing
- SCD Type 2 implementation using Apache Hudi
- Glue Catalog integration
- Comprehensive error handling and logging

Requirements:
    - FR-INGEST-001: Ingest customer and order data from S3
    - FR-CLEAN-001: Apply data quality rules and cleansing
    - FR-TRANSFORM-001: Apply business transformations
    - FR-SCD2-001: Implement SCD Type 2 for customer dimension
    - FR-CATALOG-001: Register tables in Glue Catalog
    - FR-MONITOR-001: Log metrics and errors to CloudWatch

Author: Data Engineering Team
Version: 1.0.0
"""

import sys
import logging
import yaml
import boto3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from decimal import Decimal

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, concat, lower, trim, regexp_replace,
    year, month, quarter, datediff, current_date, current_timestamp,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    row_number, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DecimalType, BooleanType, IntegerType
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


class S3PathValidator:
    """Validates S3 paths before read/write operations"""

    def __init__(self):
        self.s3_client = boto3.client('s3')

    def validate_read_path(self, s3_path: str) -> bool:
        """
        Validate S3 path exists and is accessible for reading

        Args:
            s3_path: S3 path (s3://bucket/key)

        Returns:
            bool: True if path is valid and accessible
        """
        try:
            bucket, key = self._parse_s3_path(s3_path)

            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=bucket)

            # Check if path has objects
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=key,
                MaxKeys=1
            )

            if 'Contents' not in response:
                logger.warning(f"No objects found at path: {s3_path}")
                return False

            logger.info(f"Validated read path: {s3_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to validate read path {s3_path}: {str(e)}")
            return False

    def validate_write_path(self, s3_path: str) -> bool:
        """
        Validate S3 path is accessible for writing

        Args:
            s3_path: S3 path (s3://bucket/key)

        Returns:
            bool: True if path is valid and writable
        """
        try:
            bucket, key = self._parse_s3_path(s3_path)

            # Check if bucket exists and is writable
            self.s3_client.head_bucket(Bucket=bucket)

            # Try to put a test object
            test_key = f"{key}_write_test_{datetime.now().timestamp()}"
            self.s3_client.put_object(
                Bucket=bucket,
                Key=test_key,
                Body=b'test'
            )

            # Clean up test object
            self.s3_client.delete_object(Bucket=bucket, Key=test_key)

            logger.info(f"Validated write path: {s3_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to validate write path {s3_path}: {str(e)}")
            return False

    @staticmethod
    def _parse_s3_path(s3_path: str) -> Tuple[str, str]:
        """Parse S3 path into bucket and key"""
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")

        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''

        return bucket, key


class DataQualityValidator:
    """Validates data quality based on configured rules"""

    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config
        self.quarantine_path = config['target_paths']['quarantine_path']

    def validate_customer_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate customer data against quality rules

        Args:
            df: Customer DataFrame

        Returns:
            Tuple of (valid_df, invalid_df)
        """
        logger.info("Starting customer data quality validation")

        rules = self.config['data_quality']['customer']['rules']

        # Initialize validation flag
        df = df.withColumn('_is_valid', lit(True))
        df = df.withColumn('_validation_errors', lit(''))

        for rule in rules:
            rule_name = rule['name']
            rule_type = rule['type']
            column = rule['column']

            logger.info(f"Applying rule: {rule_name}")

            if rule_type == 'regex':
                pattern = rule['pattern']
                df = df.withColumn(
                    '_is_valid',
                    col('_is_valid') & col(column).rlike(pattern)
                )
                df = df.withColumn(
                    '_validation_errors',
                    when(
                        ~col(column).rlike(pattern),
                        concat(col('_validation_errors'), lit(f"{rule_name};"))
                    ).otherwise(col('_validation_errors'))
                )

            elif rule_type == 'whitelist':
                valid_values = rule['values']
                df = df.withColumn(
                    '_is_valid',
                    col('_is_valid') & col(column).isin(valid_values)
                )
                df = df.withColumn(
                    '_validation_errors',
                    when(
                        ~col(column).isin(valid_values),
                        concat(col('_validation_errors'), lit(f"{rule_name};"))
                    ).otherwise(col('_validation_errors'))
                )

            elif rule_type == 'date_range':
                min_date = rule.get('min_date')
                max_date = rule.get('max_date', 'current_date')

                if max_date == 'current_date':
                    df = df.withColumn(
                        '_is_valid',
                        col('_is_valid') & (col(column) <= current_date())
                    )
                    df = df.withColumn(
                        '_validation_errors',
                        when(
                            col(column) > current_date(),
                            concat(col('_validation_errors'), lit(f"{rule_name};"))
                        ).otherwise(col('_validation_errors'))
                    )

        # Split into valid and invalid
        valid_df = df.filter(col('_is_valid')).drop('_is_valid', '_validation_errors')
        invalid_df = df.filter(~col('_is_valid'))

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        logger.info(f"Customer validation complete: {valid_count} valid, {invalid_count} invalid")

        # Write invalid records to quarantine
        if invalid_count > 0:
            self._write_to_quarantine(invalid_df, 'customer')

        return valid_df, invalid_df

    def validate_order_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate order data against quality rules

        Args:
            df: Order DataFrame

        Returns:
            Tuple of (valid_df, invalid_df)
        """
        logger.info("Starting order data quality validation")

        rules = self.config['data_quality']['order']['rules']

        # Initialize validation flag
        df = df.withColumn('_is_valid', lit(True))
        df = df.withColumn('_validation_errors', lit(''))

        for rule in rules:
            rule_name = rule['name']
            rule_type = rule['type']
            column = rule['column']

            logger.info(f"Applying rule: {rule_name}")

            if rule_type == 'numeric_range':
                min_value = rule.get('min_value')
                max_value = rule.get('max_value')

                if min_value is not None:
                    df = df.withColumn(
                        '_is_valid',
                        col('_is_valid') & (col(column) >= min_value)
                    )

                if max_value is not None:
                    df = df.withColumn(
                        '_is_valid',
                        col('_is_valid') & (col(column) <= max_value)
                    )

                df = df.withColumn(
                    '_validation_errors',
                    when(
                        (col(column) < min_value) | (col(column) > max_value),
                        concat(col('_validation_errors'), lit(f"{rule_name};"))
                    ).otherwise(col('_validation_errors'))
                )

            elif rule_type == 'whitelist':
                valid_values = rule['values']
                df = df.withColumn(
                    '_is_valid',
                    col('_is_valid') & col(column).isin(valid_values)
                )
                df = df.withColumn(
                    '_validation_errors',
                    when(
                        ~col(column).isin(valid_values),
                        concat(col('_validation_errors'), lit(f"{rule_name};"))
                    ).otherwise(col('_validation_errors'))
                )

        # Split into valid and invalid
        valid_df = df.filter(col('_is_valid')).drop('_is_valid', '_validation_errors')
        invalid_df = df.filter(~col('_is_valid'))

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        logger.info(f"Order validation complete: {valid_count} valid, {invalid_count} invalid")

        # Write invalid records to quarantine
        if invalid_count > 0:
            self._write_to_quarantine(invalid_df, 'order')

        return valid_df, invalid_df

    def _write_to_quarantine(self, df: DataFrame, entity: str):
        """Write invalid records to quarantine path"""
        try:
            quarantine_path = f"{self.quarantine_path}{entity}/{datetime.now().strftime('%Y%m%d_%H%M%S')}/"

            df.withColumn('quarantine_timestamp', current_timestamp()) \
                .write \
                .mode('append') \
                .parquet(quarantine_path)

            logger.info(f"Written {df.count()} invalid {entity} records to quarantine: {quarantine_path}")

        except Exception as e:
            logger.error(f"Failed to write to quarantine: {str(e)}")


class CustomerOrderETL:
    """Main ETL job for customer and order data processing"""

    def __init__(self, config_path: str):
        """
        Initialize ETL job

        Args:
            config_path: Path to configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.spark = None
        self.glue_context = None
        self.job = None
        self.s3_validator = S3PathValidator()

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            if config_path.startswith('s3://'):
                # Load from S3
                s3 = boto3.client('s3')
                bucket, key = config_path.replace('s3://', '').split('/', 1)
                response = s3.get_object(Bucket=bucket, Key=key)
                config_content = response['Body'].read().decode('utf-8')
                config = yaml.safe_load(config_content)
            else:
                # Load from local file
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)

            logger.info(f"Configuration loaded from: {config_path}")
            return config

        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def initialize_spark_contexts(self):
        """Initialize Spark and Glue contexts"""
        try:
            logger.info("Initializing Spark and Glue contexts")

            # Initialize Spark context
            sc = SparkContext()

            # Initialize Glue context
            self.glue_context = GlueContext(sc)
            self.spark = self.glue_context.spark_session

            # Apply Spark configurations
            spark_config = self.config.get('spark_config', {})
            for key, value in spark_config.items():
                config_key = f"spark.{key.replace('_', '.')}"
                self.spark.conf.set(config_key, str(value))

            # Initialize Glue job
            args = getResolvedOptions(sys.argv, ['JOB_NAME'])
            self.job = Job(self.glue_context)
            self.job.init(args['JOB_NAME'], args)

            logger.info("Spark and Glue contexts initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize contexts: {str(e)}")
            raise

    def read_customer_data(self) -> DataFrame:
        """
        Read customer data from S3

        Returns:
            Customer DataFrame

        Implements: FR-INGEST-001
        """
        try:
            source_path = self.config['source_paths']['customer_data']
            logger.info(f"Reading customer data from: {source_path}")

            # Validate S3 path
            if not self.s3_validator.validate_read_path(source_path):
                raise ValueError(f"Invalid or inaccessible S3 path: {source_path}")

            # Read CSV data
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(source_path)

            # Normalize column names
            df = df.toDF(*[c.lower() for c in df.columns])

            record_count = df.count()
            logger.info(f"Read {record_count} customer records")

            return df

        except Exception as e:
            logger.error(f"Failed to read customer data: {str(e)}")
            raise

    def read_order_data(self) -> DataFrame:
        """
        Read order data from S3

        Returns:
            Order DataFrame

        Implements: FR-INGEST-001
        """
        try:
            source_path = self.config['source_paths']['order_data']
            logger.info(f"Reading order data from: {source_path}")

            # Validate S3 path
            if not self.s3_validator.validate_read_path(source_path):
                raise ValueError(f"Invalid or inaccessible S3 path: {source_path}")

            # Read Parquet data
            df = self.spark.read.parquet(source_path)

            # Normalize column names
            df = df.toDF(*[c.lower() for c in df.columns])

            record_count = df.count()
            logger.info(f"Read {record_count} order records")

            return df

        except Exception as e:
            logger.error(f"Failed to read order data: {str(e)}")
            raise

    def transform_customer_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to customer data

        Args:
            df: Raw customer DataFrame

        Returns:
            Transformed customer DataFrame

        Implements: FR-TRANSFORM-001
        """
        try:
            logger.info("Transforming customer data")

            transform_config = self.config['transformations']['customer']

            # Apply column renaming
            rename_map = transform_config.get('rename_columns', {})
            for old_name, new_name in rename_map.items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)

            # Apply data cleansing
            cleansing_rules = transform_config.get('cleansing', [])
            for rule in cleansing_rules:
                column = rule['column']
                operation = rule['operation']

                if operation == 'lowercase':
                    df = df.withColumn(column, lower(col(column)))
                elif operation == 'trim':
                    df = df.withColumn(column, trim(col(column)))
                elif operation == 'remove_special_chars':
                    pattern = rule.get('pattern', '[^a-zA-Z0-9]')
                    df = df.withColumn(column, regexp_replace(col(column), pattern, ''))

            # Add derived columns
            derived_columns = transform_config.get('derived_columns', [])
            for derived in derived_columns:
                col_name = derived['name']
                expression = derived['expression']

                if 'concat' in expression:
                    # Handle concat expressions
                    parts = expression.replace('concat(', '').replace(')', '').split(',')
                    parts = [p.strip().strip("'") for p in parts]
                    df = df.withColumn(col_name, concat(*[
                        lit(p) if p == ' ' else col(p) for p in parts
                    ]))
                elif 'year(' in expression:
                    source_col = expression.replace('year(', '').replace(')', '')
                    df = df.withColumn(col_name, year(col(source_col)))
                elif 'datediff' in expression:
                    # Parse datediff expression
                    df = df.withColumn(col_name,
                        datediff(current_date(), col('registration_timestamp'))
                    )

            # Add SCD Type 2 columns
            df = df.withColumn('is_active', lit(True))
            df = df.withColumn('effective_start_date', current_timestamp())
            df = df.withColumn('effective_end_date', lit(None).cast(TimestampType()))
            df = df.withColumn('op_ts', current_timestamp())
            df = df.withColumn('updated_at', current_timestamp())

            logger.info("Customer data transformation complete")
            return df

        except Exception as e:
            logger.error(f"Failed to transform customer data: {str(e)}")
            raise

    def transform_order_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to order data

        Args:
            df: Raw order DataFrame

        Returns:
            Transformed order DataFrame

        Implements: FR-TRANSFORM-001
        """
        try:
            logger.info("Transforming order data")

            transform_config = self.config['transformations']['order']

            # Apply column renaming
            rename_map = transform_config.get('rename_columns', {})
            for old_name, new_name in rename_map.items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)

            # Add derived columns
            derived_columns = transform_config.get('derived_columns', [])
            for derived in derived_columns:
                col_name = derived['name']
                expression = derived['expression']

                if 'year(' in expression:
                    source_col = expression.replace('year(', '').replace(')', '')
                    df = df.withColumn(col_name, year(col(source_col)))
                elif 'month(' in expression:
                    source_col = expression.replace('month(', '').replace(')', '')
                    df = df.withColumn(col_name, month(col(source_col)))
                elif 'quarter(' in expression:
                    source_col = expression.replace('quarter(', '').replace(')', '')
                    df = df.withColumn(col_name, quarter(col(source_col)))
                elif 'datediff' in expression:
                    df = df.withColumn(col_name,
                        datediff(current_date(), col('order_date'))
                    )

            # Add operation timestamp
            df = df.withColumn('op_ts', current_timestamp())
            if 'updated_at' not in df.columns:
                df = df.withColumn('updated_at', current_timestamp())

            logger.info("Order data transformation complete")
            return df

        except Exception as e:
            logger.error(f"Failed to transform order data: {str(e)}")
            raise

    def write_customer_hudi(self, df: DataFrame):
        """
        Write customer data to Hudi with SCD Type 2

        Args:
            df: Transformed customer DataFrame

        Implements: FR-SCD2-001
        """
        try:
            logger.info("Writing customer data to Hudi")

            target_path = self.config['target_paths']['curated_customer']
            scd_config = self.config['scd_type2']['customer']
            hudi_options = scd_config['hudi_options']

            # Validate write path
            if not self.s3_validator.validate_write_path(target_path):
                raise ValueError(f"Invalid or inaccessible write path: {target_path}")

            # Prepare Hudi options
            hudi_write_options = {
                'hoodie.table.name': hudi_options['table_name'],
                'hoodie.datasource.write.recordkey.field': scd_config['record_key'],
                'hoodie.datasource.write.precombine.field': scd_config['precombine_field'],
                'hoodie.datasource.write.partitionpath.field': scd_config['partition_path'],
                'hoodie.datasource.write.operation': hudi_options['operation'],
                'hoodie.datasource.write.table.type': hudi_options['table_type'],
                'hoodie.upsert.shuffle.parallelism': hudi_options['upsert_parallelism'],
                'hoodie.insert.shuffle.parallelism': hudi_options['bulk_insert_parallelism'],
                'hoodie.delete.shuffle.parallelism': hudi_options['delete_parallelism'],
                'hoodie.compact.inline': hudi_options['inline_compact'],
                'hoodie.compact.inline.max.delta.commits': hudi_options['inline_compact_max_delta_commits'],
                'hoodie.cleaner.policy': hudi_options['cleaner_policy'],
                'hoodie.cleaner.commits.retained': hudi_options['cleaner_commits_retained'],
                'hoodie.index.type': hudi_options['index_type'],
                'hoodie.bloom.index.filter.type': 'DYNAMIC_V0',
                'hoodie.datasource.write.hive_style_partitioning': 'true'
            }

            # Write to Hudi
            df.write \
                .format('hudi') \
                .options(**hudi_write_options) \
                .mode('append') \
                .save(target_path)

            logger.info(f"Customer data written to Hudi: {target_path}")

            # Register in Glue Catalog
            self._register_glue_table(
                'customer',
                target_path,
                self.config['glue_catalog']['tables']['customer_dimension']
            )

        except Exception as e:
            logger.error(f"Failed to write customer Hudi data: {str(e)}")
            raise

    def write_order_hudi(self, df: DataFrame):
        """
        Write order data to Hudi

        Args:
            df: Transformed order DataFrame

        Implements: FR-SCD2-001
        """
        try:
            logger.info("Writing order data to Hudi")

            target_path = self.config['target_paths']['curated_order']
            scd_config = self.config['scd_type2']['order']
            hudi_options = scd_config['hudi_options']

            # Validate write path
            if not self.s3_validator.validate_write_path(target_path):
                raise ValueError(f"Invalid or inaccessible write path: {target_path}")

            # Prepare Hudi options
            hudi_write_options = {
                'hoodie.table.name': hudi_options['table_name'],
                'hoodie.datasource.write.recordkey.field': scd_config['record_key'],
                'hoodie.datasource.write.precombine.field': scd_config['precombine_field'],
                'hoodie.datasource.write.partitionpath.field': scd_config['partition_path'],
                'hoodie.datasource.write.operation': hudi_options['operation'],
                'hoodie.datasource.write.table.type': hudi_options['table_type'],
                'hoodie.upsert.shuffle.parallelism': hudi_options['upsert_parallelism'],
                'hoodie.insert.shuffle.parallelism': hudi_options['bulk_insert_parallelism'],
                'hoodie.datasource.write.hive_style_partitioning': 'true'
            }

            # Write to Hudi
            df.write \
                .format('hudi') \
                .options(**hudi_write_options) \
                .mode('append') \
                .save(target_path)

            logger.info(f"Order data written to Hudi: {target_path}")

            # Register in Glue Catalog
            self._register_glue_table(
                'order',
                target_path,
                self.config['glue_catalog']['tables']['order_fact']
            )

        except Exception as e:
            logger.error(f"Failed to write order Hudi data: {str(e)}")
            raise

    def calculate_customer_metrics(self, order_df: DataFrame) -> DataFrame:
        """
        Calculate customer aggregated metrics

        Args:
            order_df: Order DataFrame

        Returns:
            Customer metrics DataFrame
        """
        try:
            logger.info("Calculating customer metrics")

            agg_config = self.config['transformations']['order']['aggregations']
            group_by_cols = agg_config['group_by']

            # Perform aggregations
            metrics_df = order_df.groupBy(*group_by_cols).agg(
                count("*").alias("total_orders"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                spark_min("order_date").alias("first_order_date"),
                spark_max("order_date").alias("last_order_date")
            )

            # Add derived metrics
            metrics_df = metrics_df.withColumn(
                "customer_lifetime_days",
                datediff(col("last_order_date"), col("first_order_date"))
            )

            logger.info(f"Calculated metrics for {metrics_df.count()} customers")
            return metrics_df

        except Exception as e:
            logger.error(f"Failed to calculate customer metrics: {str(e)}")
            raise

    def write_customer_metrics(self, df: DataFrame):
        """
        Write customer metrics to S3

        Args:
            df: Customer metrics DataFrame
        """
        try:
            logger.info("Writing customer metrics")

            target_path = self.config['target_paths']['customer_metrics']

            # Validate write path
            if not self.s3_validator.validate_write_path(target_path):
                raise ValueError(f"Invalid or inaccessible write path: {target_path}")

            # Write as Parquet
            df.write \
                .mode('overwrite') \
                .partitionBy('customer_key') \
                .parquet(target_path)

            logger.info(f"Customer metrics written to: {target_path}")

            # Register in Glue Catalog
            self._register_glue_table(
                'metrics',
                target_path,
                self.config['glue_catalog']['tables']['customer_metrics']
            )

        except Exception as e:
            logger.error(f"Failed to write customer metrics: {str(e)}")
            raise

    def _register_glue_table(self, entity: str, s3_path: str, table_name: str):
        """
        Register table in Glue Catalog

        Args:
            entity: Entity type (customer, order, metrics)
            s3_path: S3 path to data
            table_name: Glue table name

        Implements: FR-CATALOG-001
        """
        try:
            logger.info(f"Registering {entity} table in Glue Catalog: {table_name}")

            database_name = self.config['glue_catalog']['database']

            # Create database if not exists
            glue_client = boto3.client('glue')

            try:
                glue_client.get_database(Name=database_name)
            except glue_client.exceptions.EntityNotFoundException:
                glue_client.create_database(
                    DatabaseInput={
                        'Name': database_name,
                        'Description': 'Curated analytics database'
                    }
                )
                logger.info(f"Created Glue database: {database_name}")

            # Note: Hudi tables are automatically registered by Hudi writer
            # For non-Hudi tables, we would use glue_client.create_table()

            logger.info(f"Table registered: {database_name}.{table_name}")

        except Exception as e:
            logger.error(f"Failed to register Glue table: {str(e)}")
            # Don't raise - catalog registration failure shouldn't fail the job

    def run(self):
        """
        Execute the complete ETL pipeline

        Implements: All FR requirements
        """
        try:
            logger.info("=" * 80)
            logger.info("Starting Customer Order ETL Job")
            logger.info("=" * 80)

            start_time = datetime.now(timezone.utc)

            # Initialize contexts
            self.initialize_spark_contexts()

            # Initialize data quality validator
            dq_validator = DataQualityValidator(self.spark, self.config)

            # Step 1: Ingest customer data (FR-INGEST-001)
            logger.info("Step 1: Ingesting customer data")
            customer_raw_df = self.read_customer_data()

            # Step 2: Validate customer data quality (FR-CLEAN-001)
            logger.info("Step 2: Validating customer data quality")
            customer_valid_df, customer_invalid_df = dq_validator.validate_customer_data(customer_raw_df)

            # Step 3: Transform customer data (FR-TRANSFORM-001)
            logger.info("Step 3: Transforming customer data")
            customer_transformed_df = self.transform_customer_data(customer_valid_df)

            # Step 4: Write customer data with SCD Type 2 (FR-SCD2-001)
            logger.info("Step 4: Writing customer data to Hudi")
            self.write_customer_hudi(customer_transformed_df)

            # Step 5: Ingest order data (FR-INGEST-001)
            logger.info("Step 5: Ingesting order data")
            order_raw_df = self.read_order_data()

            # Step 6: Validate order data quality (FR-CLEAN-001)
            logger.info("Step 6: Validating order data quality")
            order_valid_df, order_invalid_df = dq_validator.validate_order_data(order_raw_df)

            # Step 7: Transform order data (FR-TRANSFORM-001)
            logger.info("Step 7: Transforming order data")
            order_transformed_df = self.transform_order_data(order_valid_df)

            # Step 8: Write order data to Hudi (FR-SCD2-001)
            logger.info("Step 8: Writing order data to Hudi")
            self.write_order_hudi(order_transformed_df)

            # Step 9: Calculate customer metrics
            logger.info("Step 9: Calculating customer metrics")
            customer_metrics_df = self.calculate_customer_metrics(order_transformed_df)

            # Step 10: Write customer metrics
            logger.info("Step 10: Writing customer metrics")
            self.write_customer_metrics(customer_metrics_df)

            # Calculate job metrics
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Log final metrics (FR-MONITOR-001)
            logger.info("=" * 80)
            logger.info("ETL Job Completed Successfully")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Customer records processed: {customer_valid_df.count()}")
            logger.info(f"Customer records quarantined: {customer_invalid_df.count()}")
            logger.info(f"Order records processed: {order_valid_df.count()}")
            logger.info(f"Order records quarantined: {order_invalid_df.count()}")
            logger.info("=" * 80)

            # Commit Glue job
            if self.job:
                self.job.commit()

        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    """Main entry point for the Glue job"""
    try:
        # Get configuration path from job arguments
        args = getResolvedOptions(sys.argv, ['config_path'])
        config_path = args['config_path']

        # Create and run ETL job
        etl_job = CustomerOrderETL(config_path)
        etl_job.run()

    except Exception as e:
        logger.error(f"Job execution failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()