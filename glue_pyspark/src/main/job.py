"""
AWS Glue PySpark Job - Minimal Template

⚠️ IMPORTANT: This is a TEMPLATE implementation because the source FRD/TRD
contains NO REQUIREMENTS IDENTIFIED.

This code demonstrates:
1. Proper AWS Glue context handling (using pre-initialized contexts)
2. Parameter loading from YAML and command-line
3. Basic data processing pipeline structure
4. Error handling and logging
5. Sample data generation for testing

UPDATE THIS CODE with actual requirements when available.
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# AWS Glue imports
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# PySpark imports
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, when,
    trim, upper, lower, regexp_replace
)

# Standard library
import yaml


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Dictionary containing configuration parameters
    """
    try:
        logger.info(f"Loading configuration from: {config_path}")

        # Handle S3 paths
        if config_path.startswith('s3://'):
            import boto3
            s3 = boto3.client('s3')
            bucket, key = config_path.replace('s3://', '').split('/', 1)
            obj = s3.get_object(Bucket=bucket, Key=key)
            config_content = obj['Body'].read().decode('utf-8')
            config = yaml.safe_load(config_content)
        else:
            # Local file
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

        logger.info("Configuration loaded successfully")
        return config

    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        # Return default configuration
        logger.warning("Using default configuration")
        return get_default_config()


def get_default_config() -> Dict[str, Any]:
    """
    Return default configuration when config file is not available.

    Returns:
        Dictionary with default parameters
    """
    return {
        'job_name': 'minimal-glue-job',
        'input': {
            'source_path': 's3://default-bucket/input/',
            'format': 'csv',
            'options': {
                'header': 'true',
                'inferSchema': 'true'
            }
        },
        'output': {
            'target_path': 's3://default-bucket/output/',
            'format': 'parquet',
            'mode': 'overwrite'
        },
        'processing': {
            'drop_duplicates': True,
            'repartition': 10
        },
        'logging': {
            'level': 'INFO'
        }
    }


def read_data(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """
    Read data from source based on configuration.

    NOTE: This is a TEMPLATE implementation. Update with actual source details.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary

    Returns:
        DataFrame containing source data
    """
    try:
        source_path = config['input']['source_path']
        source_format = config['input']['format']
        options = config['input'].get('options', {})

        logger.info(f"Reading data from: {source_path}")
        logger.info(f"Format: {source_format}")

        # Read data based on format
        if source_format.lower() == 'csv':
            df = spark.read.csv(
                source_path,
                header=options.get('header', 'true') == 'true',
                inferSchema=options.get('inferSchema', 'true') == 'true'
            )
        elif source_format.lower() == 'parquet':
            df = spark.read.parquet(source_path)
        elif source_format.lower() == 'json':
            df = spark.read.json(source_path)
        else:
            raise ValueError(f"Unsupported format: {source_format}")

        record_count = df.count()
        logger.info(f"Successfully read {record_count} records")
        logger.info(f"Schema: {df.schema}")

        return df

    except Exception as e:
        logger.error(f"Failed to read data: {str(e)}")
        raise


def transform_data(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """
    Apply transformations to the data.

    NOTE: This is a TEMPLATE implementation with basic transformations.
    UPDATE with actual business logic when requirements are available.

    Args:
        df: Input DataFrame
        config: Configuration dictionary

    Returns:
        Transformed DataFrame
    """
    try:
        logger.info("Starting data transformations")

        # Add processing timestamp
        df_transformed = df.withColumn(
            'processing_timestamp',
            current_timestamp()
        )

        # Drop duplicates if configured
        if config.get('processing', {}).get('drop_duplicates', False):
            initial_count = df_transformed.count()
            df_transformed = df_transformed.dropDuplicates()
            final_count = df_transformed.count()
            logger.info(f"Dropped {initial_count - final_count} duplicate records")

        # Example: Clean string columns (trim whitespace, handle nulls)
        string_columns = [
            field.name for field in df_transformed.schema.fields
            if str(field.dataType) == 'StringType'
        ]

        for col_name in string_columns:
            df_transformed = df_transformed.withColumn(
                col_name,
                when(col(col_name).isNull(), lit(None))
                .otherwise(trim(col(col_name)))
            )

        logger.info("Transformations completed successfully")
        logger.info(f"Output record count: {df_transformed.count()}")

        return df_transformed

    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}")
        raise


def write_data(df: DataFrame, config: Dict[str, Any]) -> None:
    """
    Write transformed data to target location.

    Args:
        df: DataFrame to write
        config: Configuration dictionary
    """
    try:
        target_path = config['output']['target_path']
        target_format = config['output']['format']
        write_mode = config['output'].get('mode', 'overwrite')
        partition_by = config['output'].get('partition_by', [])

        logger.info(f"Writing data to: {target_path}")
        logger.info(f"Format: {target_format}, Mode: {write_mode}")

        # Repartition if configured
        repartition_count = config.get('processing', {}).get('repartition')
        if repartition_count:
            logger.info(f"Repartitioning to {repartition_count} partitions")
            df = df.repartition(repartition_count)

        # Write data
        writer = df.write.mode(write_mode)

        if partition_by:
            logger.info(f"Partitioning by: {partition_by}")
            writer = writer.partitionBy(*partition_by)

        if target_format.lower() == 'parquet':
            writer.parquet(target_path)
        elif target_format.lower() == 'csv':
            writer.option('header', 'true').csv(target_path)
        elif target_format.lower() == 'json':
            writer.json(target_path)
        else:
            raise ValueError(f"Unsupported format: {target_format}")

        logger.info("Data written successfully")

    except Exception as e:
        logger.error(f"Failed to write data: {str(e)}")
        raise


def register_table(
    glueContext: GlueContext,
    df: DataFrame,
    config: Dict[str, Any]
) -> None:
    """
    Register DataFrame as a table in AWS Glue Catalog.

    Args:
        glueContext: GlueContext instance
        df: DataFrame to register
        config: Configuration dictionary
    """
    try:
        catalog_config = config.get('catalog', {})

        if not catalog_config.get('create_table', False):
            logger.info("Table registration disabled in configuration")
            return

        database_name = catalog_config.get('database_name', 'default')
        table_name = catalog_config.get('table_name', 'sample_table')

        logger.info(f"Registering table: {database_name}.{table_name}")

        # Convert to DynamicFrame
        from awsglue.dynamicframe import DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

        # Write to Glue Catalog
        glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=database_name,
            table_name=table_name
        )

        logger.info("Table registered successfully")

    except Exception as e:
        logger.error(f"Failed to register table: {str(e)}")
        # Don't raise - table registration is optional
        logger.warning("Continuing without table registration")


def main():
    """
    Main execution function for AWS Glue job.

    ⚠️ CRITICAL: Uses AWS Glue pre-initialized contexts (spark, glueContext, sc)
    DO NOT create new SparkContext or SparkSession in AWS Glue environment.
    """
    # Declare global contexts (pre-initialized by AWS Glue)
    global spark, glueContext, sc

    logger.info("=" * 80)
    logger.info("AWS Glue Job Started - Minimal Template")
    logger.info("⚠️  WARNING: This is a TEMPLATE - No actual requirements provided")
    logger.info("=" * 80)

    job = None

    try:
        # Get job parameters
        try:
            args = getResolvedOptions(
                sys.argv,
                ['JOB_NAME', 'config_path']
            )
            job_name = args['JOB_NAME']
            config_path = args['config_path']
        except Exception:
            # Fallback for local testing
            logger.warning("Running in local mode - using default parameters")
            job_name = "local-test-job"

            # Calculate config path relative to script location
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(script_dir)),
                'config',
                'glue_params.yaml'
            )

        logger.info(f"Job Name: {job_name}")
        logger.info(f"Config Path: {config_path}")

        # Load configuration
        config = load_config(config_path)

        # Check if contexts exist (AWS Glue environment)
        try:
            _ = sc.version
            logger.info("Using AWS Glue pre-initialized contexts")
        except NameError:
            # Local testing - create contexts
            logger.info("Creating contexts for local testing")
            sc = SparkContext()
            glueContext = GlueContext(sc)
            spark = glueContext.spark_session

        # Initialize Glue job
        job = Job(glueContext)
        job.init(job_name, args if 'args' in locals() else {})

        # Execute data pipeline
        logger.info("Starting data pipeline execution")

        # Step 1: Read data
        logger.info("Step 1: Reading source data")
        df_source = read_data(spark, config)

        # Step 2: Transform data
        logger.info("Step 2: Transforming data")
        df_transformed = transform_data(df_source, config)

        # Step 3: Write data
        logger.info("Step 3: Writing output data")
        write_data(df_transformed, config)

        # Step 4: Register table (optional)
        logger.info("Step 4: Registering table in Glue Catalog")
        register_table(glueContext, df_transformed, config)

        # Commit job
        if job:
            job.commit()

        logger.info("=" * 80)
        logger.info("AWS Glue Job Completed Successfully")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"AWS Glue Job Failed: {str(e)}")
        logger.error("=" * 80)

        if job:
            job.commit()

        raise

    finally:
        # Cleanup (only for local testing)
        if 'sc' in locals() and hasattr(sc, 'stop'):
            try:
                # Check if we created the context (local mode)
                if not hasattr(sys.modules[__name__], 'spark'):
                    logger.info("Stopping Spark context (local mode)")
                    sc.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark context: {str(e)}")


if __name__ == "__main__":
    main()