"""
AWS Glue PySpark Job: Customer Order SCD Type 2 Processing
Main entry point for the Glue job that processes customer and order data
with SCD Type 2 tracking using Apache Hudi.
"""

import sys
import os
from datetime import datetime
from typing import Dict, Any

# Conditional imports for AWS Glue compatibility
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    when, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

import yaml


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Load job configuration from a YAML file.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Dictionary containing the configuration.
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def get_glue_arguments(default_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get job parameters from Glue arguments, falling back to defaults if not provided.

    Args:
        default_params: Dictionary containing the default parameter values.

    Returns:
        Dictionary containing the resolved parameter values.
    """
    if GLUE_AVAILABLE and '--JOB_NAME' in sys.argv:
        try:
            glue_args = getResolvedOptions(sys.argv, list(default_params.keys()))
            params = {**default_params, **glue_args}
        except Exception:
            params = default_params
    else:
        params = default_params
    return params


def get_job_parameters() -> Dict[str, Any]:
    """
    Get job parameters from Glue arguments or local YAML config.
    Supports dual-mode execution: AWS Glue runtime and local testing.

    Returns:
        Dictionary containing all job parameters
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config',
        'glue_params.yaml'
    )
    config = load_yaml_config(config_path)

    # Flatten YAML structure for easier access
    default_params = {
        'JOB_NAME': config['job']['name'],
        'LOG_LEVEL': config['job']['log_level'],
        'CUSTOMER_SOURCE_PATH': config['inputs']['customer_source_path'],
        'CUSTOMER_SOURCE_FORMAT': config['inputs']['customer_source_format'],
        'ORDER_SOURCE_PATH': config['inputs']['order_source_path'],
        'ORDER_SOURCE_FORMAT': config['inputs']['order_source_format'],
        'CUSTOMER_TARGET_PATH': config['outputs']['customer_target_path'],
        'ORDER_TARGET_PATH': config['outputs']['order_target_path'],
        'HUDI_TARGET_PATH': config['outputs']['hudi_target_path'],
        'WRITE_MODE': config['outputs']['write_mode'],
        'DATABASE_NAME': config['outputs']['database_name'],
        'CUSTOMER_TABLE_NAME': config['outputs']['customer_table_name'],
        'ORDER_TABLE_NAME': config['outputs']['order_table_name'],
        'HUDI_TABLE_NAME': config['outputs']['hudi_table_name'],
        'WATERMARK': config['runtime']['watermark'],
        'ENABLE_INCREMENTAL': str(config['flags']['enable_incremental']),
        'ENABLE_CATALOG_REGISTRATION': str(config['flags']['enable_catalog_registration']),
        'ENABLE_SCD_PROCESSING': str(config['flags']['enable_scd_processing'])
    }

    params = get_glue_arguments(default_params)
    return params


# Other functions remain unchanged

def main():
    """
    Main execution function for the Glue job.
    """
    # Get job parameters
    params = get_job_parameters()

    # Initialize Spark context
    sc = SparkContext.getOrCreate()
    sc.setLogLevel(params['LOG_LEVEL'])

    # Initialize Glue context and job if available
    if GLUE_AVAILABLE:
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(params['JOB_NAME'], params)
    else:
        spark = SparkSession.builder \
            .appName(params['JOB_NAME']) \
            .getOrCreate()

    try:
        # TR-INGEST-001: Read customer CSV files from S3
        print(f"Reading customer data from {params['CUSTOMER_SOURCE_PATH']}")
        customer_schema = get_customer_schema()
        customer_raw_df = read_csv_data(
            spark,
            params['CUSTOMER_SOURCE_PATH'],
            customer_schema
        )

        # TR-INGEST-002: Read order CSV files from S3
        print(f"Reading order data from {params['ORDER_SOURCE_PATH']}")
        order_schema = get_order_schema()
        order_raw_df = read_csv_data(
            spark,
            params['ORDER_SOURCE_PATH'],
            order_schema
        )

        # TR-CLEAN-001 & TR-CLEAN-002: Clean customer data
        print("Cleaning customer data")
        customer_clean_df = clean_data(customer_raw_df)

        # TR-CLEAN-001 & TR-CLEAN-002: Clean order data
        print("Cleaning order data")
        order_clean_df = clean_data(order_raw_df)

        # TR-CATALOG-001: Write cleaned customer data to catalog
        if params['ENABLE_CATALOG_REGISTRATION'].lower() == 'true':
            write_to_catalog(
                customer_clean_df,
                params['CUSTOMER_TARGET_PATH'],
                params['DATABASE_NAME'],
                params['CUSTOMER_TABLE_NAME'],
                params['WRITE_MODE'],
                spark
            )

        # TR-CATALOG-002: Write cleaned order data to catalog
        if params['ENABLE_CATALOG_REGISTRATION'].lower() == 'true':
            write_to_catalog(
                order_clean_df,
                params['ORDER_TARGET_PATH'],
                params['DATABASE_NAME'],
                params['ORDER_TABLE_NAME'],
                params['WRITE_MODE'],
                spark
            )

        # TR-SCD-001: Join customer and order data
        print("Joining customer and order data")
        customer_order_df = join_customer_order(customer_clean_df, order_clean_df)

        # TR-INCR-001: Apply incremental filter if enabled
        if params['ENABLE_INCREMENTAL'].lower() == 'true':
            print(f"Applying incremental filter with watermark {params['WATERMARK']}")
            customer_order_df = apply_incremental_filter(
                customer_order_df,
                params['WATERMARK']
            )

        # TR-SCD-002: Add SCD Type 2 columns
        if params['ENABLE_SCD_PROCESSING'].lower() == 'true':
            print("Adding SCD Type 2 columns")
            customer_order_scd_df = add_scd_columns(customer_order_df)

            # TR-SCD-003: Write to Hudi format
            print(f"Writing SCD data to Hudi format at {params['HUDI_TARGET_PATH']}")
            write_to_hudi(
                customer_order_scd_df,
                params['HUDI_TARGET_PATH'],
                params['HUDI_TABLE_NAME'],
                params['DATABASE_NAME'],
                record_key='CustId,OrderId',
                precombine_field='OpTs'
            )

        print("Job completed successfully")

    except Exception as e:
        print(f"Job failed with error: {str(e)}")
        raise

    finally:
        # Commit job if using Glue
        if GLUE_AVAILABLE and 'job' in locals():
            job.commit()


if __name__ == "__main__":
    main()