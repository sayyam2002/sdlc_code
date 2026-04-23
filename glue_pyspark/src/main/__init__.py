"""
Main ETL Job Module

This module contains the core implementation of the AWS Glue ETL job
for customer and order data processing with SCD Type 2 support.
"""

from .job import (
    CustomerOrderETLJob,
    initialize_spark_contexts,
    read_data_safe,
    write_data_safe,
    validate_s3_path,
)

__all__ = [
    "CustomerOrderETLJob",
    "initialize_spark_contexts",
    "read_data_safe",
    "write_data_safe",
    "validate_s3_path",
]