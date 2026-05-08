"""
AWS Glue PySpark ETL Job - Customer Order Analytics with SCD Type 2
Main package initialization

This package contains the core ETL job logic for processing customer and order data
with SCD Type 2 historical tracking using Apache Hudi format.

Modules:
    job: Main ETL job implementation with data ingestion, cleaning, SCD Type 2, and aggregation
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"
__description__ = "Customer Order Analytics ETL with SCD Type 2 using AWS Glue and Apache Hudi"

# Package metadata
__all__ = [
    "job",
]