"""
Main ETL Job Module

Contains the core ETL job implementation for customer and order data processing.

Components:
    job: Main ETL orchestration and execution

Features:
    - AWS Glue integration
    - Apache Hudi SCD Type 2
    - Data quality validation
    - S3 path validation
    - Comprehensive error handling
    - CloudWatch logging
"""

__all__ = ["job"]