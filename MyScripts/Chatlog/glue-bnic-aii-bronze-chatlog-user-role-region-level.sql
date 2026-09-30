
"""
AWS Glue ETL Pipeline for User Role and Region Data Processing

This script extracts user role and region data from source systems,
transforms organizational hierarchy information, and loads them into
a partitioned Parquet table for analytics consumption.

Job Arguments:
    data_date (str): Processing date in YYYY-MM-DD format for partitioning
    source_database (str): Redshift database name containing user role data
    source_schema (str): Redshift schema name containing the source table
    source_table (str): Redshift table name with user role information
    target_database (str): Target Glue database for processed data
    target_table (str): Target table name for user role analytics
    secret_name (str): AWS Secrets Manager secret name for Redshift credentials
    redshift_service_role_arn (str): IAM role ARN for Redshift UNLOAD operation
    extraction_s3_path (str): Base S3 path for UNLOAD output
"""

import logging
import sys
import json
import boto3
import time
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    trim,
    upper,
    when,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create StreamHandler for console output
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(stream_handler)


def get_secret(secret_name, region_name="us-east-1"):
    """
    Retrieve credentials from AWS Secrets Manager.

    Args:
        secret_name: Name of the secret in Secrets Manager
        region_name: AWS region where the secret is stored

    Returns:
        dict: Secret values as dictionary

    Raises:
        Exception: If secret retrieval fails
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response["SecretString"])
        return secret
    except Exception as e:
        raise Exception(f"Error retrieving secret {secret_name}: {e}")


def get_secret_arn(secret_name, region_name="us-east-1"):
    """
    Retrieve secret ARN from AWS Secrets Manager by secret name.

    Args:
        secret_name: Name of the secret in Secrets Manager
        region_name: AWS region where the secret is stored

    Returns:
        str: Secret ARN for authentication

    Raises:
        Exception: If secret ARN retrieval fails
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        describe_response = client.describe_secret(SecretId=secret_name)
        secret_arn = describe_response["ARN"]
        return secret_arn
    except Exception as e:
        raise Exception(f"Error retrieving secret ARN for {secret_name}: {e}")


def validate_date_parameter(data_dt_str: str) -> datetime:
    """
    Validate date parameter for format consistency.

    Args:
        data_dt_str: Data date string in YYYY-MM-DD format

    Returns:
        datetime: Parsed date object

    Raises:
        ValueError: If date format is invalid
    """
    try:
        data_date = datetime.strptime(data_dt_str, "%Y-%m-%d")
        return data_date
    except ValueError as e:
        error_msg = f"Invalid data_date format: {data_dt_str}. Expected YYYY-MM-DD"
        raise ValueError(error_msg) from e


def extract_user_role_data(
    spark,
    source_database,
    source_schema,
    source_table,
    secret_name,
    redshift_service_role_arn,
    extraction_s3_path,
):
    """
    Extract user role and region data from Redshift table using Data API UNLOAD command.

    Data is first extracted to a temporary S3 path to allow for transformation and
    validation before being written to the final destination. This approach ensures
    data integrity and allows for rollback if transformation fails.

    Args:
        spark: SparkSession instance
        source_database: Redshift database name
        source_schema: Redshift schema name
        source_table: Redshift table name
        secret_name: AWS Secrets Manager secret name for Redshift credentials
        redshift_service_role_arn: IAM role ARN for Redshift UNLOAD operation
        extraction_s3_path: Base S3 path for UNLOAD output

    Returns:
        DataFrame: Spark DataFrame with user role data from Redshift table

    Raises:
        Exception: If UNLOAD operation fails or S3 data cannot be read
    """

    full_table_name = f"{source_schema}.{source_table}"

    logger.info(
        "Starting extraction from Redshift database: %s, table: %s using UNLOAD",
        source_database,
        full_table_name,
    )

    try:
        REGION = "ap-southeast-1"

        logger.info(
            "Retrieving Redshift credentials from Secrets Manager: %s", secret_name
        )
        credentials = get_secret(secret_name, REGION)

        workgroup_name = credentials.get("workgroupName")

        if not workgroup_name:
            raise Exception("Missing required credential: workgroupName")

        # Use extraction_s3_path directly with _temp suffix for temporary storage
        temp_extraction_path = f"{extraction_s3_path.rstrip('/')}_temp/"

        redshift_client = boto3.client("redshift-data", region_name=REGION)

        logger.info("UNLOAD destination: %s", temp_extraction_path)

        # Build UNLOAD query using full table name
        unload_query = f"""
        UNLOAD ('SELECT * FROM {full_table_name}')
        TO '{temp_extraction_path}'
        IAM_ROLE '{redshift_service_role_arn}'
        FORMAT PARQUET
        CLEANPATH
        MAXFILESIZE 200 MB
        PARALLEL ON;
        """

        logger.info("Executing UNLOAD command via Redshift Data API")
        logger.debug("UNLOAD query: %s", unload_query.strip())

        secret_arn = get_secret_arn(secret_name, REGION)

        # Execute UNLOAD statement with secret ARN authentication
        response = redshift_client.execute_statement(
            WorkgroupName=workgroup_name,
            Database=source_database,
            Sql=unload_query,
            SecretArn=secret_arn,
        )

        query_id = response["Id"]
        logger.info("UNLOAD query submitted with ID: %s", query_id)

        # Wait for UNLOAD completion
        logger.info("Waiting for UNLOAD operation to complete...")
        while True:
            status_response = redshift_client.describe_statement(Id=query_id)
            status = status_response["Status"]

            if status == "FINISHED":
                logger.info("UNLOAD operation completed successfully")
                break
            elif status in ["FAILED", "ABORTED"]:
                error_msg = status_response.get("Error", "Unknown error")
                raise Exception(f"UNLOAD failed with status {status}: {error_msg}")
            else:
                logger.debug("UNLOAD status: %s", status)
                time.sleep(5)

        # Read the unloaded Parquet data with Spark
        logger.info("Reading unloaded data from S3: %s", temp_extraction_path)
        df = spark.read.parquet(temp_extraction_path)

        # Get record count for logging
        record_count = df.count()

        logger.info(
            "Extraction completed successfully. Records extracted: %d from %s.%s",
            record_count,
            source_database,
            full_table_name,
        )

        # Log schema for debugging
        logger.debug("Source table schema:")
        df.printSchema()

        return df

    except Exception as e:
        logger.exception(
            "Failed to extract data from Redshift table: %s.%s. Error: %s",
            source_database,
            full_table_name,
            str(e),
        )
        raise


def transform_user_role_data(df_source: DataFrame, data_dt_str: str) -> DataFrame:
    """
    Transform source data by adding system columns.

    Args:
        df_source: Spark DataFrame from extract function
        data_dt_str: Data date string in YYYY-MM-DD format
        logger: Logger instance for logging transformation metrics

    Returns:
        Spark DataFrame with added system columns

    Raises:
        Exception: If transformation fails
    """
    logger.info("Starting transformation - adding system columns")

    try:

        df_transformed = df_source.withColumn(
            "data_dt", lit(data_dt_str).cast("string")
        ).withColumn("load_timestamp", current_timestamp())

        logger.debug("Added metadata columns: data_dt and load_timestamp")
        logger.info("Transformation completed successfully")
        logger.info(
            "Added columns: data_dt (%s), load_timestamp (current)", data_dt_str
        )

        return df_transformed

    except Exception as e:
        logger.exception("Failed to transform data. Error: %s", str(e))
        raise


def load_user_role_table(
    df: DataFrame,
    spark: SparkSession,
    database: str,
    table: str,
    final_s3_path: str,
    temp_s3_path: str = None,
) -> None:
    """
    Write transformed user role data to final S3 path and create/replace Glue table.

    Args:
        df: Transformed DataFrame with target schema
        spark: SparkSession instance
        database: Target database name
        table: Target table name
        final_s3_path: Final S3 path for the processed data
        temp_s3_path: Temporary S3 path to clean up after successful write

    Returns:
        None (side effect: writes data to S3 and creates Glue table)

    Raises:
        Exception: If write operation fails
    """
    logger.info(
        "Starting load operation to table: %s.%s",
        database,
        table,
    )

    try:
        table_full_name = f"{database}.{table}"

        logger.info("Writing transformed data to final S3 path: %s", final_s3_path)

        # Repartition to single file (small extracted dataset)
        df_single_partition = df.repartition(1)

        # Write transformed data to final S3 location
        df_single_partition.write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).parquet(final_s3_path)

        logger.info("Successfully wrote to S3: %s", final_s3_path)

        # Clean up temporary S3 path after successful write
        if temp_s3_path:
            try:
                logger.info("Cleaning up temporary S3 path: %s", temp_s3_path)

                # Parse S3 path to get bucket and prefix
                if temp_s3_path.startswith("s3://"):
                    s3_parts = temp_s3_path[5:].split("/", 1)
                    bucket_name = s3_parts[0]
                    prefix = s3_parts[1] if len(s3_parts) > 1 else ""

                    # Create S3 client
                    s3_client = boto3.client("s3")

                    # List and delete all objects with the prefix
                    paginator = s3_client.get_paginator("list_objects_v2")
                    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

                    objects_deleted = 0
                    for page in pages:
                        if "Contents" in page:
                            objects_to_delete = [
                                {"Key": obj["Key"]} for obj in page["Contents"]
                            ]
                            if objects_to_delete:
                                s3_client.delete_objects(
                                    Bucket=bucket_name,
                                    Delete={"Objects": objects_to_delete},
                                )
                                objects_deleted += len(objects_to_delete)

                    logger.info(
                        "Successfully cleaned up temporary S3 path: %s (%d objects deleted)",
                        temp_s3_path,
                        objects_deleted,
                    )
                else:
                    logger.warning("Invalid S3 path format: %s", temp_s3_path)

            except Exception as cleanup_error:
                # Log cleanup failure but don't fail the entire job
                logger.warning(
                    "Failed to clean up temporary S3 path %s: %s. This may result in storage costs but does not affect data processing.",
                    temp_s3_path,
                    str(cleanup_error),
                )

        # Create or replace table in Glue catalog using Spark SQL
        logger.info("Creating/replacing Glue table: %s", table_full_name)

        # Use Spark SQL to create table from the written Parquet files
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_full_name}
        USING PARQUET
        LOCATION '{final_s3_path}'
        """

        spark.sql(create_table_sql)
        logger.info("Successfully created/replaced table: %s", table_full_name)

        # read from table again, to verify loading and creating table complete
        record_count = spark.table(table_full_name).count()
        logger.info(
            "Load operation completed successfully. Records processed: %d", record_count
        )

    except Exception as e:
        logger.exception(
            "Failed to write data to table %s.%s. Error: %s",
            database,
            table,
            str(e),
        )
        raise


def main() -> None:
    """
    Main entry point for the user role ETL pipeline.

    Orchestrates the extract, transform, and load operations for
    user role and region data processing.
    """
    # Initialize Glue context and Spark session
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    logger.info("Starting User Role ETL Pipeline")

    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "data_date",
                "source_database",
                "source_schema",
                "source_table",
                "target_database",
                "target_table",
                "secret_name",
                "redshift_service_role_arn",
                "extraction_s3_path",
            ],
        )
        data_dt_str = args["data_date"]
        source_database = args["source_database"]
        source_schema = args["source_schema"]
        source_table = args["source_table"]
        target_database = args["target_database"]
        target_table = args["target_table"]
        secret_name = args["secret_name"]
        redshift_service_role_arn = args["redshift_service_role_arn"]
        extraction_s3_path = args["extraction_s3_path"]

        logger.info(
            "Job parameters - data_date: %s, source: %s.%s.%s",
            data_dt_str,
            source_database,
            source_schema,
            source_table,
        )
        logger.info(
            "Target parameters - database: %s, table: %s",
            target_database,
            target_table,
        )

        # Validate date parameter format before processing
        validate_date_parameter(data_dt_str)

        # Set Bangkok timezone for consistent timestamp handling
        spark.conf.set("spark.sql.session.timeZone", "Asia/Bangkok")

        logger.info("Glue context and Spark session initialized successfully")
        logger.info("Spark version: %s", spark.version)

        # Step 1: Extract user role data from Redshift
        logger.info("Step 1: Extracting user role data from Redshift")
        df_source = extract_user_role_data(
            spark,
            source_database,
            source_schema,
            source_table,
            secret_name,
            redshift_service_role_arn,
            extraction_s3_path,
        )

        # Step 2: Transform user role data with standardization
        logger.info("Step 2: Transforming user role data with standardization")
        df_transformed = transform_user_role_data(df_source, data_dt_str)

        # Step 3: Load transformed data to target table
        logger.info("Step 3: Loading transformed data to target table")
        temp_s3_path = f"{extraction_s3_path.rstrip('/')}_temp/"
        load_user_role_table(
            df_transformed,
            spark,
            target_database,
            target_table,
            extraction_s3_path,
            temp_s3_path,
        )

        logger.info("User Role ETL Pipeline completed successfully")

    except Exception as e:
        logger.exception("ETL Pipeline failed with error")
        sys.exit(f"ETL Pipeline failed: {str(e)}")


if __name__ == "__main__":
    main()