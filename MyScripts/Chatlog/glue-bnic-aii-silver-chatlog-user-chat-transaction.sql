
"""
AWS Glue ETL Pipeline for Chatbot Transaction Processing

This script extracts chat history from DynamoDB, transforms nested conversation
sessions into flat transactional records,
and loads them into a partitioned Parquet table.
"""

import logging
import sys
from datetime import datetime
from typing import Tuple

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    explode,
    from_unixtime,
    from_utc_timestamp,
    get_json_object,
    lit,
    regexp_extract,
    size,
    to_timestamp,
    unix_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Configure module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


def validate_date_parameters(
    start_date_str: str, end_date_str: str
) -> Tuple[datetime, datetime]:
    """
    Validate date parameters for format and logical consistency.

    Args:
        start_date_str: Start date string in YYYY-MM-DD format
        end_date_str: End date string in YYYY-MM-DD format

    Returns:
        tuple: (start_date, end_date) as datetime objects

    Raises:
        ValueError: If date format is invalid or start_date > end_date
    """
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        logger.info("Parsed start_date: %s", start_date_str)
    except ValueError as e:
        error_msg = (
            f"Invalid start_data_date format: {start_date_str}. " "Expected YYYY-MM-DD"
        )
        logger.exception(error_msg)
        raise ValueError(error_msg) from e

    try:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        logger.info("Parsed end_date: %s", end_date_str)
    except ValueError as e:
        error_msg = (
            f"Invalid end_data_date format: {end_date_str}. " "Expected YYYY-MM-DD"
        )
        logger.exception(error_msg)
        raise ValueError(error_msg) from e

    if start_date > end_date:
        error_msg = (
            f"start_data_date ({start_date_str}) must be <= "
            f"end_data_date ({end_date_str})"
        )
        logger.exception(error_msg)
        raise ValueError(error_msg)

    logger.info(
        "Date parameters validated successfully: %s to %s",
        start_date_str,
        end_date_str,
    )
    return start_date, end_date


def extract_true_bnic_chat_history(
    glue_context: GlueContext,
    start_date: datetime,
    end_date: datetime,
    table_name: str,
) -> DataFrame:
    """
    Extract chat history records from DynamoDB within specified date range.
    Filters by createdAt field using Bangkok timezone conversion.

    Args:
        glue_context: GlueContext instance
        start_date: datetime object for range start (Bangkok timezone)
        end_date: datetime object for range end (Bangkok timezone)
        table_name: DynamoDB table name to extract from

    Returns:
        Spark DataFrame with schema matching DynamoDB table

    Raises:
        Exception: If connection to DynamoDB fails
    """
    logger.info(
        "Starting extraction from DynamoDB table: %s for date range %s to %s "
        "(Bangkok timezone)",
        table_name,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    try:
        dynamic_frame = glue_context.create_dynamic_frame.from_options(
            connection_type="dynamodb",
            connection_options={
                "dynamodb.input.tableName": table_name,
                "dynamodb.throughput.read.percent": "0.4",
                "dynamodb.splits": "16",
            },
        )

        df = dynamic_frame.toDF()

        # Apply date range filter on createdAt field in Bangkok timezone (inclusive)
        # Convert UTC timestamps to Bangkok timezone for filtering only
        start_timestamp = start_date.strftime("%Y-%m-%d 00:00:00")
        end_timestamp = end_date.strftime("%Y-%m-%d 23:59:59")

        logger.info(
            "Applying date filter on createdAt field: %s to %s (Bangkok timezone)",
            start_timestamp,
            end_timestamp,
        )

        # Filter using timezone-converted createdAt without storing the converted column
        df_filtered = df.filter(
            (
                from_utc_timestamp(
                    to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                    "Asia/Bangkok",
                )
                >= start_timestamp
            )
            & (
                from_utc_timestamp(
                    to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                    "Asia/Bangkok",
                )
                <= end_timestamp
            )
        )

        # Get record count and unique session count for logging
        record_count = df_filtered.count()
        unique_session_count = df_filtered.select("id").distinct().count()

        logger.info(
            "Extraction completed successfully. Records extracted: %d, "
            "Unique sessions: %d",
            record_count,
            unique_session_count,
        )

        return df_filtered

    except Exception as e:
        logger.exception(
            "Failed to extract data from DynamoDB table: %s. Error: %s",
            table_name,
            str(e),
        )
        raise


def transform_get_session_id(df_source: DataFrame) -> DataFrame:
    """
    Extract session identifiers and filter valid prompts.
    Filters out records with null or empty prompts arrays.

    Args:
        df_source: Raw DataFrame from DynamoDB extraction

    Returns:
        DataFrame with chat_log_id and filtered prompts
    """
    # Handle null/empty prompts arrays gracefully
    # Filter out records with null or empty prompts array
    df_with_prompts = df_source.filter(
        (col("prompts").isNotNull()) & (size(col("prompts")) > 0)
    )

    # Add session identifier from root id field
    df_with_session_id = df_with_prompts.withColumn("chat_log_id", col("id"))

    return df_with_session_id


def transform_explode_prompt_and_response(
    df_with_session_id: DataFrame,
) -> DataFrame:
    """
    Explode prompts array and extract conversation content.
    Filters for assistant role only to process AI responses.

    Args:
        df_with_session_id: DataFrame with session identifiers

    Returns:
        DataFrame with exploded prompts filtered for assistant responses
    """
    # Explode prompts array to create one row per prompt
    df_exploded = df_with_session_id.select(
        col("chat_log_id"),
        col("language"),
        col("createdAt"),
        col("updatedAt"),
        explode(col("prompts")).alias("prompt"),
    )

    # Filter for role = 'assistant' only, and handle missing role field
    df_assistant = df_exploded.filter(
        (col("prompt.role").isNotNull()) & (col("prompt.role") == "assistant")
    )

    return df_assistant


def transform_get_emp_id(df_assistant: DataFrame) -> DataFrame:
    """
    Extract employee ID from user context structure.
    Navigates nested tools_invoked structure to find employee identifier.

    Args:
        df_assistant: DataFrame with assistant-filtered prompts

    Returns:
        DataFrame with emp_id field added
    """

    # Path: prompt.tools_invoked[0].tool.arguments.user.context.profile.EmpID
    df_with_emp_id = df_assistant.withColumn(
        "emp_id",
        when(
            (col("prompt.tools_invoked").isNotNull())
            & (size(col("prompt.tools_invoked")) > 0)
            & (
                col("prompt.tools_invoked")[0]["tool"]["arguments"]["user"]["context"][
                    "profile"
                ]["EmpID"].isNotNull()
            ),
            col("prompt.tools_invoked")[0]["tool"]["arguments"]["user"]["context"][
                "profile"
            ]["EmpID"],
        ).otherwise(lit(None).cast("string")),
    )

    return df_with_emp_id


def transform_tools_invoked(df_with_emp_id: DataFrame) -> DataFrame:
    """
    Extract business intelligence fields from tool responses.
    Analyzes tool output to determine success status, business topic classification,
    and error messages for failed transactions.

    Args:
        df_with_emp_id: DataFrame with employee ID

    Returns:
        DataFrame with success flag, response topic, and error messages
    """
    # Extract success flag and response topic from toolOutput in single operation
    # Define common condition for tools_invoked structure validation and
    # toolOutput reference
    tools_invoked_exists = (
        (col("prompt.tools_invoked").isNotNull())
        & (size(col("prompt.tools_invoked")) > 0)
        & (col("prompt.tools_invoked")[0]["response"]["toolOutput"].isNotNull())
    )

    tool_output_col = col("prompt.tools_invoked")[0]["response"]["toolOutput"]

    df_with_response_topic = df_with_emp_id.withColumn(
        "transaction_success_flag",
        when(
            tools_invoked_exists,
            # Extract sql_query from JSON string in toolOutput
            when(
                tool_output_col.rlike("^Error.*"),
                lit("N"),  # If starts with "Error", success is N
            ).otherwise(
                lit("Y")
            ),  # Otherwise, success is Y
        ).otherwise(lit(None).cast("string")),
    ).withColumn(
        "transaction_response_topic",
        when(
            tools_invoked_exists
            & (get_json_object(tool_output_col, "$.explanation").isNotNull()),
            # Extract value after "CT |" using regex, trim whitespace
            regexp_extract(
                get_json_object(tool_output_col, "$.explanation"),
                # Match CT row and capture the value
                r"\|\s*CT\s*\|\s*([^|\n]+?)\s*\|",
                1,  # Return the first capture group
            ),
        ).otherwise(lit(None).cast("string")),
    )

    # Extract error messages for failed transactions
    df_with_error_message = df_with_response_topic.withColumn(
        "error_message",
        when(
            (col("transaction_success_flag") == "N") & tools_invoked_exists,
            when(
                # Check if toolOutput starts with "Error: [" (JSON array format)
                tool_output_col.rlike(r"^Error:\s*\["),
                # Extract the first error message from JSON array using regex
                # Pattern matches: Error: [{"query":"...","error":"MESSAGE"}...]
                regexp_extract(tool_output_col, r'"error"\s*:\s*"([^"]+)"', 1),
            ).otherwise(
                # Fallback: extract raw error text after "Error: " prefix
                regexp_extract(tool_output_col, r"^Error:\s*(.{1,100})", 1)
            ),
        ).otherwise(lit(None).cast("string")),
    )

    return df_with_error_message


def transform_mark_internal_team_flag(df: DataFrame) -> DataFrame:
    """
    Mark transactions from internal team members for filtering and analysis.

    Uses a predefined list of internal tester employee IDs to flag transactions
    that originate from internal team members vs external users.

    Args:
        df: input DataFrame including emp_id

    Returns:
        DataFrame with internal_team_flag column (Y/N values)
    """
    # Predefined list of internal tester employee IDs
    internal_tester_emp_ids = [
        "90001455",
        "90001969",
        "90002069",
        "90002268",
        "90003033",
        "90005975",
        "90005993",
        "90006037",
        "90006374",
        "90006544",
        "90008033",
        "90008344",
        "90008647",
        "90009227",
        "90009235",
        "90009772",
        "90011100",
        "90011489",
        "90011827",
        "90011987",
        "90012003",
        "90012419",
        "90049283",
        "yuth",
        "tanapat",
        "thanabodee",
        "panadda",
        "suchaya",
        "peeraphol",
    ]

    # Create internal team flag based on emp_id lookup
    df_with_internal_flag = df.withColumn(
        "internal_team_flag",
        when(col("emp_id").isin(internal_tester_emp_ids), lit("Y")).otherwise(lit("N")),
    )

    return df_with_internal_flag


def transform_execution_log(df_with_response_topic: DataFrame) -> DataFrame:
    """
    Extract execution logs and calculate timing metrics.

    Processes execution logs from both prompt and tool levels to derive
    performance metrics.

    Args:
        df_with_response_topic: DataFrame with response topic

    Returns:
        DataFrame with execution logs and timing fields
    """
    # Define schema for execution logs: array of struct with step (string) and
    # timestamp (long)
    execution_log_schema = ArrayType(
        StructType(
            [
                StructField("step", StringType(), True),
                StructField("timestamp", LongType(), True),
            ]
        )
    )

    # Add execution log fields with enforced schema
    df_with_execution_logs = df_with_response_topic.withColumn(
        "transaction_execution_log",
        # Extract executionLogs from prompt level and cast to proper schema
        when(
            col("prompt.executionLogs").isNotNull(),
            col("prompt.executionLogs").cast(execution_log_schema),
        ).otherwise(lit(None).cast(execution_log_schema)),
    ).withColumn(
        "tool_execution_log",
        # Extract executionLogs from tools_invoked response and cast to proper
        # schema
        when(
            (col("prompt.tools_invoked").isNotNull())
            & (size(col("prompt.tools_invoked")) > 0)
            & (col("prompt.tools_invoked")[0]["response"]["executionLogs"].isNotNull()),
            col("prompt.tools_invoked")[0]["response"]["executionLogs"].cast(
                execution_log_schema
            ),
        ).otherwise(lit(None).cast(execution_log_schema)),
    )

    # Add timing fields: transaction_start_time, transaction_end_time,
    # transaction_duration_second
    df_with_timing = (
        df_with_execution_logs.withColumn(
            "transaction_start_time",
            # Extract timestamp from first item in transaction_execution_log and
            # convert from unix milliseconds to timestamp
            from_unixtime(col("transaction_execution_log")[0]["timestamp"] / 1000).cast(
                TimestampType()
            ),
        )
        .withColumn(
            "transaction_end_time",
            # Extract timestamp from last item in tool_execution_log and convert
            # from unix milliseconds to timestamp
            from_unixtime(
                col("tool_execution_log")[size(col("tool_execution_log")) - 1][
                    "timestamp"
                ]
                / 1000
            ).cast(TimestampType()),
        )
        .withColumn(
            "transaction_duration_second",
            # Calculate duration in seconds between end_time and start_time
            unix_timestamp(col("transaction_end_time"))
            - unix_timestamp(col("transaction_start_time")),
        )
    )

    return df_with_timing


def transform_join_chat_txn_x_user_role(
    df_chat_transactions: DataFrame,
    spark: SparkSession,
    user_role_database: str,
    user_role_table: str,
) -> DataFrame:
    """
    Join chat transactions with user role and region data from Glue catalog.

    Enriches transaction data with organizational hierarchy and geographic
    information.

    Args:
        df_chat_transactions: Transformed chat transactions DataFrame
        spark: SparkSession instance
        user_role_database: Glue database name containing user role reference table
        user_role_table: Glue table name for user role and region mapping

    Returns:
        Spark DataFrame with enriched chat transactions including user role info

    Raises:
        Exception: If table read or join operation fails
    """
    logger.info("Starting join between chat transactions and user role data")

    try:
        user_role_database_table = f"{user_role_database}.{user_role_table}"
        logger.info(
            "Reading user role data from Glue table: %s", user_role_database_table
        )

        df_user_role = spark.table(user_role_database_table)

        # Log input counts
        chat_count = df_chat_transactions.count()
        user_role_count = df_user_role.count()

        logger.info(
            "Input counts - Chat transactions: %d, User roles: %d",
            chat_count,
            user_role_count,
        )

        # Log schema for debugging
        logger.debug("User role table schema:")
        df_user_role.printSchema()

        # Perform left join on emp_id = assignee_id
        df_joined = df_chat_transactions.alias("chat").join(
            df_user_role.alias("role"),
            col("chat.emp_id") == col("role.assignee_id"),
            "left",
        )

        # Select all chat columns and add organizational fields from user role table
        df_enriched = df_joined.select(
            col("chat.*"),
            # Add organizational fields from user role table
            col("role.bu_group").alias("bu_group"),
            col("role.emp_position").alias("emp_position"),
            col("role.area_type").alias("area_type"),
            col("role.area_code").alias("area_code"),
            col("role.area_name").alias("area_name"),
            col("role.region_code").alias("region_code"),
            col("role.region_name").alias("region_name"),
        )

        # Log join results
        output_count = df_enriched.count()
        matched_count = df_enriched.filter(col("bu_group").isNotNull()).count()
        unmatched_count = output_count - matched_count

        logger.info("Join completed successfully")
        logger.info("Output records: %d", output_count)
        logger.info("Matched records (with role data): %d", matched_count)
        logger.info("Unmatched records (no role data): %d", unmatched_count)

        if unmatched_count > 0:
            match_percentage = (
                (matched_count / output_count) * 100 if output_count > 0 else 0
            )
            logger.warning(
                "%.2f%% of records have no matching role data (%d unmatched)",
                100 - match_percentage,
                unmatched_count,
            )

        return df_enriched

    except Exception as e:
        logger.exception(
            "Failed to join chat transactions with user role data: %s", str(e)
        )
        raise


def finalize_transaction_dataframe(df_enriched: DataFrame) -> DataFrame:
    """
    Finalize the transaction DataFrame with proper field mapping and data types.

    Args:
        df_enriched: DataFrame with all transformations and joins applied

    Returns:
        Final DataFrame with target schema including error messages
    """
    # Map source fields to target fields per field mapping table
    df_final = df_enriched.select(
        col("prompt.messageId").alias("transaction_id"),
        col("chat_log_id"),
        col("language"),
        col("prompt.question.content").alias("transaction_prompt"),
        # NULL placeholder
        lit(None).cast("string").alias("transaction_prompt_topic"),
        col("prompt.content").alias("transaction_response"),
        col(
            "transaction_response_topic"
        ),  # Extracted from toolOutput explanation CT field
        col("transaction_success_flag"),  # Extracted from toolOutput sql_query
        col("error_message"),  # Error message for failed transactions
        col("emp_id"),  # Employee identifier
        col("internal_team_flag"),  # Internal team member flag (Y/N)
        col("transaction_execution_log"),  # Process execution logs
        col("tool_execution_log"),  # Tool execution logs
        col("transaction_start_time"),  # Transaction start timestamp
        col("transaction_end_time"),  # Transaction end timestamp
        col("transaction_duration_second"),  # Transaction duration in seconds
        col("bu_group"),  # Business unit group
        col("emp_position"),  # Employee Position as Nationwide, RH, PBH
        col("area_type"),  # Area classification
        col("area_code"),  # Area code identifier
        col("area_name"),  # Area name
        col("region_code"),  # Region code identifier
        col("region_name"),  # Region name
        col("createdAt").alias("transaction_prompt_timestamp"),
        date_format(
            to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            "yyyy-MM-dd",
        )
        .cast(StringType())
        .alias("data_dt"),
        current_timestamp().alias("load_timestamp"),
    )

    return df_final


def load_tbl_user_chat_transactions(
    df_target: DataFrame, spark: SparkSession, database: str, table: str
) -> None:
    """
    Write transformed data to partitioned Parquet table.

    This function writes the DataFrame to a Glue table, partitioned by data_dt,
    using dynamic partition overwrite mode to update only affected partitions.
    Creates the table if it doesn't exist, otherwise overwrites partitions.

    Args:
        df_target: Transformed Spark DataFrame with target schema
        spark: SparkSession instance
        database: Glue database name (e.g., 'chatlog_bi')
        table: Glue table name (e.g., 'tbl_user_chat_transactions')

    Returns:
        None (side effect: writes to Glue table)

    Raises:
        Exception: If write operation fails
    """
    logger.info(
        "Starting load operation to table: %s.%s",
        database,
        table,
    )

    record_count = df_target.count()
    if record_count == 0:
        logger.warning("DataFrame is empty. Skipping write operation. No data to load.")
        return

    logger.info("Records to write: %d", record_count)

    partitions_to_write = df_target.select("data_dt").distinct().collect()
    partition_dates = [str(row.data_dt) for row in partitions_to_write]
    partition_count = len(partition_dates)

    logger.info(
        "Partitions to be affected: %d (%s)",
        partition_count,
        ", ".join(sorted(partition_dates)),
    )

    try:
        table_full_name = f"{database}.{table}"

        # Check if table exists using SQL-based approach
        table_exists = False
        try:
            spark.sql(f"DESCRIBE TABLE {table_full_name}")
            table_exists = True
            logger.debug("Table %s exists", table_full_name)
        except Exception:
            logger.debug("Table %s does not exist", table_full_name)

        # Create table if not exists
        if not table_exists:
            logger.info("Table %s does not exist. Creating new table.", table_full_name)
            df_target.write.mode("overwrite").option(
                "overwriteSchema", "true"
            ).partitionBy("data_dt").saveAsTable(table_full_name)
            logger.info("Created table %s successfully", table_full_name)
        else:
            logger.info("Table %s exists. Overwriting partitions.", table_full_name)
            # Overwrite partitions using InsertInto with dynamic partition overwriting
            # Reorder columns according to the target table schema
            target_columns = spark.table(table_full_name).columns
            df_target.select(target_columns).write.mode("overwrite").insertInto(
                table_full_name
            )
            logger.info("Overwritten partitions in table %s", table_full_name)

        logger.info(
            "Load operation completed. Records written: %d, Partitions affected: %d",
            record_count,
            partition_count,
        )
        logger.info(
            "Affected partition dates: %s",
            ", ".join(sorted(partition_dates)),
        )

    except Exception as e:
        logger.exception(
            "Failed to write data to table %s.%s. Error: %s", database, table, str(e)
        )
        raise


def main() -> None:
    """
    Main entry point for the ETL pipeline.

    Orchestrates the extract, transform, and load operations for
    chatbot transaction data using modular transformation functions.
    """
    # Initialize Glue context and Spark session
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    logger.info("Starting Chatbot ETL Pipeline")

    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "source_table",
                "target_database",
                "target_table",
                "user_role_database",
                "user_role_table",
            ],
        )

        # Check sys.argv presence before resolving — getResolvedOptions has no
        # built-in optional param support
        data_dt_str = (
            getResolvedOptions(sys.argv, ["data_date"])["data_date"]
            if "--data_date" in sys.argv
            else None
        )
        start_date_str = (
            getResolvedOptions(sys.argv, ["start_data_date"])["start_data_date"]
            if "--start_data_date" in sys.argv
            else None
        )
        end_date_str = (
            getResolvedOptions(sys.argv, ["end_data_date"])["end_data_date"]
            if "--end_data_date" in sys.argv
            else None
        )

        # Validate mutual exclusivity: data_date vs start_data_date/end_data_date
        has_data_dt = data_dt_str is not None
        has_date_range = start_date_str is not None or end_date_str is not None

        if has_data_dt and has_date_range:
            raise ValueError(
                "Cannot provide both 'data_date' and 'start_data_date'/'end_data_date'. "
                "Use either 'data_date' for single-day processing or "
                "'start_data_date'/'end_data_date' for a date range."
            )

        if not has_data_dt and not has_date_range:
            raise ValueError(
                "Must provide either 'data_date' or both "
                "'start_data_date' and 'end_data_date'."
            )

        # If data_date provided, derive start/end dates from it
        if has_data_dt:
            logger.info("Using data_date mode: %s", data_dt_str)
            start_date_str = data_dt_str
            end_date_str = data_dt_str

        # If date range mode, both start and end must be present
        if not has_data_dt and (start_date_str is None or end_date_str is None):
            raise ValueError(
                "Both 'start_data_date' and 'end_data_date' are required "
                "when using date range mode."
            )

        source_table = args["source_table"]
        target_database = args["target_database"]
        target_table = args["target_table"]
        user_role_database = args["user_role_database"]
        user_role_table = args["user_role_table"]

        logger.info(
            "Job parameters - start_data_date: %s, end_data_date: %s, source_table: %s",
            start_date_str,
            end_date_str,
            source_table,
        )
        logger.info(
            "Target parameters - database: %s, table: %s",
            target_database,
            target_table,
        )

        # Validate date parameters
        start_date, end_date = validate_date_parameters(start_date_str, end_date_str)

        spark.conf.set("spark.sql.session.timeZone", "Asia/Bangkok")

        # Enable dynamic partition overwrite (only affected partitions are overwritten)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        logger.info("Glue context and Spark session initialized successfully")
        logger.info("Spark version: %s", spark.version)

        # Step 1: Extract data from DynamoDB
        logger.info("Step 1: Extracting data from DynamoDB")
        df_source = extract_true_bnic_chat_history(
            glue_context, start_date, end_date, source_table
        )

        df_source.cache()

        # Step 2: Transform Core Session Fields
        logger.info("Step 2: Transforming Core Session Fields")
        df_with_session_id = transform_get_session_id(df_source)

        # Log input and filtered counts
        input_count = df_source.count()
        session_count = df_with_session_id.count()
        null_prompts_count = input_count - session_count
        if null_prompts_count > 0:
            logger.warning(
                "Skipped %d records with null or empty prompts array",
                null_prompts_count,
            )

        # Step 3: Transform Prompt-Level Fields
        logger.info("Step 3: Transforming Prompt-Level Fields")
        df_with_prompts = transform_explode_prompt_and_response(df_with_session_id)

        # Log assistant filtering results
        exploded_count = df_with_prompts.count()
        logger.info(
            "Found %d assistant prompt records after exploding and filtering",
            exploded_count,
        )

        # Step 4: Transform User Context Fields
        logger.info("Step 4: Transforming User Context Fields")
        df_with_emp_id = transform_get_emp_id(df_with_prompts)

        # Step 5: Transform Tools Response Fields
        logger.info("Step 5: Transforming Tools Response Fields")
        df_with_tools = transform_tools_invoked(df_with_emp_id)

        # Step 6: Mark Internal Team Transactions
        logger.info("Step 6: Marking Internal Team Transactions")
        df_with_internal_flag = transform_mark_internal_team_flag(df_with_tools)

        # Step 7: Transform Execution Logs Fields
        logger.info("Step 7: Transforming Execution Logs Fields")
        df_with_exec_log = transform_execution_log(df_with_internal_flag)

        # Step 8: Transform External Reference Data (Join with user role data)
        logger.info("Step 8: Transforming External Reference Data")
        df_with_bu = transform_join_chat_txn_x_user_role(
            df_with_exec_log, spark, user_role_database, user_role_table
        )

        # Step 9: Finalize transaction DataFrame
        logger.info("Step 9: Finalizing transaction DataFrame")
        df_final = finalize_transaction_dataframe(df_with_bu)

        # Log final transformation metrics (crucial for pipeline monitoring)
        final_count = df_final.count()
        logger.info("Transformation completed successfully")
        logger.info("Final output records: %d", final_count)

        # Step 10: Load enriched data to Glue table as partitioned Parquet
        logger.info("Step 10: Loading data to target table")
        load_tbl_user_chat_transactions(df_final, spark, target_database, target_table)

        logger.info("Chatbot ETL Pipeline completed successfully")

    except Exception as e:
        logger.exception("ETL Pipeline failed with error")
        sys.exit(f"ETL Pipeline failed: {str(e)}")


if __name__ == "__main__":
    main()