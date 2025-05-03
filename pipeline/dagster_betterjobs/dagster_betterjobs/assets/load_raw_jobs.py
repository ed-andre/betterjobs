import os
import pandas as pd
from pathlib import Path
import json
import time
from typing import List, Dict, Any

from dagster import asset, AssetExecutionContext, get_dagster_logger, Output, MetadataValue
from ..utils.id_generator import generate_job_id
from google.cloud import bigquery

logger = get_dagster_logger()

@asset(
    group_name="raw_data_loading",
    kinds={"python", "bigquery"},
    io_manager_key="bigquery_io",
    deps=["initialize_db"],
    required_resource_keys={"bigquery"}
)
def raw_job_listings(context: AssetExecutionContext) -> None:
    """
    Load raw job listings from the companies_jobs.csv file into a staging table,
    then transfer to the raw_job_listings table with proper ID generation.

    Returns None instead of a DataFrame to avoid type conversion issues with the IO manager.
    """
    # Get data file path
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        data_file = cwd / "dagster_betterjobs" / "data_load" / "datasource" / "companies_jobs.csv"
    else:
        data_file = Path("pipeline/dagster_betterjobs/dagster_betterjobs/data_load/datasource/companies_jobs.csv")

    context.log.info(f"Loading job data from: {data_file}")

    if not data_file.exists():
        context.log.error(f"Data file not found: {data_file}")
        return None

    # Load the CSV file
    try:
        df_jobs = pd.read_csv(data_file)
        total_jobs = len(df_jobs)
        context.log.info(f"Loaded {total_jobs} job listings from CSV")
    except Exception as e:
        context.log.error(f"Error loading job data: {str(e)}")
        return None

    # Check for required columns
    required_columns = ['company_name', 'job_title', 'date_posted']
    missing_columns = [col for col in required_columns if col not in df_jobs.columns]
    if missing_columns:
        context.log.error(f"Missing required columns: {', '.join(missing_columns)}")
        return None

    # Prepare the dataframe for loading
    df_jobs = df_jobs[required_columns].copy()

    # Generate job IDs
    df_jobs['raw_job_id'] = df_jobs.apply(
        lambda row: generate_job_id(row['company_name'], row['job_title'], row['date_posted']),
        axis=1
    )

    # Add empty columns for job_url and processing status
    df_jobs['job_url'] = None
    df_jobs['job_url_verified'] = False
    df_jobs['processed'] = False
    df_jobs['date_retrieved'] = pd.Timestamp.now()

    # Convert None to empty string for string columns to avoid type mismatch
    df_jobs['job_url'] = df_jobs['job_url'].astype(str)

    # Create unique keys for deduplication
    df_jobs['job_key'] = df_jobs['company_name'] + '|' + df_jobs['job_title']

    # Create the main table in BigQuery if it doesn't exist
    client = context.resources.bigquery
    dataset_name = os.getenv("GCP_DATASET_ID")

    # Create the raw_job_listings table if it doesn't exist
    create_main_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {dataset_name}.raw_job_listings (
        raw_job_id STRING,
        company_name STRING NOT NULL,
        job_title STRING NOT NULL,
        date_posted STRING,
        job_url STRING,
        job_url_verified BOOL DEFAULT FALSE,
        processed BOOL DEFAULT FALSE,
        date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    query_job = client.query(create_main_table_sql)
    query_job.result()  # Wait for the query to complete
    context.log.info("Created or verified main table")

    # Create indexes (BigQuery doesn't support explicit indexes, but we'll create partitioning or clustering if needed in the future)

    # Check for existing records in the database to make rerunning idempotent
    try:
        # Get existing job keys
        existing_jobs_query = f"""
            SELECT company_name, job_title
            FROM {dataset_name}.raw_job_listings
        """
        query_job = client.query(existing_jobs_query)
        existing_jobs_df = query_job.to_dataframe()

        if not existing_jobs_df.empty:
            context.log.info(f"Found {len(existing_jobs_df)} existing job records")
            # Create matching keys for existing jobs
            existing_jobs_df['job_key'] = existing_jobs_df['company_name'] + '|' + existing_jobs_df['job_title']
            existing_keys = set(existing_jobs_df['job_key'])

            # Filter out jobs that already exist in the database
            original_count = len(df_jobs)
            df_jobs = df_jobs[~df_jobs['job_key'].isin(existing_keys)]
            context.log.info(f"Filtered out {original_count - len(df_jobs)} jobs that already exist in the database")
        else:
            context.log.info("No existing job records found in the database")
    except Exception as e:
        context.log.info(f"Error checking existing records: {str(e)}")
        context.log.info("Will proceed with loading all records")

    # Skip if there are no new jobs to load
    if df_jobs.empty:
        context.log.info("No new job records to load")

        # Add metadata to the output
        context.add_output_metadata({
            "total_jobs_in_csv": MetadataValue.int(total_jobs),
            "new_jobs": MetadataValue.int(0),
            "status": MetadataValue.text("success - no new jobs")
        })

        return None

    # Remove the temporary key column before loading
    df_jobs = df_jobs.drop(columns=['job_key'])

    # Load data into BigQuery using a more efficient approach
    try:
        context.log.info(f"Loading {len(df_jobs)} job records into BigQuery")

        # First, create a temporary table for all the data
        temp_table_name = f"{dataset_name}.raw_job_listings_temp"

        # Drop the temp table if it exists
        query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
        query_job.result()

        # Create the temporary table
        create_temp_sql = f"""
        CREATE TABLE {temp_table_name} (
            raw_job_id STRING,
            company_name STRING NOT NULL,
            job_title STRING NOT NULL,
            date_posted STRING,
            job_url STRING,
            job_url_verified BOOL DEFAULT FALSE,
            processed BOOL DEFAULT FALSE,
            date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        query_job = client.query(create_temp_sql)
        query_job.result()
        context.log.info("Created temporary table for bulk loading")

        # Load all data at once into the temporary table
        # Set up job configuration
        job_config = bigquery.LoadJobConfig(
            # Optionally, set the write disposition. To append data to an existing table use
            # WRITE_APPEND, to overwrite the data use WRITE_TRUNCATE
            write_disposition="WRITE_TRUNCATE",
            # Define explicit schema to ensure proper types
            schema=[
                bigquery.SchemaField("raw_job_id", "STRING"),
                bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("job_title", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date_posted", "STRING"),
                bigquery.SchemaField("job_url", "STRING"),
                bigquery.SchemaField("job_url_verified", "BOOL"),
                bigquery.SchemaField("processed", "BOOL"),
                bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
            ]
        )

        # Load the dataframe into the temporary table
        job = client.load_table_from_dataframe(
            df_jobs,
            temp_table_name,
            job_config=job_config
        )
        # Wait for the load job to complete
        job.result()
        context.log.info(f"Successfully loaded {len(df_jobs)} jobs into temporary table")

        # Create the main table if it doesn't exist
        create_main_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {dataset_name}.raw_job_listings (
            raw_job_id STRING,
            company_name STRING NOT NULL,
            job_title STRING NOT NULL,
            date_posted STRING,
            job_url STRING,
            job_url_verified BOOL DEFAULT FALSE,
            processed BOOL DEFAULT FALSE,
            date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        query_job = client.query(create_main_table_sql)
        query_job.result()
        context.log.info("Ensured main table exists")

        # Deduplicate and insert data in a single operation
        transfer_sql = f"""
        INSERT INTO {dataset_name}.raw_job_listings
            (raw_job_id, company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved)
        SELECT
            t.raw_job_id,
            t.company_name,
            t.job_title,
            t.date_posted,
            CAST(t.job_url AS STRING),
            CAST(t.job_url_verified AS BOOL),
            CAST(t.processed AS BOOL),
            t.date_retrieved
        FROM {temp_table_name} t
        LEFT JOIN {dataset_name}.raw_job_listings r
            ON t.raw_job_id = r.raw_job_id
        WHERE r.raw_job_id IS NULL
        """
        query_job = client.query(transfer_sql)
        query_job.result()
        context.log.info("Deduplicated and transferred data to main table")

        # Check how many records were inserted
        count_sql = f"SELECT COUNT(*) FROM {dataset_name}.raw_job_listings"
        query_job = client.query(count_sql)
        count_result = list(query_job.result())
        count = count_result[0][0]
        context.log.info(f"Total records in raw_job_listings table: {count}")

        # Clean up - drop the temporary table
        query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
        query_job.result()
        context.log.info("Dropped temporary table")

        # Add metadata to the output
        context.add_output_metadata({
            "total_jobs": MetadataValue.int(total_jobs),
            "loaded_jobs": MetadataValue.int(count),
            "status": MetadataValue.text("success")
        })

    except Exception as e:
        context.log.error(f"Error loading job data: {str(e)}")
        # Add error metadata
        context.add_output_metadata({
            "error": MetadataValue.text(str(e))
        })

    # Return None to avoid type conversion issues
    return None