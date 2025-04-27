import os
import pandas as pd
from pathlib import Path
import json
import time
from typing import List, Dict, Any

from dagster import asset, AssetExecutionContext, get_dagster_logger, Output, MetadataValue

logger = get_dagster_logger()

@asset(
    group_name="data_loading",
    compute_kind="python",
    io_manager_key="duckdb",
    deps=["initialize_db"],
    required_resource_keys={"duckdb_resource"}
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

    # Add empty columns for job_url and processing status
    df_jobs['job_url'] = None
    df_jobs['job_url_verified'] = False
    df_jobs['processed'] = False

    # Create unique keys for deduplication
    df_jobs['job_key'] = df_jobs['company_name'] + '|' + df_jobs['job_title']

    # Check for existing records in the database to make rerunning idempotent
    with context.resources.duckdb_resource.get_connection() as conn:
        # Check if the table exists
        table_exists = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='raw_job_listings'
        """).fetchone()

        if table_exists:
            # Get existing job keys
            existing_jobs_df = pd.read_sql("""
                SELECT company_name, job_title
                FROM raw_job_listings
            """, conn)

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
        else:
            context.log.info("Table raw_job_listings does not exist yet")

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

    # Create a staging table for the data
    with context.resources.duckdb_resource.get_connection() as conn:
        # First, drop the staging table if it exists
        drop_staging_sql = "DROP TABLE IF EXISTS raw_job_listings_staging"
        conn.execute(drop_staging_sql)

        # Create a staging table without ID constraints
        create_staging_sql = """
        CREATE TABLE raw_job_listings_staging (
            company_name TEXT NOT NULL,
            job_title TEXT NOT NULL,
            date_posted TEXT,
            job_url TEXT,
            job_url_verified BOOLEAN DEFAULT FALSE,
            processed BOOLEAN DEFAULT FALSE,
            date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(create_staging_sql)
        context.log.info("Created staging table for job listings")

        # Load data into staging table in batches
        try:
            # Process in batches for better performance and reliability
            batch_size = 10000  # 10K records at a time
            total_batches = (len(df_jobs) + batch_size - 1) // batch_size

            for batch_num, batch_start in enumerate(range(0, len(df_jobs), batch_size)):
                batch_end = min(batch_start + batch_size, len(df_jobs))
                batch_df = df_jobs.iloc[batch_start:batch_end]

                context.log.info(f"Loading batch {batch_num+1}/{total_batches} ({len(batch_df)} jobs) into staging table")

                # Use pandas to_sql for the staging table (which has no constraints)
                batch_df.to_sql(
                    'raw_job_listings_staging',
                    conn,
                    if_exists='append',
                    index=False
                )

                context.log.info(f"Successfully loaded batch {batch_num+1} into staging table")

            context.log.info(f"Completed loading {len(df_jobs)} jobs into staging table")

            # Debug: Check the actual schema of both tables
            raw_listings_schema = pd.read_sql("PRAGMA table_info(raw_job_listings)", conn)
            staging_schema = pd.read_sql("PRAGMA table_info(raw_job_listings_staging)", conn)

            context.log.info(f"Raw job listings schema:\n{raw_listings_schema}")
            context.log.info(f"Staging table schema:\n{staging_schema}")

            # Create a sequence for ID generation if it doesn't exist already
            try:
                conn.execute("CREATE SEQUENCE IF NOT EXISTS job_id_seq START 1")
                context.log.info("Created or verified job ID sequence")
            except Exception as e:
                context.log.error(f"Error creating sequence: {str(e)}")
                # Continue anyway as we have an alternative approach

            # Try using the sequence for ID generation first
            try:
                # Now transfer from staging to the main table with ID generation from sequence
                transfer_sql = """
                INSERT INTO raw_job_listings
                    (raw_job_id, company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved)
                SELECT
                    nextval('job_id_seq'), company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved
                FROM raw_job_listings_staging
                """

                conn.execute(transfer_sql)
                context.log.info("Transferred data using sequence for ID generation")
            except Exception as e:
                context.log.error(f"Sequence-based transfer failed: {str(e)}")

                # Alternative approach - create a temp table with proper schema including SQLite-style AUTOINCREMENT
                context.log.info("Trying alternative approach with direct SQLite AUTOINCREMENT")

                # Drop temp table if it exists
                conn.execute("DROP TABLE IF EXISTS temp_raw_job_listings")

                # Create temp table with proper AUTOINCREMENT
                conn.execute("""
                CREATE TABLE temp_raw_job_listings (
                    raw_job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL,
                    job_title TEXT NOT NULL,
                    date_posted TEXT,
                    job_url TEXT,
                    job_url_verified BOOLEAN DEFAULT FALSE,
                    processed BOOLEAN DEFAULT FALSE,
                    date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)

                # Copy data from staging to temp
                conn.execute("""
                INSERT INTO temp_raw_job_listings
                    (company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved)
                SELECT
                    company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved
                FROM raw_job_listings_staging
                """)

                # Drop the original table
                conn.execute("DROP TABLE IF EXISTS raw_job_listings")

                # Rename temp to original
                conn.execute("ALTER TABLE temp_raw_job_listings RENAME TO raw_job_listings")

                context.log.info("Rebuilt table with proper AUTOINCREMENT and transferred data")

            # Check how many records were inserted
            count_sql = "SELECT COUNT(*) FROM raw_job_listings"
            count = conn.execute(count_sql).fetchone()[0]
            context.log.info(f"Total records in raw_job_listings table: {count}")

            # Clean up - drop the staging table
            conn.execute(drop_staging_sql)
            context.log.info("Dropped staging table")

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