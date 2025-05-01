import os
import pandas as pd
from pathlib import Path
import json
import time
from typing import List, Dict, Any

from dagster import asset, AssetExecutionContext, get_dagster_logger, Output, MetadataValue
from ..utils.id_generator import generate_job_id

logger = get_dagster_logger()

@asset(
    group_name="raw_data_loading",
    compute_kind="python",
    io_manager_key="duckdb",
    deps=["initialize_db"],
    required_resource_keys={"duckdb_resource"}
)
def raw_job_listings(context: AssetExecutionContext) -> None:
    """
    Load raw job listings from the companies_jobs.csv file into a staging table,
    then transfer to the raw_job_listings table in the public schema with proper ID generation.

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

    # Create unique keys for deduplication
    df_jobs['job_key'] = df_jobs['company_name'] + '|' + df_jobs['job_title']

    # Create the main table in public schema if it doesn't exist
    with context.resources.duckdb_resource.get_connection() as conn:
        # First ensure the public schema exists
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")

        create_main_table_sql = """
        CREATE TABLE IF NOT EXISTS public.raw_job_listings (
            raw_job_id VARCHAR PRIMARY KEY,
            company_name TEXT NOT NULL,
            job_title TEXT NOT NULL,
            date_posted TEXT,
            job_url TEXT,
            job_url_verified BOOLEAN DEFAULT FALSE,
            processed BOOLEAN DEFAULT FALSE,
            date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(create_main_table_sql)
        context.log.info("Created or verified main table")

        # Create indexes for the main table
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_public_raw_jobs_company
            ON public.raw_job_listings (company_name)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_public_raw_jobs_title
            ON public.raw_job_listings (job_title)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_public_raw_jobs_processed
            ON public.raw_job_listings (processed)
        """)

        # Check for existing records in the database to make rerunning idempotent
        try:
            # Get existing job keys
            existing_jobs_df = pd.read_sql("""
                SELECT company_name, job_title
                FROM public.raw_job_listings
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

    # Create a staging table for the data
    with context.resources.duckdb_resource.get_connection() as conn:
        # First, drop the staging table if it exists
        drop_staging_sql = "DROP TABLE IF EXISTS public.raw_job_listings_staging"
        conn.execute(drop_staging_sql)

        # Create a staging table without ID constraints
        create_staging_sql = """
        CREATE TABLE public.raw_job_listings_staging (
            raw_job_id VARCHAR,
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

                # Convert batch to list of tuples for DuckDB insertion
                records = batch_df.to_dict('records')
                placeholders = ','.join(['(?, ?, ?, ?, ?, ?, ?, ?)'] * len(records))
                values = []
                for record in records:
                    values.extend([
                        record['raw_job_id'],
                        record['company_name'],
                        record['job_title'],
                        record['date_posted'],
                        record['job_url'],
                        record['job_url_verified'],
                        record['processed'],
                        record['date_retrieved'].isoformat()
                    ])

                # Use DuckDB's native SQL interface
                insert_sql = f"""
                INSERT INTO public.raw_job_listings_staging
                    (raw_job_id, company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved)
                VALUES {placeholders}
                """
                conn.execute(insert_sql, values)

                context.log.info(f"Successfully loaded batch {batch_num+1} into staging table")

            context.log.info(f"Completed loading {len(df_jobs)} jobs into staging table")

            # Deduplicate records in staging table before transfer
            dedup_sql = """
            CREATE TABLE public.raw_job_listings_dedup AS
            SELECT DISTINCT ON (raw_job_id)
                raw_job_id,
                company_name,
                job_title,
                date_posted,
                job_url,
                job_url_verified,
                processed,
                date_retrieved
            FROM public.raw_job_listings_staging
            ORDER BY raw_job_id, date_retrieved DESC
            """
            conn.execute(dedup_sql)
            context.log.info("Created deduplicated staging table")

            # Transfer data from deduplicated staging to main table
            transfer_sql = """
            INSERT INTO public.raw_job_listings
                (raw_job_id, company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved)
            SELECT
                raw_job_id, company_name, job_title, date_posted, job_url, job_url_verified, processed, date_retrieved
            FROM public.raw_job_listings_dedup
            """
            conn.execute(transfer_sql)
            context.log.info("Transferred deduplicated data to main table")

            # Check how many records were inserted
            count_sql = "SELECT COUNT(*) FROM public.raw_job_listings"
            count = conn.execute(count_sql).fetchone()[0]
            context.log.info(f"Total records in raw_job_listings table: {count}")

            # Clean up - drop the staging tables
            conn.execute("DROP TABLE IF EXISTS public.raw_job_listings_staging")
            conn.execute("DROP TABLE IF EXISTS public.raw_job_listings_dedup")
            context.log.info("Dropped staging tables")

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