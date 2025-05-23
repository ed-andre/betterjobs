import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Callable

from dagster import (
    asset, AssetExecutionContext, Config, get_dagster_logger,
    MetadataValue
)

from dagster_betterjobs.resources import PostgresResource

logger = get_dagster_logger()

class SupabaseTransportConfig(Config):
    """Configuration parameters for transporting job data to Supabase."""
    batch_size: int = 250
    days_to_look_back: int = 30  # Job freshness threshold in days
    max_jobs: int = None  # Optional limit on processed jobs
    recreate_indexes: bool = False  # Whether to recreate indices

def transport_jobs_to_supabase(
    context: AssetExecutionContext,
    config: SupabaseTransportConfig,
    platform: str,
    query: str,
    date_posted_field: str = "date_posted"
) -> Dict:
    """
    Generic function to transport job listings from BigQuery to Supabase PostgreSQL.

    Args:
        context: The Dagster execution context
        config: Configuration for the transport process
        platform: The ATS platform (e.g., 'bamboohr', 'greenhouse')
        query: SQL query to fetch jobs from BigQuery
        date_posted_field: Name of the field containing the posting date

    Returns:
        Dict with stats about the operation
    """
    # Initialize clients
    bq_client = context.resources.bigquery
    supabase_postgres = context.resources.supabase_postgres

    # Schema is managed by Prisma, but we'll ensure indexes exist for performance
    with supabase_postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create or verify text search indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS jobs_title_search_idx ON "Job" USING GIN (to_tsvector('english', job_title));
                CREATE INDEX IF NOT EXISTS jobs_description_search_idx ON "Job" USING GIN (to_tsvector('english', job_description));
            """)
            conn.commit()
            context.log.info("Verified search indexes on Job table in Supabase")

    try:
        # Execute query and fetch results
        query_job = bq_client.query(query)
        jobs_df = query_job.to_dataframe()

        total_jobs = len(jobs_df)
        context.log.info(f"Retrieved {total_jobs} {platform} jobs from BigQuery")

        if total_jobs == 0:
            return {
                "status": "success",
                "jobs_processed": 0,
                "jobs_inserted": 0,
                "jobs_updated": 0
            }

        # Pre-process dataframe to handle NaT values and other potential issues
        # Replace NaT with None for proper SQL NULL handling
        datetime_columns = ['date_posted', 'date_retrieved', 'published_at']
        for col in datetime_columns:
            if col in jobs_df.columns:
                jobs_df[col] = jobs_df[col].astype(object).where(~pd.isna(jobs_df[col]), None)

        context.log.info(f"Preprocessed dataframe to handle NaT values in datetime columns")

        # Process jobs in batches
        jobs_inserted = 0
        jobs_updated = 0
        batch_size = config.batch_size

        with supabase_postgres.get_connection() as conn:
            # Pre-fetch existing job IDs to avoid duplicate queries and incorrect counting
            existing_job_ids = {}
            with conn.cursor() as cursor:
                cursor.execute('SELECT id, job_id, company_id FROM "Job" WHERE platform = %s', (platform,))
                for row in cursor.fetchall():
                    # Use composite key of job_id + company_id
                    existing_job_ids[(row[1], row[2])] = row[0]  # (job_id, company_id) -> id mapping

            context.log.info(f"Found {len(existing_job_ids)} existing {platform} jobs in database")

            for i in range(0, total_jobs, batch_size):
                batch = jobs_df.iloc[i:i+batch_size]
                current_batch_size = len(batch)

                # Process each job in batch
                with conn.cursor() as cursor:
                    for _, job in batch.iterrows():
                        # Check if job already exists using our pre-fetched map
                        job_id = job['job_id']
                        company_id = job['company_id']
                        composite_key = (job_id, company_id)
                        if composite_key in existing_job_ids:
                            # Update existing job
                            cursor.execute("""
                                UPDATE "Job" SET
                                    job_title = %s,
                                    job_description = %s,
                                    location = %s,
                                    department = %s,
                                    date_posted = %s,
                                    date_retrieved = %s,
                                    is_active = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE job_id = %s AND company_id = %s
                            """, (
                                job['job_title'],
                                job['job_description'],
                                job['location'],
                                job['department'],
                                job['date_posted'],
                                job['date_retrieved'],
                                job['is_active'],
                                job['job_id'],
                                company_id
                            ))
                            jobs_updated += 1
                        else:
                            # Insert new job with ON CONFLICT handling
                            cursor.execute("""
                                INSERT INTO "Job" (
                                    job_id, company_id, company_name, platform, job_title,
                                    job_description, job_url, location, department,
                                    date_posted, date_retrieved, is_active, created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                ON CONFLICT (job_id, company_id) DO UPDATE SET
                                    job_title = EXCLUDED.job_title,
                                    job_description = EXCLUDED.job_description,
                                    location = EXCLUDED.location,
                                    department = EXCLUDED.department,
                                    date_posted = EXCLUDED.date_posted,
                                    date_retrieved = EXCLUDED.date_retrieved,
                                    is_active = EXCLUDED.is_active,
                                    updated_at = CURRENT_TIMESTAMP
                            """, (
                                job['job_id'],
                                company_id,
                                job['company_name'],
                                platform,
                                job['job_title'],
                                job['job_description'],
                                job['job_url'],
                                job['location'],
                                job['department'],
                                job['date_posted'],
                                job['date_retrieved'],
                                job['is_active']
                            ))
                            jobs_inserted += 1

                conn.commit()
                context.log.info(f"Processed batch of {current_batch_size} jobs: {jobs_inserted} inserted, {jobs_updated} updated")

        # Add metadata to the output
        context.add_output_metadata({
            "total_jobs": MetadataValue.int(total_jobs),
            "jobs_inserted": MetadataValue.int(jobs_inserted),
            "jobs_updated": MetadataValue.int(jobs_updated),
            "platform": MetadataValue.text(platform),
            "supabase_table": MetadataValue.text("Job")
        })

        return {
            "status": "success",
            "jobs_processed": total_jobs,
            "jobs_inserted": jobs_inserted,
            "jobs_updated": jobs_updated
        }

    except Exception as e:
        context.log.error(f"Error transporting {platform} jobs to Supabase: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

@asset(
    group_name="supabase_postgres_transport",
    kinds={"postgres", "supabase"},
    required_resource_keys={"bigquery", "supabase_postgres"},
    deps=["bamboohr_company_jobs_discovery"],
)
def bamboohr_jobs_to_supabase(
    context: AssetExecutionContext,
    config: SupabaseTransportConfig,
) -> Dict:
    """
    Transports BambooHR job listings from BigQuery to Supabase PostgreSQL.

    Retrieves job listings from BigQuery, transforms them into a unified schema,
    and loads them into the Supabase PostgreSQL database.

    Note: Table schema is defined in schema.prisma and managed through Prisma migrations.
    Job model fields:
    - id: Int (auto-increment primary key)
    - job_id: String
    - company_id: String
    - company_name: String?
    - platform: String
    - job_title: String
    - job_description: String?
    - job_url: String
    - location: String?
    - department: String?
    - date_posted: DateTime?
    - date_retrieved: DateTime?
    - is_active: Boolean (default true)
    - created_at: DateTime (default now)
    - updated_at: DateTime (auto-updated)
    - Unique constraint on [job_id, platform]
    - Index on job_url
    """
    # Build query for BambooHR jobs
    dataset_name = os.getenv("GCP_DATASET_ID")
    query = f"""
    SELECT
        j.job_id,
        j.company_id,
        c.company_name,
        'bamboohr' as platform,
        j.job_title,
        j.job_description,
        j.job_url,
        j.location,
        j.department,
        j.date_posted,
        j.date_retrieved,
        j.is_active
    FROM
        {dataset_name}.bamboohr_jobs j
    JOIN
        {dataset_name}.master_company_urls c ON j.company_id = c.company_id
    WHERE
        j.is_active = TRUE
    """

    # Apply limit if specified
    if config.max_jobs:
        query += f" LIMIT {config.max_jobs}"

    return transport_jobs_to_supabase(
        context=context,
        config=config,
        platform='bamboohr',
        query=query,
        date_posted_field='date_posted'
    )

@asset(
    group_name="supabase_postgres_transport",
    kinds={"postgres", "supabase"},
    required_resource_keys={"bigquery", "supabase_postgres"},
    deps=["greenhouse_company_jobs_discovery"],
)
def greenhouse_jobs_to_supabase(
    context: AssetExecutionContext,
    config: SupabaseTransportConfig,
) -> Dict:
    """
    Transports Greenhouse job listings from BigQuery to Supabase PostgreSQL.

    Retrieves job listings from BigQuery, transforms them into a unified schema,
    and loads them into the Supabase PostgreSQL database.

    Note: Table schema is defined in schema.prisma and managed through Prisma migrations.
    Job model fields:
    - id: Int (auto-increment primary key)
    - job_id: String
    - company_id: String
    - company_name: String?
    - platform: String
    - job_title: String
    - job_description: String?
    - job_url: String
    - location: String?
    - department: String?
    - date_posted: DateTime?
    - date_retrieved: DateTime?
    - is_active: Boolean (default true)
    - created_at: DateTime (default now)
    - updated_at: DateTime (auto-updated)
    - Unique constraint on [job_id, platform]
    - Index on job_url
    """
    # Build query for Greenhouse jobs
    dataset_name = os.getenv("GCP_DATASET_ID")
    query = f"""
    SELECT
        j.job_id,
        j.company_id,
        c.company_name,
        'greenhouse' as platform,
        j.job_title,
        j.job_description,
        j.job_url,
        j.location,
        j.department,
        j.published_at as date_posted,
        j.date_retrieved,
        j.is_active
    FROM
        {dataset_name}.greenhouse_jobs j
    JOIN
        {dataset_name}.master_company_urls c ON j.company_id = c.company_id
    WHERE
        j.is_active = TRUE
    """

    # Apply limit if specified
    if config.max_jobs:
        query += f" LIMIT {config.max_jobs}"

    return transport_jobs_to_supabase(
        context=context,
        config=config,
        platform='greenhouse',
        query=query,
        date_posted_field='date_posted'
    )

@asset(
    group_name="supabase_postgres_transport",
    kinds={"postgres", "supabase"},
    required_resource_keys={"bigquery", "supabase_postgres"},
    deps=["smartrecruiters_company_jobs_discovery"],
)
def smartrecruiters_jobs_to_supabase(
    context: AssetExecutionContext,
    config: SupabaseTransportConfig,
) -> Dict:
    """
    Transports SmartRecruiters job listings from BigQuery to Supabase PostgreSQL.

    Retrieves job listings from BigQuery, transforms them into a unified schema,
    and loads them into the Supabase PostgreSQL database.

    Note: Table schema is defined in schema.prisma and managed through Prisma migrations.
    """
    # Build query for SmartRecruiters jobs
    dataset_name = os.getenv("GCP_DATASET_ID")
    query = f"""
    SELECT
        j.job_id,
        j.company_id,
        c.company_name,
        'smartrecruiters' as platform,
        j.job_title,
        j.job_description,
        j.job_url,
        j.location,
        j.department,
        j.published_at as date_posted,
        j.date_retrieved,
        j.is_active
    FROM
        {dataset_name}.smartrecruiters_jobs j
    JOIN
        {dataset_name}.master_company_urls c ON j.company_id = c.company_id
    WHERE
        j.is_active = TRUE
    """

    # Apply limit if specified
    if config.max_jobs:
        query += f" LIMIT {config.max_jobs}"

    return transport_jobs_to_supabase(
        context=context,
        config=config,
        platform='smartrecruiters',
        query=query,
        date_posted_field='date_posted'
    )

@asset(
    group_name="supabase_postgres_transport",
    kinds={"postgres", "supabase"},
    required_resource_keys={"bigquery", "supabase_postgres"},
    deps=["workday_company_jobs_discovery"],
)
def workday_jobs_to_supabase(
    context: AssetExecutionContext,
    config: SupabaseTransportConfig,
) -> Dict:
    """
    Transports Workday job listings from BigQuery to Supabase PostgreSQL.

    Retrieves job listings from BigQuery, transforms them into a unified schema,
    and loads them into the Supabase PostgreSQL database.

    Note: Table schema is defined in schema.prisma and managed through Prisma migrations.
    """
    # Build query for Workday jobs
    dataset_name = os.getenv("GCP_DATASET_ID")
    query = f"""
    SELECT
        j.job_id,
        j.company_id,
        c.company_name,
        'workday' as platform,
        j.job_title,
        j.job_description,
        j.job_url,
        j.location,
        j.employment_type as department, -- Map employment_type to department
        j.published_at as date_posted,
        j.date_retrieved,
        j.is_active
    FROM
        {dataset_name}.workday_jobs j
    JOIN
        {dataset_name}.master_company_urls c ON j.company_id = c.company_id
    WHERE
        j.is_active = TRUE
    """

    # Apply limit if specified
    if config.max_jobs:
        query += f" LIMIT {config.max_jobs}"

    return transport_jobs_to_supabase(
        context=context,
        config=config,
        platform='workday',
        query=query,
        date_posted_field='date_posted'
    )