import time
import json
import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from google.cloud import bigquery
from dagster import (
    asset, AssetExecutionContext, Config, get_dagster_logger,
    MetadataValue, AssetMaterialization, StaticPartitionsDefinition,
    Definitions, define_asset_job
)

from dagster_betterjobs.scrapers.workday_scraper import WorkdayScraper

logger = get_dagster_logger()

# Partition companies alphabetically A-Z + numeric + other
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

class WorkdayJobsDiscoveryConfig(Config):
    """Configuration parameters for Workday job discovery."""
    max_companies: Optional[int] = None  # Optional limit on processed companies
    rate_limit: float = 2.0  # Seconds between requests
    max_retries: int = 3
    retry_delay: int = 2
    min_company_id: Optional[int] = None  # For batch processing
    max_company_id: Optional[int] = None  # For batch processing
    days_to_look_back: int = 14  # Job freshness threshold in days
    batch_size: int = 10  # Companies per batch before committing
    skip_detailed_fetch: bool = False  # Set to True to skip detailed job info
    process_all_companies: bool = True  # By default, process all companies regardless of checkpoint status

@asset(
    group_name="job_discovery",
    kinds={"API", "bigquery", "python"},
    required_resource_keys={"bigquery"},
    deps=["master_company_urls"],
    partitions_def=alpha_partitions
)
def workday_company_jobs_discovery(context: AssetExecutionContext, config: WorkdayJobsDiscoveryConfig) -> Dict:
    """
    Discovers and stores job listings from Workday career sites.

    Processes companies partitioned by first letter of company name,
    retrieves all current job listings, and stores them in BigQuery.
    """
    # Initialize BigQuery client and get dataset
    client = context.resources.bigquery
    dataset_name = os.getenv("GCP_DATASET_ID")
    partition_key = context.partition_key

    # Set job freshness cutoff date
    cutoff_date = datetime.now() - timedelta(days=config.days_to_look_back)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")

    context.log.info(f"Processing company name partition {partition_key}")
    context.log.info(f"Only processing jobs posted after {cutoff_str}")

    # Resolve checkpoint directory path
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Configure partition-specific checkpoint files
    checkpoint_file = checkpoint_dir / f"workday_jobs_discovery_{partition_key}_checkpoint.csv"
    failed_companies_file = checkpoint_dir / f"workday_jobs_discovery_{partition_key}_failed.csv"

    context.log.info(f"Using checkpoint file: {checkpoint_file}")

    # Build query for companies in current partition
    if partition_key == "0-9":
        letter_filter = "AND REGEXP_CONTAINS(company_name, '^[0-9]')"
    elif partition_key == "other":
        letter_filter = "AND NOT REGEXP_CONTAINS(company_name, '^[a-zA-Z0-9]')"
    else:
        letter_filter = f"AND REGEXP_CONTAINS(company_name, '^[{partition_key}{partition_key.lower()}]')"

    query = f"""
    SELECT company_id, company_name, company_industry, career_url, ats_url
    FROM {dataset_name}.master_company_urls
    WHERE platform = 'workday'
    AND (ats_url IS NOT NULL OR career_url IS NOT NULL)
    AND url_verified = TRUE
    {letter_filter}
    """

    # Apply additional filters if specified
    if config.min_company_id is not None:
        query += f" AND company_id >= '{config.min_company_id}'"
    if config.max_company_id is not None:
        query += f" AND company_id <= '{config.max_company_id}'"

    query += " ORDER BY company_id"

    # Apply limit if specified in config
    if config.max_companies:
        query += f" LIMIT {config.max_companies}"

    try:
        query_job = client.query(query)
        companies_df = query_job.to_dataframe()
    except Exception as e:
        context.log.error(f"Error querying master_company_urls: {str(e)}")
        return {"error": str(e), "status": "failed"}

    total_companies = len(companies_df)
    context.log.info(f"Found {total_companies} Workday companies in partition {partition_key} to process")

    # Create workday_jobs table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {dataset_name}.workday_jobs (
        job_id STRING,
        company_id STRING,
        job_title STRING,
        job_description STRING,
        job_url STRING,
        location STRING,
        time_type STRING,
        employment_type STRING,
        published_at DATE,
        valid_through DATE,
        date_retrieved TIMESTAMP,
        is_active BOOL,
        raw_data STRING,
        partition_key STRING,
        work_type STRING,
        compensation STRING
    )
    """

    try:
        query_job = client.query(create_table_sql)
        query_job.result()
        context.log.info("Created or verified workday_jobs table in BigQuery")
    except Exception as e:
        context.log.error(f"Error creating jobs table: {str(e)}")
        return {"error": str(e), "status": "failed"}

    # Track processing statistics
    stats = {
        "total_companies": total_companies,
        "companies_processed": 0,
        "companies_with_jobs": 0,
        "total_jobs_found": 0,
        "new_jobs_added": 0,
        "updated_jobs": 0,
        "recent_jobs": 0,
        "old_jobs_skipped": 0,
        "errors": 0,
        "partition_key": partition_key
    }

    # Resume from checkpoint if exists
    processed_company_ids = set()
    checkpoint_results = []
    failed_companies = []

    if checkpoint_file.exists() and not config.process_all_companies:
        try:
            checkpoint_df = pd.read_csv(checkpoint_file)
            processed_company_ids = set(checkpoint_df["company_id"].astype(str).tolist())
            checkpoint_results = checkpoint_df.to_dict("records")
            context.log.info(f"Loaded {len(processed_company_ids)} previously processed companies from checkpoint")
        except Exception as e:
            context.log.error(f"Error loading checkpoint file: {str(e)}")

    # Load previously failed companies if file exists
    if failed_companies_file.exists():
        try:
            failed_df = pd.read_csv(failed_companies_file)
            failed_companies = failed_df.to_dict("records")
            context.log.info(f"Loaded {len(failed_companies)} previously failed companies")
        except Exception as e:
            context.log.error(f"Error loading failed companies file: {str(e)}")

    # Filter out already processed companies
    if not config.process_all_companies:
        companies_to_process = companies_df[~companies_df["company_id"].astype(str).isin(processed_company_ids)]
        context.log.info(f"{len(companies_to_process)} companies remaining to process")
    else:
        companies_to_process = companies_df
        context.log.info(f"Processing all {len(companies_to_process)} companies (ignoring checkpoint)")

    # Get batch size from config
    batch_size = config.batch_size
    new_failed_companies = []

    # Process companies in batches
    for i in range(0, len(companies_to_process), batch_size):
        batch = companies_to_process.iloc[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(companies_to_process) + batch_size - 1) // batch_size
        context.log.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)")

        # List to track jobs from this batch
        batch_jobs = []

        # Process each company in batch
        for _, company in batch.iterrows():
            company_id = str(company["company_id"])
            company_name = company["company_name"]
            career_url = company["career_url"]
            ats_url = company["ats_url"]

            context.log.info(f"Processing jobs for {company_name} (ID: {company_id})")

            # Use ATS URL if available, otherwise fall back to career URL
            url_to_use = ats_url if ats_url else career_url
            context.log.info(f"Using URL: {url_to_use}")

            # Track company results for checkpoint
            company_result = {
                "company_id": company_id,
                "company_name": company_name,
                "processed_at": datetime.now().isoformat(),
                "jobs_found": 0,
                "jobs_added": 0,
                "jobs_updated": 0,
                "status": "success",
                "partition_key": partition_key
            }

            try:
                # Create Workday scraper
                scraper = WorkdayScraper(
                    career_url=url_to_use,
                    rate_limit=config.rate_limit,
                    max_retries=config.max_retries,
                    retry_delay=config.retry_delay,
                    dagster_log=context.log  # Pass Dagster logger to the scraper
                )

                # Get all job listings
                job_listings = scraper.search_jobs()

                if not job_listings:
                    context.log.info(f"No jobs found for {company_name}")
                    checkpoint_results.append(company_result)
                    continue

                context.log.info(f"Found {len(job_listings)} jobs for {company_name}")
                company_result["jobs_found"] = len(job_listings)
                stats["companies_with_jobs"] += 1
                stats["total_jobs_found"] += len(job_listings)

                # Track jobs found for this company
                company_jobs_added = 0
                company_jobs_updated = 0

                # Check which jobs already exist in BigQuery
                if len(job_listings) > 0:
                    job_ids = [job.get("job_id") for job in job_listings if job.get("job_id")]
                    # Remove None and empty values
                    job_ids = [job_id for job_id in job_ids if job_id]

                    existing_jobs = {}
                    if job_ids:
                        # Format IDs for the query with proper quoting
                        job_ids_str = ", ".join([f"'{job_id}'" for job_id in job_ids])
                        existing_query = f"""
                        SELECT job_id, is_active, date_retrieved
                        FROM {dataset_name}.workday_jobs
                        WHERE company_id = '{company_id}'
                        AND job_id IN ({job_ids_str})
                        """

                        try:
                            existing_job_rows = client.query(existing_query).result()
                            for row in existing_job_rows:
                                existing_jobs[row.job_id] = {
                                    "is_active": row.is_active,
                                    "date_retrieved": row.date_retrieved
                                }
                            context.log.info(f"Found {len(existing_jobs)} existing jobs for {company_name}")
                        except Exception as e:
                            context.log.error(f"Error querying existing jobs: {str(e)}")

                # Process each job listing
                for job in job_listings:
                    # Extract job data
                    job_id = job.get("job_id")
                    job_title = job.get("job_title", "")
                    job_url = job.get("job_url", "")
                    location = job.get("location", "")
                    time_type = job.get("time_type", "")
                    posted_on = job.get("posted_on", "")
                    raw_data = job.get("raw_data", "{}")

                    # Skip jobs without ID
                    if not job_id:
                        context.log.warning(f"Skipping job without ID: {job_title}")
                        continue

                    # Check if job already exists
                    existing_job = existing_jobs.get(job_id)

                    # Get detailed job info if needed
                    job_description = ""
                    employment_type = ""
                    date_posted = ""
                    valid_through = ""
                    work_type = job.get("work_type", "")
                    compensation = ""

                    if not config.skip_detailed_fetch:
                        try:
                            context.log.info(f"Fetching details for job: {job_title}")
                            job_details = scraper.get_job_details(job_url)

                            # Extract additional details
                            job_description = job_details.get("job_description", "")
                            date_posted = job_details.get("date_posted", "")
                            valid_through = job_details.get("valid_through", "")
                            employment_type = job_details.get("employment_type", "")

                            # Get work_type from details if available
                            if job_details.get("work_type"):
                                work_type = job_details.get("work_type")

                            # Update with more detailed raw data if available
                            if job_details.get("raw_data"):
                                raw_data = job_details.get("raw_data")

                            # Add a delay to avoid rate limiting
                            time.sleep(config.rate_limit / 2)  # Half the regular rate limit
                        except Exception as e:
                            context.log.error(f"Error fetching job details: {str(e)}")

                    # Try to parse date from different formats
                    published_at = None

                    # Try to extract date from posted_on text (e.g., "Posted 3 Days Ago")
                    if posted_on:
                        days_ago_match = re.search(r'Posted (\d+) Days? Ago', posted_on)
                        if days_ago_match:
                            days_ago = int(days_ago_match.group(1))
                            published_at = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
                        elif "Posted Today" in posted_on:
                            published_at = datetime.now().strftime("%Y-%m-%d")
                        elif "Posted Yesterday" in posted_on:
                            published_at = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                        elif "Posted 30+ Days Ago" in posted_on:
                            # Estimate as 30 days ago
                            published_at = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

                    # If we got a date_posted from detail page, use that instead
                    if date_posted:
                        published_at = date_posted

                    # Skip old jobs based on cutoff date if we have a date
                    if published_at:
                        try:
                            job_date = datetime.strptime(published_at, "%Y-%m-%d")
                            if job_date < cutoff_date:
                                context.log.info(f"Skipping old job: {job_title} (posted {published_at})")
                                stats["old_jobs_skipped"] += 1
                                continue
                            else:
                                stats["recent_jobs"] += 1
                        except Exception as e:
                            context.log.warning(f"Error parsing job date: {str(e)}")

                    # Prepare job record
                    job_record = {
                        "job_id": job_id,
                        "company_id": company_id,
                        "job_title": job_title,
                        "job_description": job_description,
                        "job_url": job_url,
                        "location": location,
                        "time_type": time_type,
                        "employment_type": employment_type,
                        "published_at": published_at,
                        "valid_through": valid_through,
                        "date_retrieved": datetime.now().isoformat(),
                        "is_active": True,
                        "raw_data": raw_data,
                        "partition_key": partition_key,
                        "work_type": work_type,
                        "compensation": compensation
                    }

                    if existing_job:
                        # For existing jobs, we'll handle updates in a separate step
                        company_jobs_updated += 1
                        stats["updated_jobs"] += 1
                    else:
                        # Add to batch for insertion
                        batch_jobs.append(job_record)
                        company_jobs_added += 1
                        stats["new_jobs_added"] += 1

                # Update company result for checkpoint
                company_result["jobs_added"] = company_jobs_added
                company_result["jobs_updated"] = company_jobs_updated
                context.log.info(f"Processed {company_name}: Added {company_jobs_added}, Updated {company_jobs_updated}")

                # Add a delay between companies to avoid rate limits
                time.sleep(config.rate_limit)

            except Exception as e:
                context.log.error(f"Error processing company {company_name}: {str(e)}")
                company_result["status"] = "error"
                company_result["error"] = str(e)
                stats["errors"] += 1

                # Add to failed companies list
                new_failed_companies.append({
                    "company_id": company_id,
                    "company_name": company_name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "partition_key": partition_key
                })

            # Update stats and checkpoints
            stats["companies_processed"] += 1
            checkpoint_results.append(company_result)

            # Save checkpoint after each company for resumability
            try:
                pd.DataFrame(checkpoint_results).to_csv(checkpoint_file, index=False)
            except Exception as e:
                context.log.error(f"Error saving checkpoint: {str(e)}")

        # Insert jobs from this batch to BigQuery
        if batch_jobs:
            try:
                # Create a temporary table for bulk loading
                temp_table_name = f"{dataset_name}.workday_jobs_temp"

                # Drop the temp table if it exists
                query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
                query_job.result()

                # Create the temporary table
                create_temp_sql = f"""
                CREATE TABLE {temp_table_name} (
                    job_id STRING,
                    company_id STRING,
                    job_title STRING,
                    job_description STRING,
                    job_url STRING,
                    location STRING,
                    time_type STRING,
                    employment_type STRING,
                    published_at DATE,
                    valid_through DATE,
                    date_retrieved TIMESTAMP,
                    is_active BOOL,
                    raw_data STRING,
                    partition_key STRING,
                    work_type STRING,
                    compensation STRING
                )
                """
                query_job = client.query(create_temp_sql)
                query_job.result()

                # Convert list of dicts to dataframe
                jobs_df = pd.DataFrame(batch_jobs)

                # Handle data types - ensure all IDs are strings
                for col in jobs_df.columns:
                    if col.endswith('_id') or col == 'job_id' or col == 'company_id':
                        jobs_df[col] = jobs_df[col].astype(str)

                # Handle other object columns
                for col in jobs_df.select_dtypes(include=['object']).columns:
                    jobs_df[col] = jobs_df[col].fillna('').astype(str)

                # Convert date columns
                if 'published_at' in jobs_df.columns:
                    jobs_df['published_at'] = pd.to_datetime(jobs_df['published_at'], errors='coerce')

                if 'valid_through' in jobs_df.columns:
                    jobs_df['valid_through'] = pd.to_datetime(jobs_df['valid_through'], errors='coerce')

                if 'date_retrieved' in jobs_df.columns:
                    jobs_df['date_retrieved'] = pd.to_datetime(jobs_df['date_retrieved'], errors='coerce')

                # Set up job configuration
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    schema=[
                        bigquery.SchemaField("job_id", "STRING"),
                        bigquery.SchemaField("company_id", "STRING"),
                        bigquery.SchemaField("job_title", "STRING"),
                        bigquery.SchemaField("job_description", "STRING"),
                        bigquery.SchemaField("job_url", "STRING"),
                        bigquery.SchemaField("location", "STRING"),
                        bigquery.SchemaField("time_type", "STRING"),
                        bigquery.SchemaField("employment_type", "STRING"),
                        bigquery.SchemaField("published_at", "DATE"),
                        bigquery.SchemaField("valid_through", "DATE"),
                        bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
                        bigquery.SchemaField("is_active", "BOOL"),
                        bigquery.SchemaField("raw_data", "STRING"),
                        bigquery.SchemaField("partition_key", "STRING"),
                        bigquery.SchemaField("work_type", "STRING"),
                        bigquery.SchemaField("compensation", "STRING")
                    ]
                )

                # Load the dataframe into the temporary table
                job = client.load_table_from_dataframe(
                    jobs_df,
                    temp_table_name,
                    job_config=job_config
                )
                # Wait for the load job to complete
                job.result()
                context.log.info(f"Loaded {len(jobs_df)} jobs into temporary table")

                # Insert new jobs into the main table
                insert_sql = f"""
                INSERT INTO {dataset_name}.workday_jobs
                SELECT * FROM {temp_table_name}
                """
                query_job = client.query(insert_sql)
                query_job.result()
                context.log.info(f"Inserted {len(jobs_df)} new jobs into main table")

                # Clean up - drop the temporary table
                query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
                query_job.result()

            except Exception as e:
                context.log.error(f"Error inserting jobs into BigQuery: {str(e)}")
                import traceback
                context.log.error(f"Traceback: {traceback.format_exc()}")

        # Update existing jobs to mark them as still active
        if stats["updated_jobs"] > 0:
            try:
                # Get job IDs from this batch
                company_ids = batch["company_id"].astype(str).tolist()
                company_ids_str = ", ".join([f"'{company_id}'" for company_id in company_ids])

                # Extract unique job IDs for active jobs
                active_job_ids = []
                for job in job_listings:
                    if job.get("job_id") and job.get("job_id") in existing_jobs:
                        active_job_ids.append(job.get("job_id"))

                if active_job_ids:
                    # Format for SQL query
                    job_ids_str = ", ".join([f"'{job_id}'" for job_id in active_job_ids])

                    # Update existing jobs
                    update_sql = f"""
                    UPDATE {dataset_name}.workday_jobs
                    SET
                        date_retrieved = CURRENT_TIMESTAMP(),
                        is_active = TRUE
                    WHERE company_id IN ({company_ids_str})
                    AND job_id IN ({job_ids_str})
                    """

                    query_job = client.query(update_sql)
                    query_job.result()
                    context.log.info(f"Updated {len(active_job_ids)} existing jobs")
            except Exception as e:
                context.log.error(f"Error updating existing jobs: {str(e)}")

        # Save failed companies if any new failures
        if new_failed_companies:
            try:
                all_failed = failed_companies + new_failed_companies
                pd.DataFrame(all_failed).to_csv(failed_companies_file, index=False)
                context.log.info(f"Updated failed companies list with {len(new_failed_companies)} new entries")
            except Exception as e:
                context.log.error(f"Error saving failed companies: {str(e)}")

        # Report progress to Dagster
        context.log_event(
            AssetMaterialization(
                asset_key=context.asset_key,
                description=f"Processed batch {batch_num}/{total_batches}",
                metadata={
                    "companies_processed": MetadataValue.int(int(stats["companies_processed"])),
                    "total_companies": MetadataValue.int(int(stats["total_companies"])),
                    "batch": MetadataValue.int(int(batch_num)),
                    "total_batches": MetadataValue.int(int(total_batches)),
                    "jobs_found": MetadataValue.int(int(stats["total_jobs_found"])),
                    "jobs_added": MetadataValue.int(int(stats["new_jobs_added"])),
                    "jobs_updated": MetadataValue.int(int(stats["updated_jobs"])),
                    "partition_key": MetadataValue.text(partition_key)
                }
            )
        )

    # Mark inactive jobs (if we found at least one job for a company)
    if stats["companies_with_jobs"] > 0:
        try:
            # Get all companies that were successfully processed
            processed_company_ids = []
            for result in checkpoint_results:
                if result.get("status") == "success" and result.get("jobs_found", 0) > 0:
                    processed_company_ids.append(result.get("company_id"))

            if processed_company_ids:
                # Format for SQL
                company_ids_str = ", ".join([f"'{company_id}'" for company_id in processed_company_ids])

                # Mark jobs as inactive if they weren't seen in this run
                current_date = datetime.now().strftime("%Y-%m-%d")
                inactivate_sql = f"""
                UPDATE {dataset_name}.workday_jobs
                SET is_active = FALSE
                WHERE company_id IN ({company_ids_str})
                AND date_retrieved < '{current_date}'
                AND is_active = TRUE
                """

                query_job = client.query(inactivate_sql)
                result = query_job.result()

                # Get count of affected rows
                inactive_count = query_job.num_dml_affected_rows
                context.log.info(f"Marked {inactive_count} jobs as inactive")
                stats["jobs_marked_inactive"] = inactive_count
        except Exception as e:
            context.log.error(f"Error marking inactive jobs: {str(e)}")

    # Log summary stats
    context.log.info(f"Workday job discovery complete for partition {partition_key}. Stats:")
    context.log.info(f"Total companies: {stats['total_companies']}")
    context.log.info(f"Companies processed: {stats['companies_processed']}")
    context.log.info(f"Companies with jobs: {stats['companies_with_jobs']}")
    context.log.info(f"Total jobs found: {stats['total_jobs_found']}")
    context.log.info(f"New jobs added: {stats['new_jobs_added']}")
    context.log.info(f"Jobs updated: {stats['updated_jobs']}")
    if "jobs_marked_inactive" in stats:
        context.log.info(f"Jobs marked inactive: {stats['jobs_marked_inactive']}")
    context.log.info(f"Errors: {stats['errors']}")

    # Add metadata to the output
    context.add_output_metadata({
        "total_companies": MetadataValue.int(int(stats["total_companies"])),
        "companies_processed": MetadataValue.int(int(stats["companies_processed"])),
        "companies_with_jobs": MetadataValue.int(int(stats["companies_with_jobs"])),
        "total_jobs_found": MetadataValue.int(int(stats["total_jobs_found"])),
        "new_jobs_added": MetadataValue.int(int(stats["new_jobs_added"])),
        "jobs_updated": MetadataValue.int(int(stats["updated_jobs"])),
        "partition_key": MetadataValue.text(partition_key),
        "bigquery_table": MetadataValue.text(f"{dataset_name}.workday_jobs")
    })

    return stats

# Create a job definition for this asset
workday_jobs_discovery_job = define_asset_job(
    name="workday_jobs_discovery_job",
    selection=[workday_company_jobs_discovery],
    description="Discovers and loads job listings from Workday career sites"
)

# Create the Dagster definitions
defs = Definitions(
    assets=[workday_company_jobs_discovery],
    jobs=[workday_jobs_discovery_job]
)