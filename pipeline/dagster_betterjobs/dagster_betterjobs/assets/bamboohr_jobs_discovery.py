import time
import json
import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from google.cloud import bigquery
from dagster import (
    asset, AssetExecutionContext, Config, get_dagster_logger,
    MetadataValue, AssetMaterialization, StaticPartitionsDefinition,
    Definitions, define_asset_job
)

from dagster_betterjobs.scrapers.bamboohr_scraper import BambooHRScraper

logger = get_dagster_logger()

# Partition companies alphabetically A-Z + numeric + other
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

class BambooHRJobsDiscoveryConfig(Config):
    """Configuration parameters for BambooHR job discovery."""
    max_companies: Optional[int] = None  # Optional limit on processed companies
    rate_limit: float = 2.0  # Seconds between requests
    max_retries: int = 3
    retry_delay: int = 2
    min_company_id: Optional[int] = None  # For batch processing
    max_company_id: Optional[int] = None  # For batch processing
    days_to_look_back: int = 14  # Job freshness threshold in days
    batch_size: int = 10  # Companies per batch before committing
    skip_detailed_fetch: bool = False  # Skip detailed job info for faster processing

@asset(
    group_name="job_discovery",
    kinds={"API", "bigquery", "python"},
    required_resource_keys={"bigquery"},
    deps=["master_company_urls"],
    partitions_def=alpha_partitions
)
def bamboohr_company_jobs_discovery(context: AssetExecutionContext, config: BambooHRJobsDiscoveryConfig) -> Dict:
    """
    Discovers and stores job listings from BambooHR career sites.

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
    checkpoint_file = checkpoint_dir / f"bamboohr_jobs_discovery_{partition_key}_checkpoint.csv"
    failed_companies_file = checkpoint_dir / f"bamboohr_jobs_discovery_{partition_key}_failed.csv"

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
    WHERE platform = 'bamboohr'
    AND ats_url IS NOT NULL
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
    context.log.info(f"Found {total_companies} BambooHR companies in partition {partition_key} to process")

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
    if checkpoint_file.exists():
        try:
            checkpoint_df = pd.read_csv(checkpoint_file)
            processed_company_ids = set(checkpoint_df["company_id"].astype(str).tolist())
            context.log.info(f"Loaded {len(processed_company_ids)} previously processed companies from checkpoint")

            # Update stats from checkpoint
            stats["companies_processed"] = len(processed_company_ids)
            if "jobs_found" in checkpoint_df.columns:
                stats["total_jobs_found"] = checkpoint_df["jobs_found"].sum()
            if "jobs_added" in checkpoint_df.columns:
                stats["new_jobs_added"] = checkpoint_df["jobs_added"].sum()
            if "jobs_updated" in checkpoint_df.columns:
                stats["updated_jobs"] = checkpoint_df["jobs_updated"].sum()

        except Exception as e:
            context.log.error(f"Error loading checkpoint file: {str(e)}")

    # Skip already processed companies
    companies_to_process = companies_df[~companies_df["company_id"].astype(str).isin(processed_company_ids)]
    context.log.info(f"{len(companies_to_process)} BambooHR companies remaining to process")

    # Load previously failed companies
    failed_companies = []
    if failed_companies_file.exists():
        try:
            failed_df = pd.read_csv(failed_companies_file)
            failed_companies = failed_df.to_dict("records")
            context.log.info(f"Loaded {len(failed_companies)} previously failed companies")
        except Exception as e:
            context.log.error(f"Error loading failed companies file: {str(e)}")

    # Create jobs table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {dataset_name}.bamboohr_jobs (
        job_id STRING,
        company_id STRING,
        job_title STRING,
        job_description STRING,
        job_url STRING,
        location STRING,
        department STRING,
        employment_status STRING,
        date_posted DATE,
        date_retrieved TIMESTAMP,
        is_active BOOL,
        raw_data STRING,
        partition_key STRING
    )
    """

    try:
        query_job = client.query(create_table_sql)
        query_job.result()
        context.log.info("Created or verified bamboohr_jobs table in BigQuery")
    except Exception as e:
        context.log.error(f"Error creating jobs table: {str(e)}")
        return {"error": str(e), "status": "failed"}

    # Process companies in batches
    batch_size = config.batch_size
    new_failed_companies = []
    checkpoint_results = []

    # Load previous checkpoint results if exists
    if checkpoint_file.exists():
        try:
            checkpoint_results = pd.read_csv(checkpoint_file).to_dict("records")
        except Exception:
            checkpoint_results = []

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
            company_id = company["company_id"]
            company_name = company["company_name"]
            career_url = company["career_url"]
            ats_url = company["ats_url"]

            context.log.info(f"Processing jobs for {company_name} (ID: {company_id})")
            context.log.info(f"Using ATS URL: {ats_url}")

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
                # Create BambooHR scraper
                scraper = BambooHRScraper(
                    career_url=career_url,
                    rate_limit=config.rate_limit,
                    max_retries=config.max_retries,
                    retry_delay=config.retry_delay,
                    dagster_log=context.log,  # Pass Dagster logger to the scraper
                    ats_url=ats_url,  # Pass the BambooHR ATS URL
                    cutoff_date=cutoff_date  # Pass the cutoff date to the scraper
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

                # Process each job listing
                for job in job_listings:
                    job_id = job.get("job_id")
                    job_url = job.get("job_url")

                    if not job_id or not job_url:
                        continue

                    # Get detailed job info unless skipped in config
                    job_details = job
                    if not config.skip_detailed_fetch:
                        try:
                            job_details = scraper.get_job_details(job_id)
                            # Add short delay to avoid overwhelming the server
                            time.sleep(0.5)
                        except Exception as e:
                            context.log.warning(f"Error getting details for job {job_id}: {str(e)}")
                            # Use basic info if detailed fetch fails
                            job_details = job

                    # Check posting date if available
                    date_posted = job_details.get("date_posted")
                    is_recent = True

                    if date_posted:
                        try:
                            posted_date = datetime.strptime(date_posted, "%Y-%m-%d")
                            is_recent = posted_date >= cutoff_date
                        except (ValueError, TypeError):
                            # If we can't parse the date, assume it's recent
                            is_recent = True

                    # Skip old jobs
                    if not is_recent:
                        stats["old_jobs_skipped"] += 1
                        continue

                    stats["recent_jobs"] += 1

                    # Check if job already exists in BigQuery
                    check_sql = f"""
                    SELECT job_id
                    FROM {dataset_name}.bamboohr_jobs
                    WHERE job_url = '{job_url}'
                    """

                    try:
                        query_job = client.query(check_sql)
                        existing_job = list(query_job.result())
                    except Exception as e:
                        context.log.error(f"Error checking if job exists: {str(e)}")
                        existing_job = []

                    # Prepare job data
                    job_title = job_details.get("job_title", "")
                    job_description = job_details.get("job_description", "")
                    department = job_details.get("department", "")
                    employment_status = job_details.get("employment_status", "")

                    # Handle location data
                    location_str = None
                    if job_details.get("location_string"):
                        location_str = job_details["location_string"]
                    elif isinstance(job_details.get("location"), dict):
                        loc_parts = []
                        loc = job_details["location"]
                        if loc.get("city"):
                            loc_parts.append(loc["city"])
                        if loc.get("state"):
                            loc_parts.append(loc["state"])
                        if loc_parts:
                            location_str = ", ".join(loc_parts)

                    # Convert any structured data to JSON strings
                    raw_data = None
                    if "raw_data" in job_details:
                        raw_data = json.dumps(job_details["raw_data"])

                    # Prepare job record
                    job_record = {
                        "job_id": job_id,
                        "company_id": company_id,
                        "job_title": job_title,
                        "job_description": job_description,
                        "job_url": job_url,
                        "location": location_str,
                        "department": department,
                        "employment_status": employment_status,
                        "date_posted": date_posted,
                        "date_retrieved": datetime.now().isoformat(),
                        "is_active": True,
                        "raw_data": raw_data,
                        "partition_key": partition_key
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
                stats["errors"] += 1

                # Update company result for checkpoint with error
                company_result["status"] = "error"
                company_result["error"] = str(e)

                # Add to failed companies
                failed_entry = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                    "partition_key": partition_key
                }
                new_failed_companies.append(failed_entry)

            # Add company result to checkpoint results
            checkpoint_results.append(company_result)

            # Increment counter
            stats["companies_processed"] += 1

        # Insert jobs from this batch to BigQuery
        if batch_jobs:
            try:
                # Create a temporary table for bulk loading
                temp_table_name = f"{dataset_name}.bamboohr_jobs_temp"

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
                    department STRING,
                    employment_status STRING,
                    date_posted DATE,
                    date_retrieved TIMESTAMP,
                    is_active BOOL,
                    raw_data STRING,
                    partition_key STRING
                )
                """
                query_job = client.query(create_temp_sql)
                query_job.result()

                # Convert list of dicts to dataframe
                jobs_df = pd.DataFrame(batch_jobs)

                # Handle data types
                for col in jobs_df.select_dtypes(include=['object']).columns:
                    jobs_df[col] = jobs_df[col].fillna('').astype(str)

                # Convert date columns
                if 'date_posted' in jobs_df.columns:
                    jobs_df['date_posted'] = pd.to_datetime(jobs_df['date_posted'], errors='coerce')

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
                        bigquery.SchemaField("department", "STRING"),
                        bigquery.SchemaField("employment_status", "STRING"),
                        bigquery.SchemaField("date_posted", "DATE"),
                        bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
                        bigquery.SchemaField("is_active", "BOOL"),
                        bigquery.SchemaField("raw_data", "STRING"),
                        bigquery.SchemaField("partition_key", "STRING")
                    ]
                )

                # Load the dataframe into the temporary table
                load_job = client.load_table_from_dataframe(
                    jobs_df,
                    temp_table_name,
                    job_config=job_config
                )
                load_job.result()

                # Insert only new jobs (not already in the main table)
                insert_sql = f"""
                INSERT INTO {dataset_name}.bamboohr_jobs (
                    job_id, company_id, job_title, job_description, job_url,
                    location, department, employment_status, date_posted,
                    date_retrieved, is_active, raw_data, partition_key
                )
                SELECT
                    t.job_id, t.company_id, t.job_title, t.job_description, t.job_url,
                    t.location, t.department, t.employment_status, t.date_posted,
                    t.date_retrieved, t.is_active, t.raw_data, t.partition_key
                FROM {temp_table_name} t
                LEFT JOIN {dataset_name}.bamboohr_jobs j
                    ON t.job_url = j.job_url
                WHERE j.job_url IS NULL
                """

                query_job = client.query(insert_sql)
                query_job.result()

                # Update existing jobs
                update_sql = f"""
                UPDATE {dataset_name}.bamboohr_jobs j
                SET
                    j.job_title = t.job_title,
                    j.job_description = t.job_description,
                    j.location = t.location,
                    j.department = t.department,
                    j.employment_status = t.employment_status,
                    j.date_posted = t.date_posted,
                    j.date_retrieved = t.date_retrieved,
                    j.is_active = t.is_active,
                    j.raw_data = t.raw_data,
                    j.partition_key = t.partition_key
                FROM {temp_table_name} t
                WHERE j.job_url = t.job_url
                """

                query_job = client.query(update_sql)
                query_job.result()

                # Drop the temporary table
                query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
                query_job.result()

                context.log.info(f"Successfully loaded batch of {len(batch_jobs)} jobs into BigQuery")

            except Exception as e:
                context.log.error(f"Error loading jobs to BigQuery: {str(e)}")
                stats["errors"] += 1

        # Save checkpoint after each batch
        try:
            checkpoint_df = pd.DataFrame(checkpoint_results)
            checkpoint_df.to_csv(checkpoint_file, index=False)
            context.log.info(f"Updated checkpoint with {len(checkpoint_results)} companies")
        except Exception as e:
            context.log.error(f"Error saving checkpoint: {str(e)}")

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

    # Log summary stats
    context.log.info(f"BambooHR job discovery complete for partition {partition_key}. Stats:")
    context.log.info(f"Total companies: {stats['total_companies']}")
    context.log.info(f"Companies processed: {stats['companies_processed']}")
    context.log.info(f"Companies with jobs: {stats['companies_with_jobs']}")
    context.log.info(f"Total jobs found: {stats['total_jobs_found']}")
    context.log.info(f"New jobs added: {stats['new_jobs_added']}")
    context.log.info(f"Jobs updated: {stats['updated_jobs']}")
    context.log.info(f"Recent jobs: {stats['recent_jobs']}")
    context.log.info(f"Old jobs skipped: {stats['old_jobs_skipped']}")
    context.log.info(f"Errors: {stats['errors']}")

    # Convert any NumPy integers to Python integers
    # This is needed because Dagster's MetadataValue.int() doesn't accept NumPy types
    for key, value in stats.items():
        if hasattr(value, 'dtype') and 'int' in str(value.dtype):
            stats[key] = int(value)
        elif isinstance(value, (list, dict)):
            # If we have nested structures, convert those too
            context.log.info(f"Converting complex stat: {key}, type: {type(value)}")

    # Update job count in BigQuery
    try:
        query_job = client.query(f"SELECT COUNT(*) FROM {dataset_name}.bamboohr_jobs WHERE partition_key = '{partition_key}'")
        count_result = list(query_job.result())
        partition_jobs = count_result[0][0]
        context.log.info(f"Jobs in BigQuery table for partition {partition_key}: {partition_jobs}")

        # Get total job count too
        query_job = client.query(f"SELECT COUNT(*) FROM {dataset_name}.bamboohr_jobs")
        count_result = list(query_job.result())
        total_jobs = count_result[0][0]
        context.log.info(f"Total jobs in BigQuery table: {total_jobs}")

        # Add to stats - ensure they're Python ints
        stats["jobs_in_partition"] = int(partition_jobs)
        stats["total_jobs_in_table"] = int(total_jobs)
    except Exception as e:
        context.log.error(f"Error getting job count: {str(e)}")

    # Add metadata to the output
    context.add_output_metadata({
        "total_companies": MetadataValue.int(int(stats["total_companies"])),
        "companies_processed": MetadataValue.int(int(stats["companies_processed"])),
        "companies_with_jobs": MetadataValue.int(int(stats["companies_with_jobs"])),
        "total_jobs_found": MetadataValue.int(int(stats["total_jobs_found"])),
        "new_jobs_added": MetadataValue.int(int(stats["new_jobs_added"])),
        "jobs_updated": MetadataValue.int(int(stats["updated_jobs"])),
        "partition_key": MetadataValue.text(partition_key),
        "bigquery_table": MetadataValue.text(f"{dataset_name}.bamboohr_jobs")
    })

    return stats

# Create a job to run for specific partitions (normally by schedule)
bamboohr_jobs_discovery_job = define_asset_job(
    name="bamboohr_jobs_discovery_job",
    selection=[bamboohr_company_jobs_discovery]
)

# Create a job to process all partitions at once (for backfills)
bamboohr_jobs_all_partitions_job = define_asset_job(
    name="bamboohr_jobs_all_partitions_job",
    selection=[bamboohr_company_jobs_discovery]
)

# Create Dagster Definitions object for deployment
defs = Definitions(
    assets=[bamboohr_company_jobs_discovery],
    jobs=[bamboohr_jobs_discovery_job, bamboohr_jobs_all_partitions_job]
)