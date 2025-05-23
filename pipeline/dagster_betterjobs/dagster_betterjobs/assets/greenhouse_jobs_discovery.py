import time
import json
import os
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

from dagster_betterjobs.scrapers.greenhouse_scraper import GreenhouseScraper

logger = get_dagster_logger()

# Partition companies alphabetically A-Z + numeric + other
alpha_partitions = StaticPartitionsDefinition([
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "0-9", "other"
])

class GreenhouseJobsDiscoveryConfig(Config):
    """Configuration parameters for Greenhouse job discovery."""
    max_companies: Optional[int] = None  # Optional limit on processed companies
    rate_limit: float = 2.0  # Seconds between requests
    max_retries: int = 3
    retry_delay: int = 2
    min_company_id: Optional[int] = None  # For batch processing
    max_company_id: Optional[int] = None  # For batch processing
    days_to_look_back: int = 14  # Job freshness threshold in days
    batch_size: int = 10  # Companies per batch before committing
    skip_detailed_fetch: bool = False  # Skip detailed job fetching if needed
    process_all_companies: bool = True  # By default, process all companies regardless of checkpoint status

@asset(
    group_name="job_discovery",
    kinds={"API", "bigquery", "python"},
    required_resource_keys={"bigquery"},
    deps=["master_company_urls"],
    partitions_def=alpha_partitions
)
def greenhouse_company_jobs_discovery(context: AssetExecutionContext, config: GreenhouseJobsDiscoveryConfig) -> Dict:
    """
    Discovers and stores job listings from Greenhouse career sites.

    Processes companies partitioned by first letter of company name,
    retrieves all current job listings, and stores them in BigQuery.
    """
    # Initialize BigQuery client and get dataset
    client = context.resources.bigquery
    dataset_name = os.getenv("GCP_DATASET_ID")
    partition_key = context.partition_key

    # Set job freshness cutoff date
    cutoff_date = datetime.now() - timedelta(days=config.days_to_look_back)
    # Make cutoff_date timezone-aware with UTC timezone to match Greenhouse dates
    cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
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
    checkpoint_file = checkpoint_dir / f"greenhouse_jobs_discovery_{partition_key}_checkpoint.csv"
    failed_companies_file = checkpoint_dir / f"greenhouse_jobs_discovery_{partition_key}_failed.csv"

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
    WHERE platform = 'greenhouse'
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
    context.log.info(f"Found {total_companies} Greenhouse companies in partition {partition_key} to process")

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
    if checkpoint_file.exists() and not config.process_all_companies:
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

    # Skip already processed companies if configured to do so
    if not config.process_all_companies and checkpoint_file.exists():
        companies_to_process = companies_df[~companies_df["company_id"].astype(str).isin(processed_company_ids)]
        context.log.info(f"{len(companies_to_process)} Greenhouse companies remaining to process after skipping processed ones")
    else:
        # Process all companies, even those previously processed
        companies_to_process = companies_df
        context.log.info(f"Processing all {len(companies_to_process)} Greenhouse companies in this partition")

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
    CREATE TABLE IF NOT EXISTS {dataset_name}.greenhouse_jobs (
        job_id STRING,
        company_id STRING,
        job_title STRING,
        job_description STRING,
        job_url STRING,
        location STRING,
        department STRING,
        department_id STRING,
        published_at DATE,
        updated_at TIMESTAMP,
        requisition_id STRING,
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
        context.log.info("Created or verified greenhouse_jobs table in BigQuery")
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
                # Create Greenhouse scraper
                scraper = GreenhouseScraper(
                    career_url=career_url,
                    rate_limit=config.rate_limit,
                    max_retries=config.max_retries,
                    retry_delay=config.retry_delay,
                    dagster_log=context.log,  # Pass Dagster logger to the scraper
                    ats_url=ats_url,  # Pass the Greenhouse ATS URL
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

                    # Ensure job_id is always a string
                    job_id = str(job_id)
                    job["job_id"] = job_id

                    # Get detailed job info unless skipped in config
                    job_details = job
                    if not config.skip_detailed_fetch:
                        try:
                            job_details = scraper.get_job_details(job_url)
                            # Add short delay to avoid overwhelming the server
                            time.sleep(0.5)
                        except Exception as e:
                            context.log.warning(f"Error getting details for job {job_id}: {str(e)}")
                            # Use basic info if detailed fetch fails
                            job_details = job

                    # Check if job is recent enough
                    is_recent = job_details.get("is_recent", True)

                    # Skip old jobs
                    if not is_recent:
                        stats["old_jobs_skipped"] += 1
                        continue

                    stats["recent_jobs"] += 1

                    # Check if job already exists in BigQuery
                    check_sql = f"""
                    SELECT job_id
                    FROM {dataset_name}.greenhouse_jobs
                    WHERE job_url = '{job_url}'
                    """

                    try:
                        query_job = client.query(check_sql)
                        existing_job = list(query_job.result())
                    except Exception as e:
                        context.log.error(f"Error checking if job exists: {str(e)}")
                        existing_job = []

                    # Extract job details
                    job_id = job_details.get("id")
                    job_title = job_details.get("job_title")
                    location = job_details.get("location")
                    job_url = job_details.get("job_url")
                    published_at = job_details.get("published_at")
                    updated_at = job_details.get("updated_at")
                    requisition_id = job_details.get("requisition_id")

                    # Handle department data
                    department = None
                    department_id = None
                    if job_details.get("department"):
                        dept_data = job_details["department"]
                        if isinstance(dept_data, dict):
                            department = dept_data.get("name")
                            department_id = dept_data.get("id")
                        elif isinstance(dept_data, str):
                            department = dept_data

                    # Get location string - ensure it's a string
                    location_str = job_details.get("location", "")

                    if isinstance(location_str, dict) and "name" in location_str:
                        location_str = location_str.get("name", "")
                    elif not isinstance(location_str, str):
                        location_str = str(location_str) if location_str is not None else ""

                    # Ensure job_title is a string
                    if not isinstance(job_title, str):
                        job_title = str(job_title) if job_title is not None else ""

                    # Ensure job_description is a string
                    if not isinstance(job_details.get("job_description", ""), str):
                        job_description = str(job_details.get("job_description", "")) if job_details.get("job_description", "") is not None else ""
                    elif not job_details.get("job_description", "") and "content" in job_details:
                        job_description = job_details.get("content", "")
                        if not isinstance(job_description, str):
                            job_description = str(job_description) if job_description is not None else ""

                    # Get dates
                    published_at = job_details.get("published_at")
                    updated_at = job_details.get("updated_at")

                    # Get requisition ID if available
                    requisition_id = job_details.get("requisition_id")

                    # Get work type if available
                    work_type = job_details.get("work_type")

                    # Get compensation if available
                    compensation = job_details.get("compensation")

                    # Convert any structured data to JSON strings
                    raw_data = None
                    if "raw_data" in job_details:
                        try:
                            raw_data = json.dumps(job_details["raw_data"])
                        except Exception as e:
                            context.log.warning(f"Error converting raw data to JSON: {str(e)}")
                            raw_data = "{}"

                    # Prepare job record
                    job_record = {
                        "job_id": str(job_id),  # Ensure job_id is a string
                        "company_id": str(company_id),  # Ensure company_id is a string
                        "job_title": job_title if job_title else "",  # Default to empty string if None
                        "job_description": job_details.get("job_description", "") if job_details.get("job_description", "") is not None else "",  # Default to empty string if None
                        "job_url": job_url,
                        "location": location_str if location_str else "",  # Default to empty string if None
                        "department": department if department else "",  # Default to empty string if None
                        "department_id": str(department_id) if department_id else "",  # Default to empty string if None
                        "published_at": published_at if published_at else None,  # Keep as None for date handling
                        "updated_at": updated_at if updated_at else None,  # Keep as None for date handling
                        "requisition_id": str(requisition_id) if requisition_id else "",  # Default to empty string if None
                        "date_retrieved": datetime.now().isoformat(),
                        "is_active": True,
                        "raw_data": raw_data if raw_data else "{}",  # Default to empty JSON if None
                        "partition_key": partition_key,
                        "work_type": work_type if work_type else "",  # Default to empty string if None
                        "compensation": compensation if compensation else ""  # Default to empty string if None
                    }

                    # Extra validation to ensure no complex types in job_record
                    for key, value in job_record.items():
                        if isinstance(value, (dict, list)):
                            context.log.warning(f"Converting complex type in {key} to string for job {job_id}")
                            try:
                                job_record[key] = json.dumps(value)
                            except:
                                job_record[key] = str(value)
                        elif value is None and key not in ["published_at", "updated_at"]:
                            # Allow None only for date fields, convert to empty string for others
                            context.log.warning(f"Converting None value in {key} to empty string for job {job_id}")
                            job_record[key] = ""

                    # Debug logging at lower level
                    context.log.debug(f"Prepared job record for job_id {job_id}")

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
                temp_table_name = f"{dataset_name}.greenhouse_jobs_temp"

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
                    department_id STRING,
                    published_at DATE,
                    updated_at TIMESTAMP,
                    requisition_id STRING,
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

                # Handle other object columns - ensure all are proper strings
                for col in jobs_df.select_dtypes(include=['object']).columns:
                    # Replace None with empty string and convert all to strings
                    jobs_df[col] = jobs_df[col].fillna('').astype(str)

                # Explicitly handle common problematic columns
                if 'department' in jobs_df.columns:
                    # Ensure department is properly stringified
                    jobs_df['department'] = jobs_df['department'].apply(
                        lambda x: x if isinstance(x, str) else (str(x) if x is not None else '')
                    )

                if 'location' in jobs_df.columns:
                    # Ensure location is properly stringified
                    jobs_df['location'] = jobs_df['location'].apply(
                        lambda x: x if isinstance(x, str) else (str(x) if x is not None else '')
                    )

                # Convert date columns
                if 'published_at' in jobs_df.columns:
                    jobs_df['published_at'] = pd.to_datetime(jobs_df['published_at'], errors='coerce')

                if 'updated_at' in jobs_df.columns:
                    jobs_df['updated_at'] = pd.to_datetime(jobs_df['updated_at'], errors='coerce')

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
                        bigquery.SchemaField("department_id", "STRING"),
                        bigquery.SchemaField("published_at", "DATE"),
                        bigquery.SchemaField("updated_at", "TIMESTAMP"),
                        bigquery.SchemaField("requisition_id", "STRING"),
                        bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
                        bigquery.SchemaField("is_active", "BOOL"),
                        bigquery.SchemaField("raw_data", "STRING"),
                        bigquery.SchemaField("partition_key", "STRING"),
                        bigquery.SchemaField("work_type", "STRING"),
                        bigquery.SchemaField("compensation", "STRING")
                    ]
                )

                # Final sanity check on data types to catch any remaining issues
                try:
                    # Check for any remaining dictionary or complex types that need to be converted
                    for col in jobs_df.columns:
                        if jobs_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                            context.log.warning(f"Converting complex types in column {col} to strings")
                            jobs_df[col] = jobs_df[col].apply(lambda x:
                                json.dumps(x) if isinstance(x, (dict, list)) else str(x))

                    # Check for NaN values and replace with appropriate values based on column type
                    for col in jobs_df.columns:
                        if pd.isna(jobs_df[col]).any():
                            if col in ["job_title", "job_description", "job_url", "location",
                                      "department", "requisition_id", "work_type",
                                      "compensation", "raw_data", "partition_key"]:
                                # String columns get empty string
                                jobs_df[col] = jobs_df[col].fillna('')
                            elif col == "is_active":
                                # Boolean columns get False
                                jobs_df[col] = jobs_df[col].fillna(False)

                    context.log.info(f"DataFrame prepared for BigQuery: {len(jobs_df)} rows")
                except Exception as e:
                    context.log.error(f"Error in final dataframe preparation: {str(e)}")

                # Load the dataframe into the temporary table
                load_job = client.load_table_from_dataframe(
                    jobs_df,
                    temp_table_name,
                    job_config=job_config
                )
                result = load_job.result()

                context.log.info(f"Load job completed with state: {load_job.state}")
                if load_job.errors:
                    context.log.error(f"Load job errors: {load_job.errors}")

                # Insert only new jobs (not already in the main table)
                insert_sql = f"""
                INSERT INTO {dataset_name}.greenhouse_jobs (
                    job_id, company_id, job_title, job_description, job_url,
                    location, department, department_id, published_at, updated_at,
                    requisition_id, date_retrieved, is_active, raw_data, partition_key,
                    work_type, compensation
                )
                SELECT
                    t.job_id, t.company_id, t.job_title, t.job_description, t.job_url,
                    t.location, t.department, t.department_id, t.published_at, t.updated_at,
                    t.requisition_id, t.date_retrieved, t.is_active, t.raw_data, t.partition_key,
                    t.work_type, t.compensation
                FROM {temp_table_name} t
                LEFT JOIN {dataset_name}.greenhouse_jobs j
                    ON t.job_url = j.job_url
                WHERE j.job_url IS NULL
                """

                context.log.info(f"Running insertion SQL")

                query_job = client.query(insert_sql)
                insert_result = query_job.result()

                # Update existing jobs
                update_sql = f"""
                UPDATE {dataset_name}.greenhouse_jobs j
                SET
                    j.job_title = t.job_title,
                    j.job_description = t.job_description,
                    j.location = t.location,
                    j.department = t.department,
                    j.department_id = t.department_id,
                    j.published_at = t.published_at,
                    j.updated_at = t.updated_at,
                    j.requisition_id = t.requisition_id,
                    j.date_retrieved = t.date_retrieved,
                    j.is_active = t.is_active,
                    j.raw_data = t.raw_data,
                    j.partition_key = t.partition_key,
                    j.work_type = t.work_type,
                    j.compensation = t.compensation
                FROM {temp_table_name} t
                WHERE j.job_url = t.job_url
                """

                query_job = client.query(update_sql)
                update_result = query_job.result()

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
    context.log.info(f"Greenhouse job discovery complete for partition {partition_key}. Stats:")
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
        query_job = client.query(f"SELECT COUNT(*) FROM {dataset_name}.greenhouse_jobs WHERE partition_key = '{partition_key}'")
        count_result = list(query_job.result())
        partition_jobs = count_result[0][0]
        context.log.info(f"Jobs in BigQuery table for partition {partition_key}: {partition_jobs}")

        # Get total job count too
        query_job = client.query(f"SELECT COUNT(*) FROM {dataset_name}.greenhouse_jobs")
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
        "bigquery_table": MetadataValue.text(f"{dataset_name}.greenhouse_jobs")
    })

    return stats

# Create a job to run for specific partitions (normally by schedule)
greenhouse_jobs_discovery_job = define_asset_job(
    name="greenhouse_jobs_discovery_job",
    selection=[greenhouse_company_jobs_discovery]
)

# Create a job to process all partitions at once (for backfills)
greenhouse_jobs_all_partitions_job = define_asset_job(
    name="greenhouse_jobs_all_partitions_job",
    selection=[greenhouse_company_jobs_discovery]
)

# Create Dagster Definitions object for deployment
defs = Definitions(
    assets=[greenhouse_company_jobs_discovery],
    jobs=[greenhouse_jobs_discovery_job, greenhouse_jobs_all_partitions_job]
)