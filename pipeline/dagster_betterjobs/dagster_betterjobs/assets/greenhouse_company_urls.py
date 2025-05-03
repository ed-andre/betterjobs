import os
import csv
import json
import pandas as pd
from typing import Dict, List, Tuple, Set, Optional, Any
import logging
import time
from pathlib import Path
import traceback
import requests
from urllib.parse import urlparse

from dagster import asset, AssetExecutionContext, Config, get_dagster_logger, Output, AssetMaterialization, MaterializeResult, MetadataValue
from dagster_gemini import GeminiResource
from google.cloud import bigquery

from .url_discovery import parse_gemini_response
from .validate_urls import validate_urls
from .retry_failed_company_urls import retry_failed_company_urls

logger = get_dagster_logger()

def process_greenhouse_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies specifically using Greenhouse ATS."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for the following companies that use Greenhouse as their ATS (Applicant Tracking System).

    IMPORTANT: Greenhouse job board URLs follow one of these specific patterns:
    - https://boards.greenhouse.io/[tenant]/
    - https://boards.greenhouse.io/embed/job_board?for=[tenant]

    For example:
    - Instacart: https://boards.greenhouse.io/embed/job_board?for=Instacart
    - Dropbox: https://boards.greenhouse.io/embed/job_board?for=dropbox
    - Ceribell, Inc: https://boards.greenhouse.io/ceribell/

    The key is to identify the correct tenant ID in the URL, which is usually
    a simplified version of the company name (lowercase, no spaces).

    Some companies integrate Greenhouse into their own career sites, so also check
    for embedded Greenhouse job boards on company career pages.

    DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists. It's better to return null than an incorrect URL.

    For each company, I need:
    1. ATS_URL: The specific Greenhouse job board URL (following one of the patterns above)
    2. CAREER_URL: Direct URL to their own company careers/jobs page

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://boards.greenhouse.io/exampleinc/",
        "career_url": "https://example.com/careers"
      }},
      {{
        "company_name": "Another Company",
        "ats_url": "https://boards.greenhouse.io/embed/job_board?for=anothercompany",
        "career_url": "https://anothercompany.com/jobs"
      }},
      {{
        "company_name": "Third Company",
        "ats_url": null,
        "career_url": "https://thirdcompany.com/jobs"
      }}
    ]

    Use null for any URL you cannot find with high confidence.

    Companies to process:
    {company_list_json}
    """

    max_retries = 2
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            with gemini.get_model(context) as model:
                context.log.info(f"Sending batch of {len(companies)} Greenhouse companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                results = parse_gemini_response(context, response.text, companies)

                # Ensure platform is set to greenhouse for all companies with greenhouse URLs
                for result in results:
                    if result.get("ats_url") and "greenhouse.io" in result.get("ats_url", "").lower():
                        result["platform"] = "greenhouse"
                    else:
                        # Only assign platform if not already set
                        if "platform" not in result:
                            result["platform"] = "greenhouse"

                return results

        except Exception as e:
            context.log.error(f"Error processing Greenhouse companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process Greenhouse companies after multiple attempts")

@asset(
    group_name="url_discovery",
    kinds={"gemini", "bigquery"},
    io_manager_key="bigquery_io",
    deps=["initialize_db"],
    required_resource_keys={"bigquery", "gemini"}
)
def greenhouse_company_urls(context: AssetExecutionContext) -> None:
    """
    Specialized asset for discovering Greenhouse ATS job board URLs.

    This asset focuses exclusively on companies using Greenhouse ATS to find their job board URLs
    following the patterns:
    - https://job-boards.greenhouse.io/[tenant]/jobs/
    - https://boards.greenhouse.io/[tenant]/

    Before processing any company, it checks if the company already exists in the master_company_urls
    table to avoid redundant processing. Companies that already exist in the master table will be
    skipped unless they are explicitly reprocessed through a full rerun.

    Returns None instead of a DataFrame to avoid type conversion issues with the IO manager.
    """
    # Access the Gemini resource from context
    gemini = context.resources.gemini

    # Get data source directory with proper path handling
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        # If we're already in pipeline/dagster_betterjobs
        datasource_dir = Path("dagster_betterjobs/data_load/datasource")
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        # Otherwise use the full path
        datasource_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/data_load/datasource")
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    # Create checkpoint directory if it doesn't exist
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = checkpoint_dir / "greenhouse_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "greenhouse_url_discovery_failed.csv"

    context.log.info(f"Looking for company data in: {datasource_dir}")
    context.log.info(f"Using checkpoint file: {checkpoint_file}")

    # Check if directory exists
    if not datasource_dir.exists():
        context.log.error(f"Data source directory not found: {datasource_dir}")
        return None

    # Load all company data
    all_companies = []
    csv_files = list(datasource_dir.glob("*_companies.csv"))

    if not csv_files:
        context.log.warning(f"No company CSV files found in {datasource_dir}")
        return None

    for file_path in csv_files:
        platform = file_path.stem.split("_")[0]
        context.log.info(f"Processing company file: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Only include Greenhouse companies
                    if platform.lower() == "greenhouse" or (row.get("platform", "").lower() == "greenhouse"):
                        company = {
                            "company_name": row["company_name"],
                            "company_industry": row["company_industry"],
                            "platform": "greenhouse"  # Ensure platform is set correctly
                        }
                        all_companies.append(company)
        except Exception as e:
            context.log.error(f"Error reading file {file_path}: {str(e)}")

    if not all_companies:
        context.log.warning("No Greenhouse company data was loaded from CSV files")
        return None

    context.log.info(f"Loaded {len(all_companies)} Greenhouse companies from CSV files")

    # Check master_company_urls table for existing companies using BigQuery
    dataset_name = os.getenv("GCP_DATASET_ID")

    try:
        # Get BigQuery client from context resources
        client = context.resources.bigquery

        # Check if master_company_urls table exists in BigQuery
        table_id = f"{dataset_name}.master_company_urls"
        try:
            client.get_table(table_id)
            table_exists = True

            # Get list of companies already in master table using BigQuery
            existing_companies_query = f"""
            SELECT company_name
            FROM {dataset_name}.master_company_urls
            """
            query_job = client.query(existing_companies_query)
            existing_df = query_job.to_dataframe()
            existing_companies = set(existing_df["company_name"])

            # Filter out companies that already exist in master table
            original_count = len(all_companies)
            all_companies = [c for c in all_companies if c["company_name"] not in existing_companies]
            skipped_count = original_count - len(all_companies)

            context.log.info(f"Skipped {skipped_count} companies that already exist in master_company_urls table")
            if skipped_count > 0:
                context.log.info(f"Processing {len(all_companies)} new companies")
        except Exception as e:
            context.log.info(f"Master company URLs table not found or not accessible: {str(e)}")
            context.log.info("Will process all companies")
    except Exception as e:
        context.log.warning(f"BigQuery client error: {str(e)}")
        context.log.warning("Cannot access BigQuery for master table check. Continuing with all companies.")
        context.log.info("Please check your GCP credentials and permissions.")

    # Load previously processed companies if checkpoint exists
    processed_companies: Set[str] = set()
    results_so_far = []

    if checkpoint_file.exists():
        try:
            checkpoint_df = pd.read_csv(checkpoint_file)
            processed_companies = set(checkpoint_df["company_name"].tolist())
            results_so_far = checkpoint_df.to_dict("records")
            context.log.info(f"Loaded {len(processed_companies)} previously processed companies from checkpoint")
        except Exception as e:
            context.log.error(f"Error loading checkpoint file: {str(e)}")

    # Filter out already processed companies
    companies_to_process = [c for c in all_companies if c["company_name"] not in processed_companies]
    context.log.info(f"{len(companies_to_process)} Greenhouse companies remaining to process")

    # Load previously failed companies if file exists
    failed_companies = []
    if failed_companies_file.exists():
        try:
            failed_df = pd.read_csv(failed_companies_file)
            failed_companies = failed_df.to_dict("records")
            context.log.info(f"Loaded {len(failed_companies)} previously failed companies")
        except Exception as e:
            context.log.error(f"Error loading failed companies file: {str(e)}")

    # Process companies in batches - smaller batch size for more reliable processing
    batch_size = 10  # Reduced batch size for more reliable processing
    results = results_so_far.copy()
    new_failed_companies = []

    for i in range(0, len(companies_to_process), batch_size):
        batch = companies_to_process[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(companies_to_process) + batch_size - 1) // batch_size
        context.log.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)")

        try:
            # Process Greenhouse companies with targeted approach
            batch_results = process_greenhouse_companies(context, gemini, batch)

            # Validate URLs
            batch_results = validate_urls(context, batch_results)
            results.extend(batch_results)

            # Save checkpoint after each successful batch
            all_results_df = pd.DataFrame(results)
            all_results_df.to_csv(checkpoint_file, index=False)
            context.log.info(f"Saved checkpoint with {len(results)} companies")

            # Report progress to Dagster
            context.log_event(
                AssetMaterialization(
                    asset_key=context.asset_key,
                    description=f"Processed batch {batch_num}/{total_batches}",
                    metadata={
                        "companies_processed": MetadataValue.int(len(results)),
                        "total_companies": MetadataValue.int(len(all_companies)),
                        "batch": MetadataValue.int(batch_num),
                        "total_batches": MetadataValue.int(total_batches)
                    }
                )
            )
        except Exception as e:
            context.log.error(f"Error processing batch {batch_num}: {str(e)}")
            context.log.error(traceback.format_exc())

            # Mark all companies in this batch as failed
            for company in batch:
                failed_entry = company.copy()
                failed_entry["error"] = str(e)
                failed_entry["batch"] = batch_num
                new_failed_companies.append(failed_entry)

            # Save failed companies immediately
            all_failed = failed_companies + new_failed_companies
            pd.DataFrame(all_failed).to_csv(failed_companies_file, index=False)
            context.log.info(f"Updated failed companies list with {len(new_failed_companies)} new entries")

        # Sleep to avoid rate limiting
        time.sleep(2)

    # Convert to DataFrame
    df = pd.DataFrame(results)

    # Create empty DataFrame with correct columns if results is empty
    if len(results) == 0:
        df = pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])
        context.log.warning("No results were produced")
        return None

    # Log summary stats
    total_companies = len(df)
    ats_urls_found = 0
    career_urls_found = 0
    verified_urls = 0

    if total_companies > 0:
        if "ats_url" in df.columns:
            ats_urls_found = df["ats_url"].notna().sum()
        if "career_url" in df.columns:
            career_urls_found = df["career_url"].notna().sum()
        if "url_verified" in df.columns:
            verified_urls = df["url_verified"].sum()

    context.log.info(f"Processed {total_companies} Greenhouse companies")
    if total_companies > 0:
        context.log.info(f"Found ATS URLs for {ats_urls_found} companies ({ats_urls_found/total_companies:.1%})")
        context.log.info(f"Found career URLs for {career_urls_found} companies ({career_urls_found/total_companies:.1%})")
        context.log.info(f"Verified URLs for {verified_urls} companies ({verified_urls/total_companies:.1%})")

    if new_failed_companies:
        context.log.warning(f"{len(new_failed_companies)} companies failed processing")
        context.log.info("Failed companies saved to checkpoint directory for reprocessing")

    # Now save the data to BigQuery
    client = context.resources.bigquery

    # Create the greenhouse_company_urls table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {dataset_name}.greenhouse_company_urls (
        company_name STRING NOT NULL,
        company_industry STRING,
        platform STRING NOT NULL,
        ats_url STRING,
        career_url STRING,
        url_verified BOOL DEFAULT FALSE,
        date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    query_job = client.query(create_table_sql)
    query_job.result()  # Wait for the query to complete
    context.log.info("Created or verified greenhouse_company_urls table")

    # Skip if there are no results
    if df.empty:
        context.log.info("No data to load into BigQuery")
        return None

    # Add timestamp for when the record was processed
    df['date_processed'] = pd.Timestamp.now()

    # Create a temporary table for the bulk load approach
    temp_table_name = f"{dataset_name}.greenhouse_company_urls_temp"

    # Drop the temp table if it exists
    query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
    query_job.result()

    # Create the temporary table
    create_temp_sql = f"""
    CREATE TABLE {temp_table_name} (
        company_name STRING NOT NULL,
        company_industry STRING,
        platform STRING NOT NULL,
        ats_url STRING,
        career_url STRING,
        url_verified BOOL DEFAULT FALSE,
        date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    query_job = client.query(create_temp_sql)
    query_job.result()
    context.log.info("Created temporary table for bulk loading")

    # Set up job configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("company_industry", "STRING"),
            bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ats_url", "STRING"),
            bigquery.SchemaField("career_url", "STRING"),
            bigquery.SchemaField("url_verified", "BOOL"),
            bigquery.SchemaField("date_processed", "TIMESTAMP"),
        ]
    )

    # Fill any null values in string columns with empty strings to avoid type errors
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('').astype(str)

    # Handle boolean column explicitly
    if 'url_verified' in df.columns:
        df['url_verified'] = df['url_verified'].fillna(False)

    # Load the dataframe into the temporary table
    try:
        job = client.load_table_from_dataframe(
            df,
            temp_table_name,
            job_config=job_config
        )
        # Wait for the load job to complete
        job.result()
        context.log.info(f"Successfully loaded {len(df)} companies into temporary table")

        # Deduplicate and insert data from temporary to main table
        transfer_sql = f"""
        INSERT INTO {dataset_name}.greenhouse_company_urls
            (company_name, company_industry, platform, ats_url, career_url, url_verified, date_processed)
        SELECT
            t.company_name,
            t.company_industry,
            t.platform,
            t.ats_url,
            t.career_url,
            CAST(t.url_verified AS BOOL),
            t.date_processed
        FROM {temp_table_name} t
        LEFT JOIN {dataset_name}.greenhouse_company_urls m
            ON t.company_name = m.company_name
        WHERE m.company_name IS NULL
        """
        query_job = client.query(transfer_sql)
        query_job.result()
        context.log.info("Deduplicated and transferred data to main table")

        # Check how many records were inserted
        count_sql = f"SELECT COUNT(*) FROM {dataset_name}.greenhouse_company_urls"
        query_job = client.query(count_sql)
        count_result = list(query_job.result())
        count = count_result[0][0]
        context.log.info(f"Total records in greenhouse_company_urls table: {count}")

        # Clean up - drop the temporary table
        query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
        query_job.result()
        context.log.info("Dropped temporary table")

        # Add metadata to the output - convert NumPy types to Python native types and wrap in MetadataValue
        context.add_output_metadata({
            "total_companies_processed": MetadataValue.int(int(len(df))),
            "ats_urls_found": MetadataValue.int(int(ats_urls_found)),
            "career_urls_found": MetadataValue.int(int(career_urls_found)),
            "verified_urls": MetadataValue.int(int(verified_urls)),
            "bigquery_table": MetadataValue.text(f"{dataset_name}.greenhouse_company_urls"),
            "total_records": MetadataValue.int(int(count))
        })

    except Exception as e:
        context.log.error(f"Error loading company data to BigQuery: {str(e)}")
        # Add error metadata
        context.add_output_metadata({
            "error": MetadataValue.text(str(e))
        })

    return None

@asset(
    group_name="url_discovery",
    kinds={"gemini", "bigquery"},
    io_manager_key="bigquery_io",
    deps=["initialize_db", "greenhouse_company_urls"],
    required_resource_keys={"bigquery", "gemini"}
)
def retry_failed_greenhouse_company_urls(context: AssetExecutionContext) -> None:
    """
    Retries finding career URLs for Greenhouse companies that failed verification.
    Uses the generic retry function with Greenhouse-specific configuration.

    Returns None instead of a DataFrame to avoid type conversion issues with the IO manager.
    Results are written directly to BigQuery.
    """
    # Access the Gemini resource from context
    gemini = context.resources.gemini

    return retry_failed_company_urls(
        context=context,
        gemini=gemini,
        ats_platform="greenhouse",
        table_name="greenhouse_company_urls"
    )