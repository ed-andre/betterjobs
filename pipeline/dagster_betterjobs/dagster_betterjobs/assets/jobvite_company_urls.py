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

from dagster import asset, AssetExecutionContext, Config, get_dagster_logger, Output, AssetMaterialization, MaterializeResult
from dagster_gemini import GeminiResource

from .url_discovery import find_company_urls_individual, parse_gemini_response
from .validate_urls import validate_urls

from .retry_failed_company_urls import retry_failed_company_urls

logger = get_dagster_logger()

def process_jobvite_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies specifically using Jobvite ATS."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for the following companies that use Jobvite as their ATS (Applicant Tracking System).

    IMPORTANT: Jobvite job board URLs follow this specific pattern:
    https://jobs.jobvite.com/[tenant]/

    For example:
    - Example Company: https://jobs.jobvite.com/examplecompany/
    - Tech Startup: https://jobs.jobvite.com/techstartup/

    The key is to identify the correct tenant name, which is usually a simplified version
    of the company name (lowercase, no spaces, no special characters).

    Some companies may use:
    - Careers.[company].com that redirects to a Jobvite portal
    - jobs.[company].com that integrates with Jobvite
    - Company websites with embedded Jobvite job boards

    DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists. It's better to return null than an incorrect URL.

    For each company, I need:
    1. ATS_URL: The specific Jobvite job board URL (following the pattern above)
    2. CAREER_URL: Direct URL to their own company careers/jobs page

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://jobs.jobvite.com/exampleinc/",
        "career_url": "https://example.com/careers"
      }},
      {{
        "company_name": "Another Company",
        "ats_url": null,
        "career_url": "https://anothercompany.com/jobs"
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
                context.log.info(f"Sending batch of {len(companies)} Jobvite companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                results = parse_gemini_response(context, response.text, companies)

                # Ensure platform is set to jobvite for all companies with jobvite URLs
                for result in results:
                    if result.get("ats_url") and "jobvite.com" in result.get("ats_url", "").lower():
                        result["platform"] = "jobvite"
                    else:
                        # Only assign platform if not already set
                        if "platform" not in result:
                            result["platform"] = "jobvite"

                return results

        except Exception as e:
            context.log.error(f"Error processing Jobvite companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process Jobvite companies after multiple attempts")

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def jobvite_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Specialized asset for discovering Jobvite ATS job board URLs.

    This asset focuses exclusively on companies using Jobvite ATS to find their job board URLs
    following the pattern: https://jobs.jobvite.com/[tenant]/

    Before processing any company, it checks if the company already exists in the master_company_urls
    table to avoid redundant processing. Companies that already exist in the master table will be
    skipped unless they are explicitly reprocessed through a full rerun.
    """
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
    checkpoint_file = checkpoint_dir / "jobvite_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "jobvite_url_discovery_failed.csv"

    context.log.info(f"Looking for company data in: {datasource_dir}")
    context.log.info(f"Using checkpoint file: {checkpoint_file}")

    # Check if directory exists
    if not datasource_dir.exists():
        context.log.error(f"Data source directory not found: {datasource_dir}")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Load all company data
    all_companies = []
    csv_files = list(datasource_dir.glob("*_companies.csv"))

    if not csv_files:
        context.log.warning(f"No company CSV files found in {datasource_dir}")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    for file_path in csv_files:
        platform = file_path.stem.split("_")[0]
        context.log.info(f"Processing company file: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Only include Jobvite companies
                    if platform.lower() == "jobvite" or (row.get("platform", "").lower() == "jobvite"):
                        company = {
                            "company_name": row["company_name"],
                            "company_industry": row["company_industry"],
                            "platform": "jobvite"  # Ensure platform is set correctly
                        }
                        all_companies.append(company)
        except Exception as e:
            context.log.error(f"Error reading file {file_path}: {str(e)}")

    if not all_companies:
        context.log.warning("No Jobvite company data was loaded from CSV files")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Loaded {len(all_companies)} Jobvite companies from CSV files")

    # Check master_company_urls table for existing companies
    try:
        with context.resources.duckdb_resource.get_connection() as conn:
            # Check if master_company_urls table exists
            table_exists = conn.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                AND name='master_company_urls'
            """).fetchone()

            if table_exists:
                # Get list of companies already in master table
                existing_companies_query = """
                SELECT company_name
                FROM public.master_company_urls
                """
                existing_df = conn.execute(existing_companies_query).df()
                existing_companies = set(existing_df["company_name"])

                # Filter out companies that already exist in master table
                original_count = len(all_companies)
                all_companies = [c for c in all_companies if c["company_name"] not in existing_companies]
                skipped_count = original_count - len(all_companies)

                context.log.info(f"Skipped {skipped_count} companies that already exist in master_company_urls table")
                if skipped_count > 0:
                    context.log.info(f"Processing {len(all_companies)} new companies")
    except Exception as e:
        context.log.info(f"Could not check master_company_urls table, will process all companies: {str(e)}")

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
    context.log.info(f"{len(companies_to_process)} Jobvite companies remaining to process")

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
            # Process Jobvite companies with targeted approach
            batch_results = process_jobvite_companies(context, gemini, batch)

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
                        "companies_processed": len(results),
                        "total_companies": len(all_companies),
                        "batch": batch_num,
                        "total_batches": total_batches
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

    context.log.info(f"Processed {total_companies} Jobvite companies")
    if total_companies > 0:
        context.log.info(f"Found ATS URLs for {ats_urls_found} companies ({ats_urls_found/total_companies:.1%})")
        context.log.info(f"Found career URLs for {career_urls_found} companies ({career_urls_found/total_companies:.1%})")
        context.log.info(f"Verified URLs for {verified_urls} companies ({verified_urls/total_companies:.1%})")

    if new_failed_companies:
        context.log.warning(f"{len(new_failed_companies)} companies failed processing")
        context.log.info("Failed companies saved to checkpoint directory for reprocessing")

    return df

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db", "jobvite_company_urls"]
)
def retry_failed_jobvite_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retries finding career URLs for Jobvite companies that failed verification.
    Uses the generic retry function with Jobvite-specific configuration.
    """


    return retry_failed_company_urls(
        context=context,
        gemini=gemini,
        ats_platform="jobvite",
        table_name="jobvite_company_urls"
    )
