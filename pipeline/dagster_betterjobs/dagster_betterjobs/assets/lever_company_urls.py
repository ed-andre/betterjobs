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

logger = get_dagster_logger()

def process_lever_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies specifically using Lever ATS."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for the following companies that use Lever as their ATS (Applicant Tracking System).

    IMPORTANT: Lever job board URLs follow this specific pattern:
    https://jobs.lever.co/[tenant]/

    For example:
    - Example Company: https://jobs.lever.co/examplecompany/
    - Tech Startup: https://jobs.lever.co/techstartup/

    The key is to identify the correct tenant name, which is usually a simplified version
    of the company name (lowercase, no spaces, no special characters).

    Some companies may use:
    - Careers.[company].com that redirects to a Lever portal
    - jobs.[company].com that integrates with Lever
    - Company websites with embedded Lever job boards

    DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists. It's better to return null than an incorrect URL.

    For each company, I need:
    1. ATS_URL: The specific Lever job board URL (following the pattern above)
    2. CAREER_URL: Direct URL to their own company careers/jobs page

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://jobs.lever.co/exampleinc/",
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
                context.log.info(f"Sending batch of {len(companies)} Lever companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing Lever companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process Lever companies after multiple attempts")

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def lever_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Specialized asset for discovering Lever ATS job board URLs.

    This asset focuses exclusively on companies using Lever ATS to find their job board URLs
    following the pattern: https://jobs.lever.co/[tenant]/
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
    checkpoint_file = checkpoint_dir / "lever_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "lever_url_discovery_failed.csv"

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
                    # Only include Lever companies
                    if platform.lower() == "lever" or (row.get("platform", "").lower() == "lever"):
                        company = {
                            "company_name": row["company_name"],
                            "company_industry": row["company_industry"],
                            "platform": "lever"  # Ensure platform is set correctly
                        }
                        all_companies.append(company)
        except Exception as e:
            context.log.error(f"Error reading file {file_path}: {str(e)}")

    if not all_companies:
        context.log.warning("No Lever company data was loaded from CSV files")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Loaded {len(all_companies)} Lever companies from CSV files")

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
    context.log.info(f"{len(companies_to_process)} Lever companies remaining to process")

    # Load previously failed companies if file exists
    failed_companies = []
    if failed_companies_file.exists():
        try:
            failed_df = pd.read_csv(failed_companies_file)
            failed_companies = failed_df.to_dict("records")
            context.log.info(f"Loaded {len(failed_companies)} previously failed companies")
        except Exception as e:
            context.log.error(f"Error loading failed companies file: {str(e)}")

    # Process companies in batches
    batch_size = 10  # Reduced batch size for more reliable processing
    results = results_so_far.copy()
    new_failed_companies = []

    for i in range(0, len(companies_to_process), batch_size):
        batch = companies_to_process[i:i+batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(companies_to_process) + batch_size - 1) // batch_size
        context.log.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} companies)")

        try:
            # Process lever companies with targeted approach
            batch_results = process_lever_companies(context, gemini, batch)

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

    context.log.info(f"Processed {total_companies} lever companies")
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
    deps=["initialize_db", "lever_company_urls"]
)
def retry_failed_lever_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retry processing failed companies from the lever_company_urls asset.

    This asset attempts to discover URLs for companies that failed in the initial processing,
    using an individual processing approach that might succeed where batch processing failed.
    """
    # Get data source directory with proper path handling
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        # If we're already in pipeline/dagster_betterjobs
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        # Otherwise use the full path
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    # Check for failed companies file
    failed_companies_file = checkpoint_dir / "lever_url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed companies file found, nothing to retry")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_companies_df = pd.read_csv(failed_companies_file)
        context.log.info(f"Found {len(failed_companies_df)} failed companies to retry")
    except Exception as e:
        context.log.error(f"Error loading failed companies: {str(e)}")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    if len(failed_companies_df) == 0:
        context.log.info("No failed companies to retry")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Process each failed company individually
    successful_retries = []

    for _, company in failed_companies_df.iterrows():
        try:
            context.log.info(f"Retrying company: {company['company_name']}")

            # Use individual processing with more retries
            result = find_company_urls_individual(
                context,
                gemini,
                company["company_name"],
                company.get("company_industry", "Unknown"),
                "lever",
                max_retries=3,
                retry_delay=3
            )

            # Add to successful retries if we found an ATS URL
            if result and result.get("ats_url"):
                company_result = {
                    "company_name": company["company_name"],
                    "company_industry": company.get("company_industry", "Unknown"),
                    "platform": "lever",
                    "ats_url": result.get("ats_url"),
                    "career_url": result.get("career_url"),
                    "url_verified": False
                }

                # Validate the URLs
                validated_results = validate_urls(context, [company_result])
                if validated_results:
                    successful_retries.append(validated_results[0])
                else:
                    successful_retries.append(company_result)

                context.log.info(f"Successfully found Lever URL for {company['company_name']}")
            else:
                context.log.info(f"Still unable to find Lever URL for {company['company_name']}")

        except Exception as e:
            context.log.error(f"Error retrying company {company['company_name']}: {str(e)}")

    # Create DataFrame from successful retries
    retry_results = pd.DataFrame(successful_retries)

    if len(retry_results) == 0:
        context.log.info("No companies were successfully retried")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Successfully retried {len(retry_results)} companies")
    return retry_results