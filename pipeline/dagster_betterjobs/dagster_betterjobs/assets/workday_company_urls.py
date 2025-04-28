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

def process_workday_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies specifically using Workday ATS."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for the following companies that use Workday as their ATS (Applicant Tracking System).

    IMPORTANT: Workday job board URLs follow this specific pattern:
    https://[tenant].wd1.myworkdayjobs.com/[locale]/[site-name]

    For example:
    - CoStar: https://costar.wd1.myworkdayjobs.com/en-US/CoStarCareers
    - TBK Bank: https://tbkbank.wd1.myworkdayjobs.com/en-US/tpay
    - Go Limitless: https://golimitless.wd1.myworkdayjobs.com/en-US/Staff_External_career_site

    The key is to identify the correct tenant name (usually derived from the company name)
    and the site name (often a variation of the company name or careers).

    DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists. It's better to return null than an incorrect URL.

    For each company, I need:
    1. ATS_URL: The specific Workday job board URL (following the pattern above)
    2. CAREER_URL: Direct URL to their own company careers/jobs page

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://exampleinc.wd1.myworkdayjobs.com/en-US/ExampleCareers",
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
                context.log.info(f"Sending batch of {len(companies)} Workday companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing Workday companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process Workday companies after multiple attempts")


@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def workday_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Specialized asset for discovering Workday ATS job board URLs.

    This asset focuses exclusively on companies using Workday ATS to find their job board URLs
    following the pattern: https://[tenant].wd1.myworkdayjobs.com/[locale]/[site-name]
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
    checkpoint_file = checkpoint_dir / "workday_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "workday_url_discovery_failed.csv"

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
                    # Only include Workday companies
                    if platform.lower() == "workday" or (row.get("platform", "").lower() == "workday"):
                        company = {
                            "company_name": row["company_name"],
                            "company_industry": row["company_industry"],
                            "platform": "workday"  # Ensure platform is set correctly
                        }
                        all_companies.append(company)
        except Exception as e:
            context.log.error(f"Error reading file {file_path}: {str(e)}")

    if not all_companies:
        context.log.warning("No Workday company data was loaded from CSV files")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Loaded {len(all_companies)} Workday companies from CSV files")

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
    context.log.info(f"{len(companies_to_process)} Workday companies remaining to process")

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
            # Process Workday companies with targeted approach
            batch_results = process_workday_companies(context, gemini, batch)

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

    context.log.info(f"Processed {total_companies} Workday companies")
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
    deps=["initialize_db", "workday_company_urls"]
)
def retry_failed_workday_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retries finding career URLs for Workday companies that previously failed.

    This asset processes the failed companies with a more conservative approach,
    using individual processing and additional retries.
    """
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    failed_companies_file = checkpoint_dir / "workday_url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed Workday companies file found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_df = pd.read_csv(failed_companies_file)
        failed_companies = failed_df.to_dict("records")
        context.log.info(f"Found {len(failed_companies)} failed Workday companies to retry")
    except Exception as e:
        context.log.error(f"Error reading failed companies file: {str(e)}")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    if not failed_companies:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Process failed companies individually with additional safeguards
    results = []
    still_failed = []

    for company in failed_companies:
        try:
            context.log.info(f"Retrying Workday company: {company['company_name']}")
            urls = find_company_urls_individual(
                context=context,
                gemini=gemini,
                company_name=company["company_name"],
                company_industry=company["company_industry"],
                platform="workday",  # Ensure platform is set correctly
                max_retries=3  # More retries for failed companies
            )

            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": "workday",
                "ats_url": urls.get("ats_url"),
                "career_url": urls.get("career_url"),
                "url_verified": False
            }

            # Validate the URLs
            validated_results = validate_urls(context, [result])
            if validated_results:
                results.append(validated_results[0])
            else:
                results.append(result)

            time.sleep(2)  # Longer pause between requests

        except Exception as e:
            context.log.error(f"Still failed for Workday company {company['company_name']}: {str(e)}")
            company["error"] = str(e)
            still_failed.append(company)

    # Update the failed companies file with only those that still failed
    if still_failed:
        pd.DataFrame(still_failed).to_csv(failed_companies_file, index=False)
        context.log.info(f"Updated failed companies file with {len(still_failed)} remaining failures")
    else:
        # If all were successful, remove the failed companies file
        failed_companies_file.unlink(missing_ok=True)
        context.log.info("All failed Workday companies successfully processed, removed failed companies file")

    # Return the successfully processed results
    if not results:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    return pd.DataFrame(results)
