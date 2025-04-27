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

logger = get_dagster_logger()

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Discovers and retrieves both ATS job board URLs and career page URLs for companies.

    Uses Google's Gemini API to find:
    1. Third-party ATS job board URLs (primary goal)
    2. Direct company career page URLs (fallback)

    Results are stored in a DuckDB table for further use in job scraping.
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
    checkpoint_file = checkpoint_dir / "url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "url_discovery_failed.csv"

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
                    company = {
                        "company_name": row["company_name"],
                        "company_industry": row["company_industry"],
                        "platform": platform
                    }
                    all_companies.append(company)
        except Exception as e:
            context.log.error(f"Error reading file {file_path}: {str(e)}")

    if not all_companies:
        context.log.warning("No company data was loaded from CSV files")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Loaded {len(all_companies)} companies from CSV files")

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
    context.log.info(f"{len(companies_to_process)} companies remaining to process")

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
            # Find URLs for all companies in the batch at once
            batch_results = find_urls_batch(context, gemini, batch)

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

    context.log.info(f"Processed {total_companies} companies")
    if total_companies > 0:
        context.log.info(f"Found ATS URLs for {ats_urls_found} companies ({ats_urls_found/total_companies:.1%})")
        context.log.info(f"Found career URLs for {career_urls_found} companies ({career_urls_found/total_companies:.1%})")
        context.log.info(f"Verified URLs for {verified_urls} companies ({verified_urls/total_companies:.1%})")

    if new_failed_companies:
        context.log.warning(f"{len(new_failed_companies)} companies failed processing")
        context.log.info("Failed companies saved to checkpoint directory for reprocessing")

    return df

def validate_urls(context: AssetExecutionContext, companies: List[Dict]) -> List[Dict]:
    """
    Validate URLs by checking if they return a successful response.

    Args:
        context: Dagster execution context
        companies: List of company dictionaries with URLs

    Returns:
        List of companies with validated URLs and url_verified flag
    """
    validated_companies = []
    timeout = 5  # Short timeout to avoid long waits

    for company in companies:
        company_copy = company.copy()
        company_copy["url_verified"] = False

        # Try to validate ATS URL
        ats_url = company.get("ats_url")
        if ats_url:
            try:
                # Check URL format first
                parsed_url = urlparse(ats_url)
                if not (parsed_url.scheme and parsed_url.netloc):
                    context.log.warning(f"Invalid ATS URL format for {company['company_name']}: {ats_url}")
                    company_copy["ats_url"] = None
                else:
                    # Make a HEAD request to check if URL exists
                    response = requests.head(ats_url, timeout=timeout, allow_redirects=True)

                    if response.status_code < 400:
                        context.log.info(f"Verified ATS URL for {company['company_name']}: {ats_url}")
                        company_copy["url_verified"] = True
                    else:
                        context.log.warning(f"Invalid ATS URL for {company['company_name']}: {ats_url} (Status: {response.status_code})")
                        company_copy["ats_url"] = None
            except Exception as e:
                context.log.warning(f"Error validating ATS URL for {company['company_name']}: {str(e)}")
                company_copy["ats_url"] = None

        # If ATS URL is invalid, try the career URL
        if not company_copy["ats_url"] and company.get("career_url"):
            career_url = company.get("career_url")
            try:
                # Check URL format first
                parsed_url = urlparse(career_url)
                if not (parsed_url.scheme and parsed_url.netloc):
                    context.log.warning(f"Invalid career URL format for {company['company_name']}: {career_url}")
                    company_copy["career_url"] = None
                else:
                    # Make a HEAD request to check if URL exists
                    response = requests.head(career_url, timeout=timeout, allow_redirects=True)

                    if response.status_code < 400:
                        context.log.info(f"Verified career URL for {company['company_name']}: {career_url}")
                        company_copy["url_verified"] = True
                    else:
                        context.log.warning(f"Invalid career URL for {company['company_name']}: {career_url} (Status: {response.status_code})")
                        company_copy["career_url"] = None
            except Exception as e:
                context.log.warning(f"Error validating career URL for {company['company_name']}: {str(e)}")
                company_copy["career_url"] = None

        validated_companies.append(company_copy)

    return validated_companies

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db", "company_urls"]
)
def retry_failed_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retries finding career URLs for companies that previously failed.

    This asset processes the failed companies with a more conservative approach,
    using individual processing and additional retries.
    """
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    failed_companies_file = checkpoint_dir / "url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed companies file found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_df = pd.read_csv(failed_companies_file)
        failed_companies = failed_df.to_dict("records")
        context.log.info(f"Found {len(failed_companies)} failed companies to retry")
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
            context.log.info(f"Retrying company: {company['company_name']}")
            urls = find_company_urls_individual(
                context=context,
                gemini=gemini,
                company_name=company["company_name"],
                company_industry=company["company_industry"],
                platform=company["platform"],
                max_retries=3  # More retries for failed companies
            )

            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": company["platform"],
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
            context.log.error(f"Still failed for {company['company_name']}: {str(e)}")
            company["error"] = str(e)
            still_failed.append(company)

    # Update the failed companies file with only those that still failed
    if still_failed:
        pd.DataFrame(still_failed).to_csv(failed_companies_file, index=False)
        context.log.info(f"Updated failed companies file with {len(still_failed)} remaining failures")
    else:
        # If all were successful, remove the failed companies file
        failed_companies_file.unlink(missing_ok=True)
        context.log.info("All failed companies successfully processed, removed failed companies file")

    # Return the successfully processed results
    if not results:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    return pd.DataFrame(results)

def find_urls_batch(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """
    Use Gemini API to find both ATS job board URLs and company career URLs for multiple companies.

    Args:
        context: Dagster execution context
        gemini: Gemini resource
        companies: List of company dictionaries

    Returns:
        List of dictionaries with company info and URLs
    """
    # Group companies by ATS platform for more targeted prompting
    greenhouse_companies = [c for c in companies if c["platform"].lower() == "greenhouse"]
    workday_companies = [c for c in companies if c["platform"].lower() == "workday"]
    bamboohr_companies = [c for c in companies if c["platform"].lower() == "bamboohr"]
    icims_companies = [c for c in companies if c["platform"].lower() == "icims"]
    jobvite_companies = [c for c in companies if c["platform"].lower() == "jobvite"]
    lever_companies = [c for c in companies if c["platform"].lower() == "lever"]
    smartrecruiters_companies = [c for c in companies if c["platform"].lower() == "smartrecruiters"]
    other_companies = [c for c in companies if c["platform"].lower() not in ["greenhouse", "workday", "bamboohr", "icims", "jobvite", "lever", "smartrecruiters"]]

    all_results = []

    # Process Greenhouse companies separately with specific instructions
    if greenhouse_companies:
        greenhouse_results = process_greenhouse_companies(context, gemini, greenhouse_companies)
        all_results.extend(greenhouse_results)

    # Process Workday companies with targeted approach
    if workday_companies:
        workday_results = process_workday_companies(context, gemini, workday_companies)
        all_results.extend(workday_results)

    # Process BambooHR companies with targeted approach
    if bamboohr_companies:
        bamboohr_results = process_bamboohr_companies(context, gemini, bamboohr_companies)
        all_results.extend(bamboohr_results)

    # Process iCIMS companies with targeted approach
    if icims_companies:
        icims_results = process_icims_companies(context, gemini, icims_companies)
        all_results.extend(icims_results)

    # Process Jobvite companies with targeted approach
    if jobvite_companies:
        jobvite_results = process_jobvite_companies(context, gemini, jobvite_companies)
        all_results.extend(jobvite_results)

    # Process Lever companies with targeted approach
    if lever_companies:
        lever_results = process_lever_companies(context, gemini, lever_companies)
        all_results.extend(lever_results)

    # Process SmartRecruiters companies with targeted approach
    if smartrecruiters_companies:
        smartrecruiters_results = process_smartrecruiters_companies(context, gemini, smartrecruiters_companies)
        all_results.extend(smartrecruiters_results)

    # Process other companies with general instructions
    if other_companies:
        other_results = process_other_companies(context, gemini, other_companies)
        all_results.extend(other_results)

    return all_results

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
    - https://job-boards.greenhouse.io/[tenant]/jobs/
    - https://boards.greenhouse.io/[tenant]/jobs/
    - https://boards.greenhouse.io/embed/job_board?for=[tenant]

    For example:
    - Instacart: https://boards.greenhouse.io/embed/job_board?for=Instacart
    - Dropbox: https://boards.greenhouse.io/embed/job_board?for=dropbox
    - Other companies may use direct tenant URLs like: https://boards.greenhouse.io/company/jobs/

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
        "ats_url": "https://boards.greenhouse.io/exampleinc/jobs/",
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
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing Greenhouse companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process Greenhouse companies after multiple attempts")

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

def process_other_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies using ATS platforms other than Greenhouse."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"],
        "platform": company["platform"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for these companies using various ATS (Applicant Tracking Systems).

    For each company, I've provided:
    - Company name
    - Industry
    - The ATS they use (e.g., Workday, Lever, etc.)

    IMPORTANT: DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists and you've verified it.
    It's better to return null than an incorrect URL.

    For each company, I need TWO distinct URLs:

    1. ATS_URL: The specific URL to their jobs on the third-party ATS platform they use
       For example:
       - Workday: https://company.wd5.myworkdayjobs.com/careers
       - Lever: https://jobs.lever.co/company

    2. CAREER_URL: Direct URL to their own company careers/jobs page as a fallback

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://jobs.lever.co/exampleinc",
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
                context.log.info(f"Sending batch of {len(companies)} non-Greenhouse companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing non-Greenhouse companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process non-Greenhouse companies after multiple attempts")

def parse_gemini_response(
    context: AssetExecutionContext,
    response_text: str,
    companies: List[Dict]
) -> List[Dict]:
    """Parse Gemini response and extract URLs."""
    response_text = response_text.strip()

    # Find the JSON array in the text
    json_start = response_text.find('[')
    json_end = response_text.rfind(']') + 1

    if json_start == -1 or json_end <= json_start:
        raise ValueError("Could not find JSON array in Gemini response")

    json_str = response_text[json_start:json_end]

    try:
        url_data = json.loads(json_str)

        # Create a lookup dictionary of company name to URLs
        company_urls = {}
        for item in url_data:
            if "company_name" in item:
                company_urls[item["company_name"]] = {
                    "ats_url": item.get("ats_url"),
                    "career_url": item.get("career_url")
                }

        # Map the URLs back to the original company objects
        results = []
        for company in companies:
            company_name = company["company_name"]
            urls = company_urls.get(company_name, {})

            ats_url = urls.get("ats_url")
            career_url = urls.get("career_url")

            # Handle URLs that need normalization
            if ats_url and ats_url != "null" and not isinstance(ats_url, bool) and not ats_url.startswith(("http://", "https://")):
                ats_url = "https://" + ats_url

            if career_url and career_url != "null" and not isinstance(career_url, bool) and not career_url.startswith(("http://", "https://")):
                career_url = "https://" + career_url

            # Replace "null" string with None for DB storage
            if ats_url == "null" or ats_url is None or isinstance(ats_url, bool):
                ats_url = None

            if career_url == "null" or career_url is None or isinstance(career_url, bool):
                career_url = None

            results.append({
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": company["platform"],
                "ats_url": ats_url,
                "career_url": career_url
            })

        return results

    except json.JSONDecodeError as e:
        context.log.error(f"Failed to parse JSON from response: {str(e)}")
        context.log.error(f"Response: {response_text}")
        raise

def find_company_urls_individual(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    company_name: str,
    company_industry: str,
    platform: str,
    max_retries: int = 2,
    retry_delay: int = 2
) -> Dict[str, Optional[str]]:
    """
    Use Gemini API to find both ATS job board URL and career URL for a single company.

    Returns a dictionary with both URL types.
    """

    # Different prompt based on the platform
    if platform.lower() == "greenhouse":
        prompt = f"""
        Find accurate job board URLs for this company that uses Greenhouse as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: Greenhouse job board URLs follow one of these specific patterns:
        - https://job-boards.greenhouse.io/[tenant]/jobs/
        - https://boards.greenhouse.io/[tenant]/jobs/
        - https://boards.greenhouse.io/embed/job_board?for=[tenant]

        For example:
        - Instacart: https://boards.greenhouse.io/embed/job_board?for=Instacart
        - Dropbox: https://boards.greenhouse.io/embed/job_board?for=dropbox
        - Other companies use direct tenant URLs like: https://boards.greenhouse.io/company/jobs/

        The key is to identify the correct tenant ID in the URL, which is usually
        a simplified version of the company name (lowercase, no spaces).

        Some companies integrate Greenhouse into their own career sites, so also check
        for embedded Greenhouse job boards on company career pages.

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Greenhouse job board URL (following one of the patterns above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://boards.greenhouse.io/companyname/jobs/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "workday":
        prompt = f"""
        Find accurate job board URLs for this company that uses Workday as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: Workday job board URLs follow this specific pattern:
        https://[tenant].wd1.myworkdayjobs.com/[locale]/[site-name]

        For example:
        - CoStar: https://costar.wd1.myworkdayjobs.com/en-US/CoStarCareers
        - TBK Bank: https://tbkbank.wd1.myworkdayjobs.com/en-US/tpay
        - Go Limitless: https://golimitless.wd1.myworkdayjobs.com/en-US/Staff_External_career_site

        The key is to identify the correct tenant name (usually derived from the company name)
        and the site name (often a variation of the company name or careers).

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Workday job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://tenant.wd1.myworkdayjobs.com/en-US/SiteName",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "bamboohr":
        prompt = f"""
        Find accurate job board URLs for this company that uses BambooHR as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: BambooHR job board URLs follow this specific pattern:
        https://[tenant].bamboohr.com/careers

        For example:
        - Example Company: https://examplecompany.bamboohr.com/careers
        - Tech Startup: https://techstartup.bamboohr.com/careers

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (lowercase, no spaces, no special characters).

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific BambooHR job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://tenant.bamboohr.com/careers",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "icims":
        prompt = f"""
        Find accurate job board URLs for this company that uses iCIMS as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: iCIMS job board URLs follow this specific pattern:
        https://[tenant].icims.com/jobs/

        For example:
        - Example Company: https://examplecompany.icims.com/jobs/
        - Tech Company: https://techcompany.icims.com/jobs/

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (lowercase, no spaces, no special characters).

        Some companies may use:
        - Careers.[company].com that redirects to an iCIMS portal
        - jobs.[company].com that uses iCIMS integration
        - Variations like careers-icims.[company].com

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific iCIMS job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://tenant.icims.com/jobs/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "jobvite":
        prompt = f"""
        Find accurate job board URLs for this company that uses Jobvite as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

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

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Jobvite job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://jobs.jobvite.com/tenant/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "lever":
        prompt = f"""
        Find accurate job board URLs for this company that uses Lever as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

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

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Lever job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://jobs.lever.co/tenant/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "smartrecruiters":
        prompt = f"""
        Find accurate job board URLs for this company that uses SmartRecruiters as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: SmartRecruiters job board URLs follow one of these specific patterns:
        - https://jobs.smartrecruiters.com/[tenant]/
        - https://careers.smartrecruiters.com/[tenant]/

        For example:
        - Example Company: https://jobs.smartrecruiters.com/ExampleCompany/
        - Tech Startup: https://careers.smartrecruiters.com/TechStartup/

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (may include capitalization, and may not have spaces).

        Some companies may use:
        - Careers.[company].com that redirects to a SmartRecruiters portal
        - jobs.[company].com that integrates with SmartRecruiters
        - Company websites with embedded SmartRecruiters job boards

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific SmartRecruiters job board URL (following one of the patterns above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://jobs.smartrecruiters.com/Tenant/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    else:
        prompt = f"""
        Find accurate job board URLs for this company.

        Company Name: "{company_name}"
        Industry: {company_industry}
        ATS Platform Used: {platform.upper()}

        IMPORTANT: DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.
        It's better to return null than an incorrect URL.

        I need TWO distinct URLs:

        1. The specific URL to their jobs on the {platform.upper()} platform
           For example:
           - Workday: https://company.wd5.myworkdayjobs.com/careers
           - Lever: https://jobs.lever.co/company
           - BambooHR: https://company.bamboohr.com/careers
           - Greenhouse: https://boards.greenhouse.io/company/jobs/
           - iCIMS: https://tenant.icims.com/jobs/
           - Jobvite: https://jobs.jobvite.com/tenant/
           - SmartRecruiters: https://jobs.smartrecruiters.com/Tenant/

        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://job-boards.platform.io/companyname",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """

    for attempt in range(max_retries):
        try:
            with gemini.get_model(context) as model:
                response = model.generate_content(prompt)
                response_text = response.text.strip()

                try:
                    # Try to find and extract JSON from the response
                    json_start = response_text.find('{')
                    json_end = response_text.rfind('}') + 1

                    if json_start != -1 and json_end > json_start:
                        json_str = response_text[json_start:json_end]
                        data = json.loads(json_str)

                        # Normalize URLs
                        ats_url = data.get("ats_url")
                        career_url = data.get("career_url")

                        if ats_url and ats_url != "null" and not isinstance(ats_url, bool) and not ats_url.startswith(("http://", "https://")):
                            ats_url = "https://" + ats_url

                        if career_url and career_url != "null" and not isinstance(career_url, bool) and not career_url.startswith(("http://", "https://")):
                            career_url = "https://" + career_url

                        # Replace "null" string with None
                        if ats_url == "null" or ats_url is None or isinstance(ats_url, bool):
                            ats_url = None

                        if career_url == "null" or career_url is None or isinstance(career_url, bool):
                            career_url = None

                        return {
                            "ats_url": ats_url,
                            "career_url": career_url
                        }
                    else:
                        raise ValueError("Could not find JSON object in response")

                except (json.JSONDecodeError, ValueError) as e:
                    if attempt == max_retries - 1:
                        context.log.error(f"Failed to parse response for {company_name}: {str(e)}")
                        context.log.error(f"Response: {response_text}")
                        return {"ats_url": None, "career_url": None}

        except Exception as e:
            context.log.error(f"Error on attempt {attempt+1} for {company_name}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return {"ats_url": None, "career_url": None}

    return {"ats_url": None, "career_url": None}

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

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def greenhouse_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Specialized asset for discovering Greenhouse ATS job board URLs.

    This asset focuses exclusively on companies using Greenhouse ATS to find their job board URLs
    following the patterns:
    - https://job-boards.greenhouse.io/[tenant]/jobs/
    - https://boards.greenhouse.io/[tenant]/jobs/
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
    checkpoint_file = checkpoint_dir / "greenhouse_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "greenhouse_url_discovery_failed.csv"

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
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Loaded {len(all_companies)} Greenhouse companies from CSV files")

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

    context.log.info(f"Processed {total_companies} Greenhouse companies")
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
    deps=["initialize_db", "greenhouse_company_urls"]
)
def retry_failed_greenhouse_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retries finding career URLs for Greenhouse companies that previously failed.

    This asset processes the failed companies with a more conservative approach,
    using individual processing and additional retries.
    """
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    failed_companies_file = checkpoint_dir / "greenhouse_url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed Greenhouse companies file found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_df = pd.read_csv(failed_companies_file)
        failed_companies = failed_df.to_dict("records")
        context.log.info(f"Found {len(failed_companies)} failed Greenhouse companies to retry")
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
            context.log.info(f"Retrying Greenhouse company: {company['company_name']}")
            urls = find_company_urls_individual(
                context=context,
                gemini=gemini,
                company_name=company["company_name"],
                company_industry=company["company_industry"],
                platform="greenhouse",  # Ensure platform is set correctly
                max_retries=3  # More retries for failed companies
            )

            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": "greenhouse",
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
            context.log.error(f"Still failed for Greenhouse company {company['company_name']}: {str(e)}")
            company["error"] = str(e)
            still_failed.append(company)

    # Update the failed companies file with only those that still failed
    if still_failed:
        pd.DataFrame(still_failed).to_csv(failed_companies_file, index=False)
        context.log.info(f"Updated failed companies file with {len(still_failed)} remaining failures")
    else:
        # If all were successful, remove the failed companies file
        failed_companies_file.unlink(missing_ok=True)
        context.log.info("All failed Greenhouse companies successfully processed, removed failed companies file")

    # Return the successfully processed results
    if not results:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    return pd.DataFrame(results)

def process_bamboohr_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies specifically using BambooHR ATS."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for the following companies that use BambooHR as their ATS (Applicant Tracking System).

    IMPORTANT: BambooHR job board URLs follow this specific pattern:
    https://[tenant].bamboohr.com/careers

    For example:
    - Example Company: https://examplecompany.bamboohr.com/careers
    - Tech Startup: https://techstartup.bamboohr.com/careers

    The key is to identify the correct tenant name, which is usually a simplified version
    of the company name (lowercase, no spaces, no special characters).

    DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists. It's better to return null than an incorrect URL.

    For each company, I need:
    1. ATS_URL: The specific BambooHR job board URL (following the pattern above)
    2. CAREER_URL: Direct URL to their own company careers/jobs page

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://exampleinc.bamboohr.com/careers",
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
                context.log.info(f"Sending batch of {len(companies)} BambooHR companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing BambooHR companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process BambooHR companies after multiple attempts")

def process_icims_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies specifically using iCIMS ATS."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for the following companies that use iCIMS as their ATS (Applicant Tracking System).

    IMPORTANT: iCIMS job board URLs follow this specific pattern:
    https://[tenant].icims.com/jobs/

    For example:
    - Example Company: https://examplecompany.icims.com/jobs/
    - Tech Company: https://techcompany.icims.com/jobs/

    The key is to identify the correct tenant name, which is usually a simplified version
    of the company name (lowercase, no spaces, no special characters).

    Some companies may use:
    - Careers.[company].com that redirects to an iCIMS portal
    - jobs.[company].com that uses iCIMS integration
    - Variations like careers-icims.[company].com

    DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists. It's better to return null than an incorrect URL.

    For each company, I need:
    1. ATS_URL: The specific iCIMS job board URL (following the pattern above)
    2. CAREER_URL: Direct URL to their own company careers/jobs page

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://exampleinc.icims.com/jobs/",
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
                context.log.info(f"Sending batch of {len(companies)} iCIMS companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing iCIMS companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process iCIMS companies after multiple attempts")

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
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing Jobvite companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process Jobvite companies after multiple attempts")

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

def process_smartrecruiters_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies specifically using SmartRecruiters ATS."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for the following companies that use SmartRecruiters as their ATS (Applicant Tracking System).

    IMPORTANT: SmartRecruiters job board URLs follow one of these specific patterns:
    - https://jobs.smartrecruiters.com/[tenant]/
    - https://careers.smartrecruiters.com/[tenant]/

    For example:
    - Example Company: https://jobs.smartrecruiters.com/ExampleCompany/
    - Tech Startup: https://careers.smartrecruiters.com/TechStartup/

    The key is to identify the correct tenant name, which is usually a simplified version
    of the company name (may include capitalization, and may not have spaces).

    Some companies may use:
    - Careers.[company].com that redirects to a SmartRecruiters portal
    - jobs.[company].com that integrates with SmartRecruiters
    - Company websites with embedded SmartRecruiters job boards

    DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists. It's better to return null than an incorrect URL.

    For each company, I need:
    1. ATS_URL: The specific SmartRecruiters job board URL (following one of the patterns above)
    2. CAREER_URL: Direct URL to their own company careers/jobs page

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://jobs.smartrecruiters.com/ExampleInc/",
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
                context.log.info(f"Sending batch of {len(companies)} SmartRecruiters companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing SmartRecruiters companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process SmartRecruiters companies after multiple attempts")

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def bamboohr_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Specialized asset for discovering BambooHR ATS job board URLs.

    This asset focuses exclusively on companies using BambooHR ATS to find their job board URLs
    following the pattern: https://[tenant].bamboohr.com/careers
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
    checkpoint_file = checkpoint_dir / "bamboohr_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "bamboohr_url_discovery_failed.csv"

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
                    # Only include BambooHR companies
                    if platform.lower() == "bamboohr" or (row.get("platform", "").lower() == "bamboohr"):
                        company = {
                            "company_name": row["company_name"],
                            "company_industry": row["company_industry"],
                            "platform": "bamboohr"  # Ensure platform is set correctly
                        }
                        all_companies.append(company)
        except Exception as e:
            context.log.error(f"Error reading file {file_path}: {str(e)}")

    if not all_companies:
        context.log.warning("No BambooHR company data was loaded from CSV files")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Loaded {len(all_companies)} BambooHR companies from CSV files")

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
    context.log.info(f"{len(companies_to_process)} BambooHR companies remaining to process")

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
            # Process BambooHR companies with targeted approach
            batch_results = process_bamboohr_companies(context, gemini, batch)

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

    context.log.info(f"Processed {total_companies} BambooHR companies")
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
    deps=["initialize_db", "bamboohr_company_urls"]
)
def retry_failed_bamboohr_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retries finding career URLs for BambooHR companies that previously failed.

    This asset processes the failed companies with a more conservative approach,
    using individual processing and additional retries.
    """
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    failed_companies_file = checkpoint_dir / "bamboohr_url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed BambooHR companies file found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_df = pd.read_csv(failed_companies_file)
        failed_companies = failed_df.to_dict("records")
        context.log.info(f"Found {len(failed_companies)} failed BambooHR companies to retry")
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
            context.log.info(f"Retrying BambooHR company: {company['company_name']}")
            urls = find_company_urls_individual(
                context=context,
                gemini=gemini,
                company_name=company["company_name"],
                company_industry=company["company_industry"],
                platform="bamboohr",  # Ensure platform is set correctly
                max_retries=3  # More retries for failed companies
            )

            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": "bamboohr",
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
            context.log.error(f"Still failed for BambooHR company {company['company_name']}: {str(e)}")
            company["error"] = str(e)
            still_failed.append(company)

    # Update the failed companies file with only those that still failed
    if still_failed:
        pd.DataFrame(still_failed).to_csv(failed_companies_file, index=False)
        context.log.info(f"Updated failed companies file with {len(still_failed)} remaining failures")
    else:
        # If all were successful, remove the failed companies file
        failed_companies_file.unlink(missing_ok=True)
        context.log.info("All failed BambooHR companies successfully processed, removed failed companies file")

    # Return the successfully processed results
    if not results:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    return pd.DataFrame(results)

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def icims_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Specialized asset for discovering iCIMS ATS job board URLs.

    This asset focuses exclusively on companies using iCIMS ATS to find their job board URLs
    following the pattern: https://[tenant].icims.com/jobs/
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
    checkpoint_file = checkpoint_dir / "icims_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "icims_url_discovery_failed.csv"

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
                    # Only include iCIMS companies
                    if platform.lower() == "icims" or (row.get("platform", "").lower() == "icims"):
                        company = {
                            "company_name": row["company_name"],
                            "company_industry": row["company_industry"],
                            "platform": "icims"  # Ensure platform is set correctly
                        }
                        all_companies.append(company)
        except Exception as e:
            context.log.error(f"Error reading file {file_path}: {str(e)}")

    if not all_companies:
        context.log.warning("No iCIMS company data was loaded from CSV files")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Loaded {len(all_companies)} iCIMS companies from CSV files")

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
    context.log.info(f"{len(companies_to_process)} iCIMS companies remaining to process")

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
            # Process iCIMS companies with targeted approach
            batch_results = process_icims_companies(context, gemini, batch)

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

    context.log.info(f"Processed {total_companies} iCIMS companies")
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
    deps=["initialize_db", "icims_company_urls"]
)
def retry_failed_icims_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retries finding career URLs for iCIMS companies that previously failed.

    This asset processes the failed companies with a more conservative approach,
    using individual processing and additional retries.
    """
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    failed_companies_file = checkpoint_dir / "icims_url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed iCIMS companies file found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_df = pd.read_csv(failed_companies_file)
        failed_companies = failed_df.to_dict("records")
        context.log.info(f"Found {len(failed_companies)} failed iCIMS companies to retry")
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
            context.log.info(f"Retrying iCIMS company: {company['company_name']}")
            urls = find_company_urls_individual(
                context=context,
                gemini=gemini,
                company_name=company["company_name"],
                company_industry=company["company_industry"],
                platform="icims",  # Ensure platform is set correctly
                max_retries=3  # More retries for failed companies
            )

            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": "icims",
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
            context.log.error(f"Still failed for iCIMS company {company['company_name']}: {str(e)}")
            company["error"] = str(e)
            still_failed.append(company)

    # Update the failed companies file with only those that still failed
    if still_failed:
        pd.DataFrame(still_failed).to_csv(failed_companies_file, index=False)
        context.log.info(f"Updated failed companies file with {len(still_failed)} remaining failures")
    else:
        # If all were successful, remove the failed companies file
        failed_companies_file.unlink(missing_ok=True)
        context.log.info("All failed iCIMS companies successfully processed, removed failed companies file")

    # Return the successfully processed results
    if not results:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    return pd.DataFrame(results)

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db", "icims_company_urls"]
)
def retry_failed_icims_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retries finding career URLs for iCIMS companies that previously failed.

    This asset processes the failed companies with a more conservative approach,
    using individual processing and additional retries.
    """
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    failed_companies_file = checkpoint_dir / "icims_url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed iCIMS companies file found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_df = pd.read_csv(failed_companies_file)
        failed_companies = failed_df.to_dict("records")
        context.log.info(f"Found {len(failed_companies)} failed iCIMS companies to retry")
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
            context.log.info(f"Retrying iCIMS company: {company['company_name']}")
            urls = find_company_urls_individual(
                context=context,
                gemini=gemini,
                company_name=company["company_name"],
                company_industry=company["company_industry"],
                platform="icims",  # Ensure platform is set correctly
                max_retries=3  # More retries for failed companies
            )

            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": "icims",
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
            context.log.error(f"Still failed for iCIMS company {company['company_name']}: {str(e)}")
            company["error"] = str(e)
            still_failed.append(company)

    # Update the failed companies file with only those that still failed
    if still_failed:
        pd.DataFrame(still_failed).to_csv(failed_companies_file, index=False)
        context.log.info(f"Updated failed companies file with {len(still_failed)} remaining failures")
    else:
        # If all were successful, remove the failed companies file
        failed_companies_file.unlink(missing_ok=True)
        context.log.info("All failed iCIMS companies successfully processed, removed failed companies file")

    # Return the successfully processed results
    if not results:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    return pd.DataFrame(results)

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
    Retries finding career URLs for Jobvite companies that previously failed.

    This asset processes the failed companies with a more conservative approach,
    using individual processing and additional retries.
    """
    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    failed_companies_file = checkpoint_dir / "jobvite_url_discovery_failed.csv"

    if not failed_companies_file.exists():
        context.log.info("No failed Jobvite companies file found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    try:
        failed_df = pd.read_csv(failed_companies_file)
        failed_companies = failed_df.to_dict("records")
        context.log.info(f"Found {len(failed_companies)} failed Jobvite companies to retry")
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
            context.log.info(f"Retrying Jobvite company: {company['company_name']}")
            urls = find_company_urls_individual(
                context=context,
                gemini=gemini,
                company_name=company["company_name"],
                company_industry=company["company_industry"],
                platform="jobvite",  # Ensure platform is set correctly
                max_retries=3  # More retries for failed companies
            )

            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": "jobvite",
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
            context.log.error(f"Still failed for Jobvite company {company['company_name']}: {str(e)}")
            company["error"] = str(e)
            still_failed.append(company)

    # Update the failed companies file with only those that still failed
    if still_failed:
        pd.DataFrame(still_failed).to_csv(failed_companies_file, index=False)
        context.log.info(f"Updated failed companies file with {len(still_failed)} remaining failures")
    else:
        # If all were successful, remove the failed companies file
        failed_companies_file.unlink(missing_ok=True)
        context.log.info("All failed Jobvite companies successfully processed, removed failed companies file")

    # Return the successfully processed results
    if not results:
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    return pd.DataFrame(results)

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
    try:
        company_data_file = datasource_dir / "company_data.csv"
        if not company_data_file.exists():
            context.log.error(f"Company data file not found: {company_data_file}")
            return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

        companies_df = pd.read_csv(company_data_file)
        # Filter for companies with unknown platform or specifically marked as Lever
        companies_df = companies_df[
            (companies_df["platform"].isnull()) |
            (companies_df["platform"].str.lower() == "lever") |
            (companies_df["platform"].str.lower() == "unknown")
        ].copy()

        context.log.info(f"Found {len(companies_df)} potential Lever companies to process")
    except Exception as e:
        context.log.error(f"Error loading company data: {str(e)}")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Load checkpoint data if it exists
    try:
        if checkpoint_file.exists():
            checkpoint_df = pd.read_csv(checkpoint_file)
            context.log.info(f"Loaded checkpoint with {len(checkpoint_df)} previously processed companies")

            # Filter out already processed companies
            companies_df = companies_df[~companies_df["company_name"].isin(checkpoint_df["company_name"])]
            context.log.info(f"After filtering, {len(companies_df)} companies remain to be processed")
        else:
            checkpoint_df = pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])
            context.log.info("No checkpoint file found, starting fresh")
    except Exception as e:
        context.log.error(f"Error loading checkpoint data: {str(e)}")
        checkpoint_df = pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Process companies in batches
    batch_size = 20
    all_results = []
    failed_companies = []

    for i in range(0, len(companies_df), batch_size):
        batch = companies_df.iloc[i:i+batch_size].to_dict('records')
        context.log.info(f"Processing batch {i//batch_size + 1} of {(len(companies_df) + batch_size - 1) // batch_size}")

        try:
            # Process using specialized function for Lever
            results = process_lever_companies(context, gemini, batch)
            valid_results = []

            # Validate URLs and update platform info
            for result in results:
                result["platform"] = "lever" if result.get("ats_url") else "unknown"
                valid_results.append(result)

            # Save results and update checkpoint
            all_results.extend(valid_results)

            # Identify failed companies (those without an ats_url)
            failed_in_batch = [r for r in valid_results if not r.get("ats_url")]
            failed_companies.extend(failed_in_batch)

            # Update checkpoint file with each successful batch
            batch_df = pd.DataFrame(valid_results)
            updated_checkpoint = pd.concat([checkpoint_df, batch_df], ignore_index=True)
            updated_checkpoint.to_csv(checkpoint_file, index=False)
            checkpoint_df = updated_checkpoint

            context.log.info(f"Completed batch {i//batch_size + 1}. Found {len([r for r in valid_results if r.get('ats_url')])} Lever URLs")

        except Exception as e:
            context.log.error(f"Error processing batch: {str(e)}")
            # Add companies to failed list so they can be retried later
            for company in batch:
                company["platform"] = "unknown"
                company["ats_url"] = None
                company["career_url"] = None
                company["url_verified"] = False
                failed_companies.append(company)

    # Save failed companies for retry
    if failed_companies:
        pd.DataFrame(failed_companies).to_csv(failed_companies_file, index=False)
        context.log.info(f"Saved {len(failed_companies)} failed companies for retry")

    # Combine checkpoint data with new results
    final_results = pd.DataFrame(all_results)
    if len(final_results) == 0:
        context.log.info("No new Lever URLs found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Found {len(final_results[final_results['platform'] == 'lever'])} companies using Lever ATS")
    final_results["url_verified"] = final_results["ats_url"].notna()

    return final_results

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
                successful_retries.append({
                    "company_name": company["company_name"],
                    "company_industry": company.get("company_industry", "Unknown"),
                    "platform": "lever",
                    "ats_url": result.get("ats_url"),
                    "career_url": result.get("career_url"),
                    "url_verified": True
                })
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

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db"]
)
def smartrecruiters_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Specialized asset for discovering SmartRecruiters ATS job board URLs.

    This asset focuses exclusively on companies using SmartRecruiters ATS to find their job board URLs
    following the pattern: https://jobs.smartrecruiters.com/[tenant]/ or https://careers.smartrecruiters.com/[tenant]/
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
    checkpoint_file = checkpoint_dir / "smartrecruiters_url_discovery_checkpoint.csv"
    failed_companies_file = checkpoint_dir / "smartrecruiters_url_discovery_failed.csv"

    context.log.info(f"Looking for company data in: {datasource_dir}")
    context.log.info(f"Using checkpoint file: {checkpoint_file}")

    # Check if directory exists
    if not datasource_dir.exists():
        context.log.error(f"Data source directory not found: {datasource_dir}")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Load all company data
    try:
        company_data_file = datasource_dir / "company_data.csv"
        if not company_data_file.exists():
            context.log.error(f"Company data file not found: {company_data_file}")
            return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

        companies_df = pd.read_csv(company_data_file)
        # Filter for companies with unknown platform or specifically marked as SmartRecruiters
        companies_df = companies_df[
            (companies_df["platform"].isnull()) |
            (companies_df["platform"].str.lower() == "smartrecruiters") |
            (companies_df["platform"].str.lower() == "unknown")
        ].copy()

        context.log.info(f"Found {len(companies_df)} potential SmartRecruiters companies to process")
    except Exception as e:
        context.log.error(f"Error loading company data: {str(e)}")
        # Return empty DataFrame with the correct structure
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Load checkpoint data if it exists
    try:
        if checkpoint_file.exists():
            checkpoint_df = pd.read_csv(checkpoint_file)
            context.log.info(f"Loaded checkpoint with {len(checkpoint_df)} previously processed companies")

            # Filter out already processed companies
            companies_df = companies_df[~companies_df["company_name"].isin(checkpoint_df["company_name"])]
            context.log.info(f"After filtering, {len(companies_df)} companies remain to be processed")
        else:
            checkpoint_df = pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])
            context.log.info("No checkpoint file found, starting fresh")
    except Exception as e:
        context.log.error(f"Error loading checkpoint data: {str(e)}")
        checkpoint_df = pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    # Process companies in batches
    batch_size = 20
    all_results = []
    failed_companies = []

    for i in range(0, len(companies_df), batch_size):
        batch = companies_df.iloc[i:i+batch_size].to_dict('records')
        context.log.info(f"Processing batch {i//batch_size + 1} of {(len(companies_df) + batch_size - 1) // batch_size}")

        try:
            # Process using specialized function for SmartRecruiters
            results = process_smartrecruiters_companies(context, gemini, batch)
            valid_results = []

            # Validate URLs and update platform info
            for result in results:
                result["platform"] = "smartrecruiters" if result.get("ats_url") else "unknown"
                valid_results.append(result)

            # Save results and update checkpoint
            all_results.extend(valid_results)

            # Identify failed companies (those without an ats_url)
            failed_in_batch = [r for r in valid_results if not r.get("ats_url")]
            failed_companies.extend(failed_in_batch)

            # Update checkpoint file with each successful batch
            batch_df = pd.DataFrame(valid_results)
            updated_checkpoint = pd.concat([checkpoint_df, batch_df], ignore_index=True)
            updated_checkpoint.to_csv(checkpoint_file, index=False)
            checkpoint_df = updated_checkpoint

            context.log.info(f"Completed batch {i//batch_size + 1}. Found {len([r for r in valid_results if r.get('ats_url')])} SmartRecruiters URLs")

        except Exception as e:
            context.log.error(f"Error processing batch: {str(e)}")
            # Add companies to failed list so they can be retried later
            for company in batch:
                company["platform"] = "unknown"
                company["ats_url"] = None
                company["career_url"] = None
                company["url_verified"] = False
                failed_companies.append(company)

    # Save failed companies for retry
    if failed_companies:
        pd.DataFrame(failed_companies).to_csv(failed_companies_file, index=False)
        context.log.info(f"Saved {len(failed_companies)} failed companies for retry")

    # Combine checkpoint data with new results
    final_results = pd.DataFrame(all_results)
    if len(final_results) == 0:
        context.log.info("No new SmartRecruiters URLs found")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Found {len(final_results[final_results['platform'] == 'smartrecruiters'])} companies using SmartRecruiters ATS")
    final_results["url_verified"] = final_results["ats_url"].notna()

    return final_results

@asset(
    group_name="url_discovery",
    compute_kind="gemini",
    io_manager_key="duckdb",
    deps=["initialize_db", "smartrecruiters_company_urls"]
)
def retry_failed_smartrecruiters_company_urls(context: AssetExecutionContext, gemini: GeminiResource) -> pd.DataFrame:
    """
    Retry processing failed companies from the smartrecruiters_company_urls asset.

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
    failed_companies_file = checkpoint_dir / "smartrecruiters_url_discovery_failed.csv"

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
                "smartrecruiters",
                max_retries=3,
                retry_delay=3
            )

            # Add to successful retries if we found an ATS URL
            if result and result.get("ats_url"):
                successful_retries.append({
                    "company_name": company["company_name"],
                    "company_industry": company.get("company_industry", "Unknown"),
                    "platform": "smartrecruiters",
                    "ats_url": result.get("ats_url"),
                    "career_url": result.get("career_url"),
                    "url_verified": True
                })
                context.log.info(f"Successfully found SmartRecruiters URL for {company['company_name']}")
            else:
                context.log.info(f"Still unable to find SmartRecruiters URL for {company['company_name']}")

        except Exception as e:
            context.log.error(f"Error retrying company {company['company_name']}: {str(e)}")

    # Create DataFrame from successful retries
    retry_results = pd.DataFrame(successful_retries)

    if len(retry_results) == 0:
        context.log.info("No companies were successfully retried")
        return pd.DataFrame(columns=["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"])

    context.log.info(f"Successfully retried {len(retry_results)} companies")
    return retry_results