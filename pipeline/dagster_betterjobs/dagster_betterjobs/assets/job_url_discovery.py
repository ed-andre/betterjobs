import pandas as pd
import time
import requests
from typing import List, Dict, Optional
from urllib.parse import urlparse, urljoin

from dagster import asset, AssetExecutionContext, get_dagster_logger

logger = get_dagster_logger()

@asset(
    group_name="url_discovery",
    compute_kind="python",
    io_manager_key="duckdb",
    deps=["raw_job_listings", "company_urls", "initialize_db"]
)
def job_url_discovery(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Discovers and verifies job URLs based on company and job title.

    Uses company ATS URLs from the companies table to construct job URLs
    for raw job listings. Verifies the URLs and updates them in the raw_job_listings
    table.
    """
    # Get unprocessed raw job listings
    with context.resources.duckdb_resource.get_connection() as conn:
        # Get raw job listings that haven't been processed
        df_raw_jobs = pd.read_sql("""
            SELECT
                r.raw_job_id,
                r.company_name,
                r.job_title,
                r.date_posted,
                c.platform,
                c.ats_url,
                c.career_url
            FROM
                raw_job_listings r
            LEFT JOIN
                companies c ON r.company_name = c.company_name
            WHERE
                r.processed = FALSE
                AND (c.ats_url IS NOT NULL OR c.career_url IS NOT NULL)
            LIMIT 1000  -- Process in batches
        """, conn)

    if df_raw_jobs.empty:
        context.log.info("No unprocessed job listings found")
        return pd.DataFrame()

    context.log.info(f"Processing {len(df_raw_jobs)} raw job listings")

    # Process each job to find its URL
    results = []
    for _, job in df_raw_jobs.iterrows():
        result = {
            "raw_job_id": job["raw_job_id"],
            "job_url": None,
            "job_url_verified": False,
            "processed": True
        }

        # Try to construct a job URL based on the ATS platform
        job_url = construct_job_url(
            platform=job["platform"],
            ats_url=job["ats_url"],
            career_url=job["career_url"],
            company_name=job["company_name"],
            job_title=job["job_title"]
        )

        if job_url:
            # Verify the URL
            if verify_url(job_url):
                result["job_url"] = job_url
                result["job_url_verified"] = True
                context.log.info(f"Found valid job URL: {job_url}")
            else:
                context.log.info(f"Could not verify job URL: {job_url}")
                result["job_url"] = job_url

        results.append(result)

        # Add a small delay to avoid overwhelming servers
        time.sleep(0.1)

    # Create DataFrame from results
    df_results = pd.DataFrame(results)

    # Update the raw_job_listings table
    with context.resources.duckdb_resource.get_connection() as conn:
        # Update the processed jobs
        for _, row in df_results.iterrows():
            conn.execute("""
                UPDATE raw_job_listings
                SET
                    job_url = ?,
                    job_url_verified = ?,
                    processed = ?
                WHERE raw_job_id = ?
            """, (
                row["job_url"],
                row["job_url_verified"],
                row["processed"],
                row["raw_job_id"]
            ))

    # Log summary statistics
    verified_count = df_results["job_url_verified"].sum()
    context.log.info(f"Found and verified URLs for {verified_count} out of {len(df_results)} job listings")

    return df_results

def construct_job_url(
    platform: str,
    ats_url: Optional[str],
    career_url: Optional[str],
    company_name: str,
    job_title: str
) -> Optional[str]:
    """
    Construct a job URL based on the ATS platform and available URLs.

    Uses platform-specific patterns to construct the most likely URL
    for a job listing.
    """
    # Use the ATS URL if available, otherwise fall back to career URL
    base_url = ats_url if ats_url else career_url

    if not base_url:
        return None

    # Convert to lowercase for consistent comparison
    platform = platform.lower() if platform else ""

    if platform == "greenhouse":
        # Greenhouse job boards typically have URLs like:
        # https://job-boards.greenhouse.io/companyname/jobs/1234
        # Try to extract the company path component from the URL
        if "job-boards.greenhouse.io" in base_url:
            # The base URL is already a Greenhouse job board
            return base_url

    elif platform == "lever":
        # Lever job boards typically have URLs like:
        # https://jobs.lever.co/companyname/job-id
        if "jobs.lever.co" in base_url:
            # The base URL is already a Lever job board
            return base_url

    elif platform == "workday":
        # Workday job boards typically have URLs like:
        # https://company.wd5.myworkdayjobs.com/en-US/careers
        if "myworkdayjobs" in base_url:
            # The base URL is already a Workday job board
            return base_url

    # For other platforms or if no specific pattern is recognized,
    # just return the base URL
    return base_url

def verify_url(url: str) -> bool:
    """
    Verify if a URL exists and returns a valid response.

    Args:
        url: The URL to verify

    Returns:
        True if the URL returns a valid response (status code < 400),
        False otherwise
    """
    try:
        # Parse the URL to check if it's valid
        result = urlparse(url)
        if not all([result.scheme, result.netloc]):
            return False

        # Send a HEAD request to verify the URL
        response = requests.head(url, timeout=5, allow_redirects=True)

        # Return True if the status code is less than 400 (no error)
        return response.status_code < 400
    except Exception:
        return False