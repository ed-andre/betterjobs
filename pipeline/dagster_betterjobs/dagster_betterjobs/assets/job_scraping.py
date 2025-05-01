import time
import pandas as pd
from typing import Dict, List, Optional
from dagster import asset, AssetExecutionContext, Config, get_dagster_logger

from dagster_betterjobs.scrapers import create_scraper

logger = get_dagster_logger()

class JobScrapingConfig(Config):
    search_keyword: str
    location: Optional[str] = None
    platforms: List[str] = ["workday", "greenhouse"]
    max_companies_per_platform: Optional[int] = None
    rate_limit: float = 2.0
    max_retries: int = 3
    retry_delay: int = 2

class JobSearchConfig(Config):
    search_keyword: str

@asset(
    group_name="job_scraping",
    compute_kind="web_scraping",
    required_resource_keys={"duckdb_resource"},
    deps=["initialize_db"]
)
def scrape_jobs(context: AssetExecutionContext, config: JobScrapingConfig) -> Dict:
    """
    Scrapes job listings from configured companies based on search criteria.

    This asset uses the company URLs stored in the database to scrape job
    listings matching the specified search criteria. Results are stored
    in the jobs table in the database.
    """
    conn = context.resources.duckdb_resource.get_connection()

    # Get companies with valid URLs for the specified platforms
    platform_filter = "', '".join(config.platforms)
    query = f"""
    SELECT company_id, company_name, company_industry, platform, career_url
    FROM companies
    WHERE platform IN ('{platform_filter}')
    AND career_url IS NOT NULL
    AND career_url != 'Not found'
    ORDER BY company_name
    """

    # Apply limit if specified
    if config.max_companies_per_platform:
        query = f"""
        WITH ranked_companies AS (
            SELECT
                company_id, company_name, company_industry, platform, career_url,
                ROW_NUMBER() OVER (PARTITION BY platform ORDER BY company_name) as rank
            FROM companies
            WHERE platform IN ('{platform_filter}')
            AND career_url IS NOT NULL
            AND career_url != 'Not found'
        )
        SELECT company_id, company_name, company_industry, platform, career_url
        FROM ranked_companies
        WHERE rank <= {config.max_companies_per_platform}
        ORDER BY platform, company_name
        """

    # Execute query and load results
    companies_df = conn.execute(query).fetch_df()

    total_companies = len(companies_df)
    context.log.info(f"Found {total_companies} companies to scrape across {len(config.platforms)} platforms")

    # Track statistics
    stats = {
        "total_companies": total_companies,
        "companies_scraped": 0,
        "jobs_found": 0,
        "jobs_stored": 0,
        "errors": 0,
        "platforms": {}
    }

    for platform in config.platforms:
        stats["platforms"][platform] = {
            "companies": 0,
            "jobs": 0,
            "errors": 0
        }

    # Process each company
    for _, company in companies_df.iterrows():
        platform = company["platform"]
        company_name = company["company_name"]
        company_id = company["company_id"]
        career_url = company["career_url"]

        context.log.info(f"Scraping jobs for {company_name} ({platform})")

        try:
            # Create the appropriate scraper
            scraper = create_scraper(
                platform=platform,
                career_url=career_url,
                rate_limit=config.rate_limit,
                max_retries=config.max_retries,
                retry_delay=config.retry_delay
            )

            if not scraper:
                context.log.warning(f"No scraper available for platform: {platform}")
                stats["errors"] += 1
                stats["platforms"][platform]["errors"] += 1
                continue

            # Search for jobs
            jobs = scraper.search_jobs(
                keyword=config.search_keyword,
                location=config.location
            )

            context.log.info(f"Found {len(jobs)} matching jobs for {company_name}")

            # Update stats
            stats["companies_scraped"] += 1
            stats["jobs_found"] += len(jobs)
            stats["platforms"][platform]["companies"] += 1
            stats["platforms"][platform]["jobs"] += len(jobs)

            # Store the jobs in the database
            for job in jobs:
                # Get job details if we only have basic info
                if 'job_description' not in job and 'job_url' in job:
                    try:
                        job_details = scraper.get_job_details(job['job_url'])
                        job.update(job_details)
                    except Exception as e:
                        context.log.warning(f"Error getting job details: {str(e)}")

                # Store the job
                try:
                    # Check if job already exists
                    existing = conn.execute(
                        "SELECT job_id FROM jobs WHERE job_url = ?",
                        (job.get('job_url'),)
                    ).fetchone()

                    if existing:
                        # Update existing job
                        conn.execute(
                            """
                            UPDATE jobs
                            SET job_title = ?,
                                job_description = ?,
                                location = ?,
                                date_posted = ?,
                                date_retrieved = CURRENT_TIMESTAMP,
                                is_active = TRUE
                            WHERE job_url = ?
                            """,
                            (
                                job.get('job_title'),
                                job.get('job_description'),
                                job.get('location'),
                                job.get('date_posted'),
                                job.get('job_url')
                            )
                        )
                    else:
                        # Insert new job
                        conn.execute(
                            """
                            INSERT INTO jobs (
                                company_id, job_title, job_description,
                                job_url, location, date_posted
                            )
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                company_id,
                                job.get('job_title'),
                                job.get('job_description'),
                                job.get('job_url'),
                                job.get('location'),
                                job.get('date_posted')
                            )
                        )
                        stats["jobs_stored"] += 1

                except Exception as e:
                    context.log.error(f"Error storing job: {str(e)}")
                    stats["errors"] += 1

            # Sleep to avoid overwhelming servers
            time.sleep(0.5)

        except Exception as e:
            context.log.error(f"Error scraping {company_name}: {str(e)}")
            stats["errors"] += 1
            stats["platforms"][platform]["errors"] += 1

    # Log summary statistics
    context.log.info(f"Scraping complete. Processed {stats['companies_scraped']} companies")
    context.log.info(f"Found {stats['jobs_found']} jobs, stored {stats['jobs_stored']} new jobs")
    context.log.info(f"Encountered {stats['errors']} errors")

    for platform, platform_stats in stats["platforms"].items():
        context.log.info(f"Platform {platform}: {platform_stats['companies']} companies, {platform_stats['jobs']} jobs, {platform_stats['errors']} errors")

    return stats

@asset(
    group_name="job_scraping",
    compute_kind="database",
    required_resource_keys={"duckdb_resource"},
    deps=["scrape_jobs"]
)
def job_search_results(context: AssetExecutionContext, config: JobSearchConfig) -> pd.DataFrame:
    """
    Retrieves the results of the job search for the specified keyword.

    This asset queries the database for jobs matching the search keyword
    and returns them as a DataFrame, which can be used for display or export.
    """
    conn = context.resources.duckdb_resource.get_connection()

    # Query for jobs matching the keyword
    query = """
    SELECT
        j.job_id,
        c.company_name,
        c.company_industry,
        c.platform,
        j.job_title,
        j.location,
        j.job_url,
        j.date_posted,
        j.date_retrieved
    FROM
        jobs j
    JOIN
        companies c ON j.company_id = c.company_id
    WHERE
        LOWER(j.job_title) LIKE ?
        OR LOWER(j.job_description) LIKE ?
    ORDER BY
        j.date_retrieved DESC,
        c.company_name
    """

    search_pattern = f"%{config.search_keyword.lower()}%"

    # Execute query and load results
    results_df = conn.execute(
        query,
        (search_pattern, search_pattern)
    ).fetch_df()

    context.log.info(f"Found {len(results_df)} jobs matching '{config.search_keyword}'")

    return results_df