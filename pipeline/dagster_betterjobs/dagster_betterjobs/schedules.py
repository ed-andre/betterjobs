from dagster import schedule
from .jobs import job_scraping_job

@schedule(
    cron_schedule="0 0 * * *",  # Run daily at midnight
    execution_timezone="UTC",
    job=job_scraping_job,
)
def daily_job_scrape_schedule():
    """Schedule that runs the job scraping pipeline daily."""
    return {
        "config": {
            "ops": {
                "scrape_jobs": {
                    "config": {
                        "search_keyword": "data engineer",
                        "location": None,
                        "platforms": ["workday", "greenhouse"],
                        "max_companies_per_platform": 20,
                        "rate_limit": 2.0
                    }
                },
                "job_search_results": {
                    "config": {
                        "search_keyword": "data engineer"
                    }
                }
            }
        }
    }
