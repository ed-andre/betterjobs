from dagster import schedule
from .jobs import job_scraping_job
from .assets.bamboohr_jobs_discovery import bamboohr_jobs_discovery_job, alpha_partitions
from datetime import datetime

# @schedule(
#     cron_schedule="0 0 * * *",  # Run daily at midnight
#     execution_timezone="UTC",
#     job=job_scraping_job,
# )
# def daily_job_scrape_schedule():
#     """Daily job discovery pipeline for data engineer positions."""
#     return {
#         "config": {
#             "ops": {
#                 "scrape_jobs": {
#                     "config": {
#                         "search_keyword": "data engineer",
#                         "location": None,
#                         "platforms": ["workday", "greenhouse"],
#                         "max_companies_per_platform": 20,
#                         "rate_limit": 2.0
#                     }
#                 },
#                 "job_search_results": {
#                     "config": {
#                         "search_keyword": "data engineer"
#                     }
#                 }
#             }
#         }
#     }

# Schedule for BambooHR jobs running at different times throughout the day
@schedule(
    cron_schedule="0 */2 * * *",  # Run every 2 hours
    execution_timezone="US/Eastern",
    job=bamboohr_jobs_discovery_job,
)
def bamboohr_jobs_hourly_schedule(context):
    """BambooHR job discovery that cycles through alphabetical partitions every 2 hours."""
    # Select partition based on current hour
    current_hour = datetime.now().hour
    hour_index = (current_hour // 2) % len(alpha_partitions.get_partition_keys())
    partition_to_run = alpha_partitions.get_partition_keys()[hour_index]

    return {
        "run_key": f"bamboohr_jobs_{partition_to_run}_{context.scheduled_execution_time.strftime('%Y-%m-%d_%H')}",
        "run_config": {
            "ops": {
                "bamboohr_company_jobs_discovery": {
                    "config": {
                        "partition_key": partition_to_run
                    }
                }
            }
        },
        "tags": {"partition": partition_to_run}
    }
