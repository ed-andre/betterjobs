from dagster import schedule, RunRequest
from .jobs import (
    full_jobs_discovery_and_search_job,
    full_jobs_discovery_except_icims_job,
    supabase_transport_job,
    discovery_and_transport_job
)
from .assets.bamboohr_jobs_discovery import alpha_partitions
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

# # Schedule for BambooHR jobs running at different times throughout the day
# @schedule(
#     cron_schedule="0 */2 * * *",  # Run every 2 hours
#     execution_timezone="US/Eastern",
#     job=bamboohr_jobs_discovery_job,
# )
# def bamboohr_jobs_hourly_schedule(context):
#     """BambooHR job discovery that runs all alphabetical partitions every 2 hours."""
#     # Get all partition keys from alpha_partitions
#     for partition_key in alpha_partitions.get_partition_keys():
#         # Create a unique run key for each partition
#         run_key = f"bamboohr_jobs_{partition_key}_{context.scheduled_execution_time.strftime('%Y-%m-%d_%H')}"

#         # Yield a RunRequest for each partition
#         yield RunRequest(
#             run_key=run_key,
#             partition_key=partition_key,
#             tags={"partition": partition_key}
#         )

# Schedule for running all job discovery assets (except iCIMS) plus job search
@schedule(
    cron_schedule="0 4 * * *",  # Run daily at 4 AM
    execution_timezone="US/Eastern",
    job=full_jobs_discovery_and_search_job,
)
def full_jobs_discovery_and_search_schedule(context):
    """Schedule that runs all job discovery assets (except iCIMS) plus job search daily."""
    # Get all partition keys from alpha_partitions
    for partition_key in alpha_partitions.get_partition_keys():
        # Create a unique run key for each partition
        run_key = f"full_jobs_discovery_and_search_{partition_key}_{context.scheduled_execution_time.strftime('%Y-%m-%d')}"

        # Yield a RunRequest for each partition
        yield RunRequest(
            run_key=run_key,
            partition_key=partition_key,
            tags={"partition": partition_key}
        )

# Schedule for running all job discovery assets (except for ICMS) plus Supabase transport
@schedule(
    cron_schedule="0 12 * * *",  # Run daily at noon
    execution_timezone="US/Eastern",
    job=discovery_and_transport_job,
)
def full_jobs_discovery_and_supabase_schedule(context):
    """Schedule that runs job discovery (except iCIMS) followed by Supabase transport daily at noon."""
    for partition_key in alpha_partitions.get_partition_keys():
        run_key = f"discovery_and_transport_{partition_key}_{context.scheduled_execution_time.strftime('%Y-%m-%d')}"
        yield RunRequest(
            run_key=run_key,
            partition_key=partition_key,
            tags={"partition": partition_key}
        )