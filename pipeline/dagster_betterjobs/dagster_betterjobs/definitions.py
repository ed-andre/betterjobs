from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_duckdb import DuckDBResource
from dagster_gemini import GeminiResource
from dagster_openai import OpenAIResource
from pathlib import Path
import os

from dagster_betterjobs import assets  # noqa: TID252
from dagster_betterjobs.assets.job_scraping import JobScrapingConfig, JobSearchConfig
from dagster_betterjobs.io import BetterJobsIOManager
from dagster_betterjobs.jobs import (
    job_scraping_job,
    workday_url_discovery_job,
    greenhouse_url_discovery_job,
    bamboohr_url_discovery_job,
    icims_url_discovery_job,
    jobvite_url_discovery_job,
    lever_url_discovery_job,
    smartrecruiters_url_discovery_job,
    full_url_discovery_job,
    master_company_urls_job
)
from dagster_betterjobs.schedules import daily_job_scrape_schedule


# Load all assets including:
# - Platform-specific assets (workday_company_urls, greenhouse_company_urls, bamboohr_company_urls, etc.)
# - Job scraping assets
all_assets = load_assets_from_modules([assets])

# Create properly resolved database path to avoid duplication issues
current_dir = Path(os.getcwd())
if current_dir.name == "dagster_betterjobs" and "pipeline" in str(current_dir):
    # If we're already in pipeline/dagster_betterjobs
    db_path = Path("dagster_betterjobs/db/betterjobs.db")
else:
    # Otherwise use the full path
    db_path = Path("pipeline/dagster_betterjobs/dagster_betterjobs/db/betterjobs.db")

# Make sure the directory for the database exists
db_dir = db_path.parent
os.makedirs(db_dir, exist_ok=True)

# Define resources configuration
resources = {
    "duckdb_resource": DuckDBResource(
        database=str(db_path)
    ),
    "gemini": GeminiResource(
        api_key=EnvVar("GEMINI_API_KEY"),
        generative_model_name="gemini-2.5-flash-preview-04-17",
    ),
    "openai": OpenAIResource(
        api_key=EnvVar("OPENAI_API_KEY"),
        model=""
    ),
    "duckdb": BetterJobsIOManager(database=str(db_path)),
}

# Create the definitions object
defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[
        job_scraping_job,
        workday_url_discovery_job,
        greenhouse_url_discovery_job,
        bamboohr_url_discovery_job,
        icims_url_discovery_job,
        jobvite_url_discovery_job,
        lever_url_discovery_job,
        smartrecruiters_url_discovery_job,
        full_url_discovery_job,
        master_company_urls_job,
    ],
    schedules=[
        daily_job_scrape_schedule,
    ],
)
