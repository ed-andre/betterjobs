from dagster import Definitions, load_assets_from_modules, EnvVar, resource
from dagster_duckdb import DuckDBResource
from dagster_gcp_pandas import BigQueryPandasIOManager
from dagster_gcp import BigQueryResource
from dagster_gemini import GeminiResource
from dagster_openai import OpenAIResource
from pathlib import Path
import os
import base64
import json
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

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


@resource
def bigquery_client_resource(context):
    """Resource that creates a BigQuery client using credentials from the environment."""
    # Get environment variables
    project_id = os.environ.get('GCP_PROJECT_ID')
    gcp_credentials_b64 = os.environ.get('GCP_CREDENTIALS')

    if not project_id or not gcp_credentials_b64:
        raise ValueError("Missing required environment variables: GCP_PROJECT_ID, GCP_CREDENTIALS")

    # Decode and parse credentials
    try:
        credentials_json = base64.b64decode(gcp_credentials_b64).decode("utf-8")
        credentials_info = json.loads(credentials_json)
        credentials = Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=project_id)
        return client
    except Exception as e:
        context.log.error(f"Failed to create BigQuery client: {str(e)}")
        raise


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
    "bigquery": bigquery_client_resource,
    "bigquery_io": BigQueryPandasIOManager(
        project=EnvVar("GCP_PROJECT_ID"),
        dataset=EnvVar("GCP_DATASET_ID"),
        location=EnvVar("GCP_LOCATION"),
        timeout=15.0,
        gcp_credentials=EnvVar("GCP_CREDENTIALS")
    ),
}

# Debug: Print environment variables
print("GCP_CREDENTIALS present:", bool(os.getenv("GCP_CREDENTIALS")))
print("GCP_PROJECT_ID present:", bool(os.getenv("GCP_PROJECT_ID")))
print("GCP_DATASET_ID present:", bool(os.getenv("GCP_DATASET_ID")))
print("GCP_LOCATION present:", bool(os.getenv("GCP_LOCATION")))

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
