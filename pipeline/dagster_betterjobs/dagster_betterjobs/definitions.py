from dagster import (
    Definitions,
    load_assets_from_modules,
    EnvVar,
    resource
)
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

from dagster_betterjobs.assets.bamboohr_jobs_discovery import (
    bamboohr_company_jobs_discovery
)
from dagster_betterjobs.assets.greenhouse_jobs_discovery import (
    greenhouse_company_jobs_discovery
)
from dagster_betterjobs.assets.smartrecruiters_jobs_discovery import (
    smartrecruiters_company_jobs_discovery
)
from dagster_betterjobs.assets.workday_jobs_discovery import (
    workday_company_jobs_discovery
)
from dagster_betterjobs.io import BetterJobsIOManager
from dagster_betterjobs.jobs import (
   
    workday_url_discovery_job,
    greenhouse_url_discovery_job,
    bamboohr_url_discovery_job,
    icims_url_discovery_job,
    jobvite_url_discovery_job,
    lever_url_discovery_job,
    smartrecruiters_url_discovery_job,
    full_url_discovery_job,
    master_company_urls_job,
    data_engineering_job,
    full_jobs_discovery_job,
    bamboohr_jobs_discovery_job,
    greenhouse_jobs_discovery_job,
    smartrecruiters_jobs_discovery_job,
    workday_jobs_discovery_job,
    full_jobs_discovery_and_search_job
)
from dagster_betterjobs.schedules import (
    bamboohr_jobs_hourly_schedule,
    full_jobs_discovery_and_search_schedule
)


@resource
def bigquery_client_resource(context):
    """Creates a BigQuery client using environment credentials."""
    project_id = os.environ.get('GCP_PROJECT_ID')
    gcp_credentials_b64 = os.environ.get('GCP_CREDENTIALS')

    if not project_id or not gcp_credentials_b64:
        raise ValueError("Missing required environment variables: GCP_PROJECT_ID, GCP_CREDENTIALS")

    try:
        credentials_json = base64.b64decode(gcp_credentials_b64).decode("utf-8")
        credentials_info = json.loads(credentials_json)
        credentials = Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=project_id)
        return client
    except Exception as e:
        context.log.error(f"Failed to create BigQuery client: {str(e)}")
        raise


# Load assets from modules
all_assets = load_assets_from_modules([assets])

# Resolve database path based on execution directory
current_dir = Path(os.getcwd())
if current_dir.name == "dagster_betterjobs" and "pipeline" in str(current_dir):
    db_path = Path("dagster_betterjobs/db/betterjobs.db")
else:
    db_path = Path("pipeline/dagster_betterjobs/dagster_betterjobs/db/betterjobs.db")

# Ensure database directory exists
db_dir = db_path.parent
os.makedirs(db_dir, exist_ok=True)

# Configure resources
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

# Verify presence of required environment variables
print("GCP_CREDENTIALS present:", bool(os.getenv("GCP_CREDENTIALS")))
print("GCP_PROJECT_ID present:", bool(os.getenv("GCP_PROJECT_ID")))
print("GCP_DATASET_ID present:", bool(os.getenv("GCP_DATASET_ID")))
print("GCP_LOCATION present:", bool(os.getenv("GCP_LOCATION")))

# Define Dagster application
defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[

        workday_url_discovery_job,
        greenhouse_url_discovery_job,
        bamboohr_url_discovery_job,
        icims_url_discovery_job,
        jobvite_url_discovery_job,
        lever_url_discovery_job,
        smartrecruiters_url_discovery_job,
        full_url_discovery_job,
        master_company_urls_job,
        bamboohr_jobs_discovery_job,
        greenhouse_jobs_discovery_job,
        smartrecruiters_jobs_discovery_job,
        workday_jobs_discovery_job,
        full_jobs_discovery_job,
        data_engineering_job,
        full_jobs_discovery_and_search_job,
    ],
    schedules=[
        bamboohr_jobs_hourly_schedule,
        full_jobs_discovery_and_search_schedule,
    ],
)
