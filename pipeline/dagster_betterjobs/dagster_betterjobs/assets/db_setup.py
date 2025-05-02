import os
from pathlib import Path
from dagster import asset, AssetExecutionContext, get_dagster_logger, EnvVar
from google.cloud import bigquery
import json
import base64

logger = get_dagster_logger()

@asset(
    group_name="database",
    compute_kind="database",
)
def initialize_db(context: AssetExecutionContext):
    """
    Initialize the database schema by executing the SQL schema file.
    This asset creates the necessary tables for storing company and job data.
    """
    # Get environment variables directly
    project_id = os.environ.get('GCP_PROJECT_ID')
    dataset_id = os.environ.get('GCP_DATASET_ID')
    gcp_credentials = os.environ.get('GCP_CREDENTIALS')
    location = os.environ.get('GCP_LOCATION', 'US')  # Default to US if not specified

    if not project_id or not dataset_id or not gcp_credentials:
        raise ValueError("Missing required environment variables: GCP_PROJECT_ID, GCP_DATASET_ID, GCP_CREDENTIALS")

    context.log.info(f"Using project: {project_id}, dataset: {dataset_id}, location: {location}")

    # Create BigQuery client directly
    try:
        credentials_json = base64.b64decode(gcp_credentials).decode("utf-8")
        credentials_info = json.loads(credentials_json)
        from google.oauth2.service_account import Credentials
        credentials = Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=project_id)
        context.log.info("BigQuery client created successfully")
    except Exception as e:
        context.log.error(f"Failed to create BigQuery client: {str(e)}")
        raise

    # First, create the dataset if it doesn't exist
    try:
        # Get dataset reference
        dataset_ref = client.dataset(dataset_id)

        # Try to get dataset to check if it exists
        try:
            client.get_dataset(dataset_ref)
            context.log.info(f"Dataset {dataset_id} already exists")
        except Exception:
            # Dataset doesn't exist, create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = client.create_dataset(dataset)
            context.log.info(f"Created dataset {dataset_id} in location {location}")
    except Exception as e:
        context.log.error(f"Error creating dataset: {str(e)}")
        raise

    # Set fully qualified table names with proper formatting for BigQuery
    companies_table = f"`{project_id}`.`{dataset_id}`.`companies`"
    jobs_table = f"`{project_id}`.`{dataset_id}`.`jobs`"

    # Drop existing tables to ensure clean schema
    try:
        # Drop tables in correct order (most dependent first)
        query_job = client.query(f"DROP TABLE IF EXISTS {jobs_table}")
        query_job.result()  # Wait for query to finish

        query_job = client.query(f"DROP TABLE IF EXISTS {companies_table}")
        query_job.result()  # Wait for query to finish

        context.log.info("Tables dropped or reset for fresh schema")
    except Exception as e:
        context.log.error(f"Failed to drop tables: {str(e)}")
        context.log.info("Will attempt to create schema anyway")

    # Create the tables using BigQuery API instead of SQL
    try:
        context.log.info("Creating companies table")

        # Define the companies table schema
        companies_schema = [
            bigquery.SchemaField("company_id", "INT64"),
            bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("company_industry", "STRING"),
            bigquery.SchemaField("employee_count_range", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ats_url", "STRING"),
            bigquery.SchemaField("career_url", "STRING"),
            bigquery.SchemaField("url_verified", "BOOL"),
            bigquery.SchemaField("last_updated", "TIMESTAMP"),
        ]

        # Create table reference
        companies_ref = client.dataset(dataset_id).table("companies")

        # Create companies table with clustering
        companies_table_obj = bigquery.Table(companies_ref, schema=companies_schema)
        companies_table_obj.clustering_fields = ["platform", "company_name"]
        companies_table_obj = client.create_table(companies_table_obj)
        context.log.info(f"Created companies table: {companies_table_obj.full_table_id}")

        context.log.info("Creating jobs table")

        # Define the jobs table schema
        jobs_schema = [
            bigquery.SchemaField("job_id", "INT64"),
            bigquery.SchemaField("company_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("job_title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("job_description", "STRING"),
            bigquery.SchemaField("job_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("date_posted", "TIMESTAMP"),
            bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
            bigquery.SchemaField("is_active", "BOOL"),
        ]

        # Create table reference
        jobs_ref = client.dataset(dataset_id).table("jobs")

        # Create jobs table with clustering
        jobs_table_obj = bigquery.Table(jobs_ref, schema=jobs_schema)
        jobs_table_obj.clustering_fields = ["company_id", "job_title"]
        jobs_table_obj = client.create_table(jobs_table_obj)
        context.log.info(f"Created jobs table: {jobs_table_obj.full_table_id}")

    except Exception as e:
        context.log.error(f"Error creating tables: {str(e)}")
        raise

    context.log.info("Database schema initialized successfully")
    return "Database initialized"