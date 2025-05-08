import os
import pandas as pd
from datetime import datetime
import hashlib
from dagster import asset, AssetExecutionContext, MetadataValue

def generate_company_id(company_name: str) -> str:
    """
    Generate a stable, unique company ID from company name.
    Uses first 8 characters of SHA-256 hash to create a compact ID.
    """
    # Normalize company name (lowercase, remove extra spaces)
    normalized_name = " ".join(company_name.lower().split())
    # Generate hash
    hash_object = hashlib.sha256(normalized_name.encode())
    # Take first 8 characters for a compact but still unique ID
    return hash_object.hexdigest()[:8]

@asset(
    group_name="adhoc_request",
    kinds={"python", "sql", "bigquery"},
    io_manager_key="bigquery_io",
    deps=["master_company_urls"],
    required_resource_keys={"bigquery"}
)
def adhoc_company_urls(context: AssetExecutionContext) -> None:
    """
    Processes manually added company URLs from CSV files in the input folder.
    Adds new companies to the master_company_urls table if they don't exist
    or if they have a different ATS URL than what's already in the database.

    CSV files must have the following headers:
    company_name, company_industry, platform, ats_url, career_url
    """
    client = context.resources.bigquery
    dataset_name = os.getenv("GCP_DATASET_ID")

    # Get input folder path from environment variable or use relative path as fallback
    input_folder = os.getenv("ADHOC_INPUT_FOLDER")

    if not input_folder:
        # Use relative path as fallback
        # Assuming the code is run from the project root
        input_folder = os.path.join("pipeline", "dagster_betterjobs", "input")
        context.log.info(f"ADHOC_INPUT_FOLDER environment variable not set, using relative path: {input_folder}")

    if not os.path.exists(input_folder):
        context.log.error(f"Input folder not found at: {input_folder}")
        return None

    # List directory contents for debugging
    try:
        context.log.info(f"Contents of input folder {input_folder}: {os.listdir(input_folder)}")
    except Exception as e:
        context.log.error(f"Error listing input folder: {str(e)}")

    # Get list of CSV files in input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    if not csv_files:
        context.log.info("No CSV files found in input folder")
        return None

    context.log.info(f"Found {len(csv_files)} CSV files in input folder")

    # Read and process each CSV file
    all_companies = []
    for csv_file in csv_files:
        file_path = os.path.join(input_folder, csv_file)
        try:
            df = pd.read_csv(file_path)
            required_columns = ["company_name", "company_industry", "platform", "ats_url", "career_url"]

            # Check if all required columns are present
            if not all(col in df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in df.columns]
                context.log.warning(f"File {csv_file} is missing required columns: {missing_cols}")
                continue

            # Filter out rows with missing company_name or ats_url
            df = df[df['company_name'].notna() & df['ats_url'].notna()]

            # Add url_verified column (set to True as these are manually verified)
            df['url_verified'] = True

            # Generate company_id for each row
            df['company_id'] = df['company_name'].apply(generate_company_id)

            context.log.info(f"Processed {len(df)} companies from {csv_file}")
            all_companies.append(df[required_columns + ['url_verified', 'company_id']])

            # Move processed file to processed folder or delete
            # os.rename(file_path, os.path.join(input_folder, "processed", csv_file))
        except Exception as e:
            context.log.error(f"Error processing file {csv_file}: {str(e)}")

    if not all_companies:
        context.log.info("No valid company data found in CSV files")
        return None

    # Combine all company data
    combined_df = pd.concat(all_companies, ignore_index=True)

    # Get existing master table data
    try:
        query = f"""
        SELECT
            company_id,
            company_name,
            ats_url,
            date_added,
            last_updated
        FROM {dataset_name}.master_company_urls
        """
        query_job = client.query(query)
        existing_df = query_job.to_dataframe()
        context.log.info(f"Loaded existing master table with {len(existing_df)} records")
    except Exception as e:
        context.log.warning(f"Error accessing master_company_urls: {str(e)}")
        context.log.info("Will process all companies as new")
        existing_df = pd.DataFrame(columns=["company_id", "company_name", "ats_url"])

    # Identify new companies and companies with changed ATS URLs
    if not existing_df.empty:
        # Create a mapping of company_id to ats_url from existing data
        existing_map = dict(zip(existing_df["company_id"], existing_df["ats_url"]))

        # Filter to keep only new companies or companies with different ats_url
        new_or_changed = []
        for _, row in combined_df.iterrows():
            company_id = row["company_id"]
            if company_id not in existing_map:
                new_or_changed.append(row)
                context.log.info(f"New company: {row['company_name']}")
            elif existing_map[company_id] != row["ats_url"]:
                new_or_changed.append(row)
                context.log.info(f"Updated ATS URL for: {row['company_name']}")

        if not new_or_changed:
            context.log.info("No new or changed companies to add")
            return None

        # Create DataFrame from the new or changed companies
        new_companies_df = pd.DataFrame(new_or_changed)
    else:
        new_companies_df = combined_df
        context.log.info(f"All {len(new_companies_df)} companies will be processed as new")

    # Current timestamp for updates
    current_time = datetime.now()

    # Add date_added and last_updated columns
    new_companies_df["date_added"] = current_time
    new_companies_df["last_updated"] = current_time

    # Set up job configuration for BigQuery
    from google.cloud import bigquery
    temp_table_name = f"{dataset_name}.adhoc_company_urls_temp"

    # Drop the temp table if it exists
    query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
    query_job.result()

    # Create the temporary table
    create_temp_sql = f"""
    CREATE TABLE {temp_table_name} (
        company_id STRING,
        company_name STRING NOT NULL,
        company_industry STRING,
        platform STRING,
        ats_url STRING,
        career_url STRING,
        url_verified BOOL DEFAULT TRUE,
        date_added TIMESTAMP,
        last_updated TIMESTAMP
    )
    """
    query_job = client.query(create_temp_sql)
    query_job.result()
    context.log.info("Created temporary table for bulk loading")

    # Ensure all string columns have values and datetime columns are properly formatted
    for col in new_companies_df.select_dtypes(include=['object']).columns:
        new_companies_df[col] = new_companies_df[col].fillna('').astype(str)

    # Set up job configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("company_id", "STRING"),
            bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("company_industry", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("ats_url", "STRING"),
            bigquery.SchemaField("career_url", "STRING"),
            bigquery.SchemaField("url_verified", "BOOL"),
            bigquery.SchemaField("date_added", "TIMESTAMP"),
            bigquery.SchemaField("last_updated", "TIMESTAMP"),
        ]
    )

    # Load the dataframe into the temporary table
    job = client.load_table_from_dataframe(
        new_companies_df,
        temp_table_name,
        job_config=job_config
    )
    # Wait for the load job to complete
    job.result()
    context.log.info(f"Successfully loaded {len(new_companies_df)} companies into temporary table")

    # Insert new records into master_company_urls
    insert_sql = f"""
    INSERT INTO {dataset_name}.master_company_urls (
        company_id,
        company_name,
        company_industry,
        platform,
        ats_url,
        career_url,
        url_verified,
        date_added,
        last_updated
    )
    SELECT
        t.company_id,
        t.company_name,
        t.company_industry,
        t.platform,
        t.ats_url,
        t.career_url,
        CAST(t.url_verified AS BOOL),
        t.date_added,
        t.last_updated
    FROM {temp_table_name} t
    LEFT JOIN {dataset_name}.master_company_urls m
        ON t.company_id = m.company_id
    WHERE m.company_id IS NULL
    """
    query_job = client.query(insert_sql)
    query_job.result()

    # Update existing records where ats_url is different
    update_sql = f"""
    UPDATE {dataset_name}.master_company_urls m
    SET
        ats_url = t.ats_url,
        career_url = t.career_url,
        platform = t.platform,
        company_industry = t.company_industry,
        url_verified = CAST(t.url_verified AS BOOL),
        last_updated = t.last_updated
    FROM {temp_table_name} t
    WHERE m.company_id = t.company_id
      AND m.ats_url != t.ats_url
    """
    query_job = client.query(update_sql)
    update_result = query_job.result()

    # Drop the temporary table
    query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
    query_job.result()
    context.log.info("Dropped temporary table")

    # Log summary statistics
    total_processed = len(new_companies_df)

    # Add metadata to the output
    context.add_output_metadata({
        "total_companies_processed": MetadataValue.int(total_processed),
        "bigquery_table": MetadataValue.text(f"{dataset_name}.master_company_urls")
    })

    context.log.info(f"Successfully processed {total_processed} companies from manual input files")

    return None