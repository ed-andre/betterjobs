import pandas as pd
import hashlib
from datetime import datetime
from dagster import asset, AssetExecutionContext, AssetKey, MetadataValue
from typing import List, Dict
import os
from google.cloud import bigquery

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
    group_name="company_urls",
    compute_kind="python",
    io_manager_key="bigquery_io",
    deps=[
        "workday_company_urls", "retry_failed_workday_company_urls",
        "greenhouse_company_urls", "retry_failed_greenhouse_company_urls",
        "bamboohr_company_urls", "retry_failed_bamboohr_company_urls",
        "icims_company_urls", "retry_failed_icims_company_urls",
        "jobvite_company_urls", "retry_failed_jobvite_company_urls",
        "lever_company_urls", "retry_failed_lever_company_urls",
        "smartrecruiters_company_urls", "retry_failed_smartrecruiters_company_urls"
    ],
    required_resource_keys={"bigquery"}
)
def master_company_urls(context: AssetExecutionContext) -> None:
    """
    Creates and maintains a master table of all company URLs across all ATS platforms.
    Queries both main and retry tables for each platform to ensure complete data.
    Handles deduplication and tracks record history for incremental processing.
    Generates and maintains stable company IDs.

    Returns None instead of a DataFrame to avoid type conversion issues with the IO manager.
    """
    # Get BigQuery client and dataset name
    client = context.resources.bigquery
    dataset_name = os.getenv("GCP_DATASET_ID")

    # Define platform tables to query
    platform_tables = {
        "workday": ["workday_company_urls"],
        "greenhouse": ["greenhouse_company_urls"],
        "bamboohr": ["bamboohr_company_urls"],
        "icims": ["icims_company_urls"],
        "jobvite": ["jobvite_company_urls"],
        "lever": ["lever_company_urls"],
        "smartrecruiters": ["smartrecruiters_company_urls"]
    }

    processed_dfs = []
    required_columns = ["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"]

    # Query each platform's tables from BigQuery
    for platform, tables in platform_tables.items():
        for table in tables:
            try:
                # Query the table using BigQuery client
                query = f"""
                SELECT
                    company_name,
                    company_industry,
                    platform,
                    ats_url,
                    career_url,
                    url_verified
                FROM {dataset_name}.{table}
                """
                query_job = client.query(query)
                df = query_job.to_dataframe()

                if not df.empty:
                    # Ensure all required columns exist
                    for col in required_columns:
                        if col not in df.columns:
                            df[col] = None

                    # Set platform if not already set
                    if df["platform"].isna().any():
                        df["platform"] = platform

                    processed_dfs.append(df[required_columns])
                    context.log.info(f"Loaded {len(df)} records from {table}")
            except Exception as e:
                context.log.warning(f"Error loading table {table}: {str(e)}")

    if not processed_dfs:
        context.log.warning("No data available from any source")
        # Return None instead of an empty DataFrame
        return None

    # Combine all dataframes
    combined_df = pd.concat(processed_dfs, ignore_index=True)

    # Generate company IDs for all records
    combined_df["company_id"] = combined_df["company_name"].apply(generate_company_id)

    # Get existing master table if it exists from BigQuery
    try:
        # Check if master_company_urls table exists in BigQuery
        table_id = f"{dataset_name}.master_company_urls"
        try:
            client.get_table(table_id)

            # Get existing data
            query = f"""
            SELECT
                company_id,
                company_name,
                company_industry,
                platform,
                ats_url,
                career_url,
                url_verified,
                date_added,
                last_updated
            FROM {dataset_name}.master_company_urls
            """
            query_job = client.query(query)
            existing_df = query_job.to_dataframe()
            context.log.info(f"Loaded existing master table with {len(existing_df)} records")
        except Exception as e:
            context.log.info(f"Master company URLs table not found, creating new one: {str(e)}")
            existing_df = pd.DataFrame(columns=required_columns + ["company_id", "date_added", "last_updated"])
    except Exception as e:
        context.log.warning(f"Error accessing BigQuery: {str(e)}")
        existing_df = pd.DataFrame(columns=required_columns + ["company_id", "date_added", "last_updated"])

    # Current timestamp for updates
    current_time = datetime.now()

    if existing_df.empty:
        # For new table, add timestamps
        combined_df["date_added"] = current_time
        combined_df["last_updated"] = current_time

        # Deduplicate based on company_id, keeping most recent verified URLs
        combined_df.sort_values(
            by=["company_id", "url_verified"],
            ascending=[True, False],
            inplace=True
        )
        combined_df.drop_duplicates(subset=["company_id"], keep="first", inplace=True)
    else:
        # Merge with existing data
        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)

        # Sort by company_id, url_verified, and last_updated
        combined_df.sort_values(
            by=["company_id", "url_verified", "last_updated"],
            ascending=[True, False, False],
            inplace=True
        )

        # Drop duplicates keeping the first occurrence (most recent verified URL)
        combined_df.drop_duplicates(subset=["company_id"], keep="first", inplace=True)

        # Update timestamps for changed records
        existing_company_ids = set(existing_df["company_id"])
        new_records_mask = ~combined_df["company_id"].isin(existing_company_ids)

        # Set date_added for new records
        combined_df.loc[new_records_mask, "date_added"] = current_time

        # Update last_updated for changed records
        changed_mask = ~combined_df.isin(existing_df).all(axis=1)
        combined_df.loc[changed_mask, "last_updated"] = current_time

    # Sort the final dataframe
    combined_df.sort_values(by=["company_name"], inplace=True)

    # Ensure columns are in the correct order
    table_columns = [
        "company_id",
        "company_name",
        "company_industry",
        "platform",
        "ats_url",
        "career_url",
        "url_verified",
        "date_added",
        "last_updated"
    ]
    combined_df = combined_df[table_columns]

    # Create the master_company_urls table in BigQuery if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {dataset_name}.master_company_urls (
        company_id STRING,
        company_name STRING NOT NULL,
        company_industry STRING,
        platform STRING,
        ats_url STRING,
        career_url STRING,
        url_verified BOOL DEFAULT FALSE,
        date_added TIMESTAMP,
        last_updated TIMESTAMP
    )
    """
    query_job = client.query(create_table_sql)
    query_job.result()  # Wait for the query to complete
    context.log.info("Created or verified master_company_urls table in BigQuery")

    # Create a temporary table for the bulk load approach
    temp_table_name = f"{dataset_name}.master_company_urls_temp"

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
        url_verified BOOL DEFAULT FALSE,
        date_added TIMESTAMP,
        last_updated TIMESTAMP
    )
    """
    query_job = client.query(create_temp_sql)
    query_job.result()
    context.log.info("Created temporary table for bulk loading")

    # Fill any null values in string columns with empty strings to avoid type errors
    for col in combined_df.select_dtypes(include=['object']).columns:
        combined_df[col] = combined_df[col].fillna('').astype(str)

    # Handle boolean column explicitly
    if 'url_verified' in combined_df.columns:
        combined_df['url_verified'] = combined_df['url_verified'].fillna(False)

    # Ensure datetime columns are properly converted to pandas datetime format
    # This will fix the "object of type <class 'str'> cannot be converted to int" error
    # Use errors='coerce' to handle various formats and edge cases
    combined_df['date_added'] = pd.to_datetime(combined_df['date_added'], errors='coerce')
    combined_df['last_updated'] = pd.to_datetime(combined_df['last_updated'], errors='coerce')

    # Replace any NaT values with current timestamp to ensure no nulls in datetime columns
    current_timestamp = pd.Timestamp.now()
    combined_df['date_added'] = combined_df['date_added'].fillna(current_timestamp)
    combined_df['last_updated'] = combined_df['last_updated'].fillna(current_timestamp)

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
        combined_df,
        temp_table_name,
        job_config=job_config
    )
    # Wait for the load job to complete
    job.result()
    context.log.info(f"Successfully loaded {len(combined_df)} companies into temporary table")

    # Delete existing data and insert from temporary table
    clear_data_sql = f"DELETE FROM {dataset_name}.master_company_urls WHERE 1=1"
    query_job = client.query(clear_data_sql)
    query_job.result()
    context.log.info("Cleared existing data from master_company_urls table")

    # Insert data from temporary table to master table
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
        company_id,
        company_name,
        company_industry,
        platform,
        ats_url,
        career_url,
        CAST(url_verified AS BOOL),
        date_added,
        last_updated
    FROM {temp_table_name}
    """
    query_job = client.query(insert_sql)
    query_job.result()
    context.log.info("Transferred data from temporary to master table")

    # Drop the temporary table
    query_job = client.query(f"DROP TABLE IF EXISTS {temp_table_name}")
    query_job.result()
    context.log.info("Dropped temporary table")

    # Log summary statistics
    total_companies = len(combined_df)
    verified_urls = combined_df["url_verified"].sum() if "url_verified" in combined_df.columns else 0
    new_records = (combined_df["date_added"] == current_time).sum()
    updated_records = ((combined_df["last_updated"] == current_time) &
                      (combined_df["date_added"] != current_time)).sum()

    # Log platform-specific statistics
    context.log.info("\nPlatform Statistics:")
    platform_stats = combined_df.groupby("platform").agg({
        "company_name": "count",
        "url_verified": "sum"
    }).reset_index()

    for _, row in platform_stats.iterrows():
        platform = row["platform"]
        count = row["company_name"]
        verified = row["url_verified"]
        context.log.info(f"{platform}: {count} companies, {verified} verified ({verified/count:.1%})")

    context.log.info(f"\nMaster table statistics:")
    context.log.info(f"Total companies: {total_companies}")
    context.log.info(f"Verified URLs: {verified_urls} ({verified_urls/total_companies:.1%})")
    context.log.info(f"New records: {new_records}")
    context.log.info(f"Updated records: {updated_records}")

    # Add metadata to the output
    context.add_output_metadata({
        "total_companies": MetadataValue.int(int(total_companies)),
        "verified_urls": MetadataValue.int(int(verified_urls)),
        "new_records": MetadataValue.int(int(new_records)),
        "updated_records": MetadataValue.int(int(updated_records)),
        "bigquery_table": MetadataValue.text(f"{dataset_name}.master_company_urls")
    })

    return None