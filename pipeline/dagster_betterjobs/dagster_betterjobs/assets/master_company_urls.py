import pandas as pd
import hashlib
from datetime import datetime
from dagster import asset, AssetExecutionContext, AssetKey
from typing import List, Dict

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
    io_manager_key="duckdb",
    deps=[
        "workday_company_urls", "retry_failed_workday_company_urls",
        "greenhouse_company_urls", "retry_failed_greenhouse_company_urls",
        "bamboohr_company_urls", "retry_failed_bamboohr_company_urls",
        "icims_company_urls", "retry_failed_icims_company_urls",
        "jobvite_company_urls", "retry_failed_jobvite_company_urls",
        "lever_company_urls", "retry_failed_lever_company_urls",
        "smartrecruiters_company_urls", "retry_failed_smartrecruiters_company_urls"
    ],
    required_resource_keys={"duckdb_resource"}
)
def master_company_urls(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Creates and maintains a master table of all company URLs across all ATS platforms.
    Queries both main and retry tables for each platform to ensure complete data.
    Handles deduplication and tracks record history for incremental processing.
    Generates and maintains stable company IDs.
    """
    # Define platform tables to query
    platform_tables = {
        "workday": ["workday_company_urls", "retry_failed_workday_company_urls"],
        "greenhouse": ["greenhouse_company_urls", "retry_failed_greenhouse_company_urls"],
        "bamboohr": ["bamboohr_company_urls", "retry_failed_bamboohr_company_urls"],
        "icims": ["icims_company_urls", "retry_failed_icims_company_urls"],
        "jobvite": ["jobvite_company_urls", "retry_failed_jobvite_company_urls"],
        "lever": ["lever_company_urls", "retry_failed_lever_company_urls"],
        "smartrecruiters": ["smartrecruiters_company_urls", "retry_failed_smartrecruiters_company_urls"]
    }

    # Get DuckDB resource for querying
    duckdb = context.resources.duckdb_resource

    processed_dfs = []
    required_columns = ["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"]

    # Query each platform's tables
    for platform, tables in platform_tables.items():
        for table in tables:
            try:
                # Query the table using connection
                with duckdb.get_connection() as conn:
                    query = f"""
                    SELECT
                        company_name,
                        company_industry,
                        platform,
                        ats_url,
                        career_url,
                        url_verified
                    FROM public.{table}
                    """
                    df = conn.execute(query).df()

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
        return pd.DataFrame(columns=required_columns + ["company_id", "date_added", "last_updated"])

    # Combine all dataframes
    combined_df = pd.concat(processed_dfs, ignore_index=True)

    # Generate company IDs for all records
    combined_df["company_id"] = combined_df["company_name"].apply(generate_company_id)

    # Get existing master table if it exists
    try:
        with duckdb.get_connection() as conn:
            query = """
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
            FROM public.master_company_urls
            """
            existing_df = conn.execute(query).df()
            context.log.info(f"Loaded existing master table with {len(existing_df)} records")
    except Exception as e:
        context.log.info(f"No existing master table found, creating new one: {str(e)}")
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

    # Ensure the table exists with correct schema
    with duckdb.get_connection() as conn:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.master_company_urls (
            company_id VARCHAR,
            company_name VARCHAR,
            company_industry VARCHAR,
            platform VARCHAR,
            ats_url VARCHAR,
            career_url VARCHAR,
            url_verified BOOLEAN,
            date_added TIMESTAMP,
            last_updated TIMESTAMP,
            PRIMARY KEY (company_id)
        )
        """
        conn.execute(create_table_query)

        # Register the DataFrame as a temporary view
        conn.register('combined_df_view', combined_df)

        # Store the updated data
        conn.execute("DELETE FROM public.master_company_urls")  # Clear existing data
        conn.execute("""
            INSERT INTO public.master_company_urls (
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
                CAST(url_verified AS BOOLEAN),
                date_added,
                last_updated
            FROM combined_df_view
        """)

        # Unregister the temporary view
        conn.unregister('combined_df_view')

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

    return combined_df