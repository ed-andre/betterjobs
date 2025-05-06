import os
import csv
import json
import pandas as pd
from typing import Dict, List, Tuple, Set, Optional, Any
import logging
import time
from pathlib import Path
import traceback
import requests
from urllib.parse import urlparse

from dagster import asset, AssetExecutionContext, Config, get_dagster_logger, Output, AssetMaterialization, MaterializeResult
from dagster_gemini import GeminiResource
from google.cloud import bigquery

from .url_discovery import find_company_urls_individual, parse_gemini_response
from .validate_urls import validate_urls

logger = get_dagster_logger()

# MAIN RETRY FUNCTION

def retry_failed_company_urls(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    ats_platform: str,
    table_name: str,
    url_patterns: Dict[str, List[str]] = None
) -> None:
    """
    Generic function to retry finding career URLs for companies that failed verification.
    Uses a comprehensive approach to check multiple URL patterns for each ATS platform.

    The function directly updates the BigQuery table and returns None instead of a DataFrame
    to avoid type conversion issues with the IO manager.

    Args:
        context: Dagster execution context
        gemini: Gemini resource for AI-powered URL discovery
        ats_platform: The ATS platform to retry (e.g., "greenhouse", "lever", etc.)
        table_name: The database table name to query and update
        url_patterns: Optional dictionary of URL patterns per platform. If None, uses default patterns.

    Returns:
        None (results are written directly to BigQuery)
    """
    # Default URL patterns if none provided
    DEFAULT_URL_PATTERNS = {
        "greenhouse": [
            "https://boards.greenhouse.io/[tenant]",
            "https://boards.greenhouse.io/[tenant]/",
            "https://job-boards.greenhouse.io/[tenant]",
            "https://boards.greenhouse.io/embed/job_board?for=[tenant]"
        ],
        "bamboohr": [
            "https://[tenant].bamboohr.com/careers",
            "https://[tenant].bamboohr.com/jobs"
        ],
        "icims": [
            "https://careers-[tenant].icims.com/",
            "https://jobs-[tenant].icims.com/",
        ],
        "jobvite": [
            "https://jobs.jobvite.com/[tenant]",
            "https://careers.jobvite.com/[tenant]"
        ],
        "lever": [
            "https://jobs.lever.co/[tenant]",
            "https://careers.lever.co/[tenant]"
        ],
        "smartrecruiters": [
            "https://jobs.smartrecruiters.com/[tenant]",
            "https://careers.smartrecruiters.com/[tenant]",
        ],
        "workday": [
            "https://[tenant].wd1.myworkdayjobs.com/careers",
            "https://[tenant].workday.com/careers"
        ]
    }

    url_patterns = url_patterns or DEFAULT_URL_PATTERNS

    cwd = Path(os.getcwd())
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        checkpoint_dir = Path("dagster_betterjobs/checkpoints")
    else:
        checkpoint_dir = Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

    # Create checkpoint directory if it doesn't exist
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Define checkpoint files
    url_discovery_checkpoint = checkpoint_dir / "retry_url_discovery_checkpoint.csv"
    batch_checkpoint = checkpoint_dir / f"{ats_platform}_url_discovery_checkpoint.csv"
    failed_retries_checkpoint = checkpoint_dir / f"{ats_platform}_failed_retries.csv"

    # Load existing verified records from checkpoint if it exists
    existing_verified = {}
    if batch_checkpoint.exists():
        try:
            checkpoint_df = pd.read_csv(batch_checkpoint)
            for _, row in checkpoint_df.iterrows():
                if row.get("url_verified", False):
                    existing_verified[row["company_name"]] = row.to_dict()
            context.log.info(f"Loaded {len(existing_verified)} existing verified records from checkpoint")
        except Exception as e:
            context.log.error(f"Error loading checkpoint file: {str(e)}")

    # Load existing platform switches
    ats_switched_companies = []
    if url_discovery_checkpoint.exists():
        try:
            switched_df = pd.read_csv(url_discovery_checkpoint)
            ats_switched_companies.extend(switched_df.to_dict("records"))
            context.log.info(f"Loaded {len(ats_switched_companies)} existing ATS switched companies")
        except Exception as e:
            context.log.error(f"Error loading ATS switched companies: {str(e)}")

    try:
        # Get dataset name for BigQuery operations
        dataset_name = os.getenv("GCP_DATASET_ID")
        if not dataset_name:
            raise ValueError("GCP_DATASET_ID environment variable not set")

        # Get BigQuery client from resources
        client = context.resources.bigquery

        # SQL to fetch companies with unverified URLs from BigQuery
        sql = f"""
        SELECT company_name, company_industry, platform
        FROM `{dataset_name}.{table_name}`
        WHERE url_verified = FALSE
        """

        context.log.info(f"Querying BigQuery for unverified companies in {dataset_name}.{table_name}")
        query_job = client.query(sql)
        unverified_df = query_job.to_dataframe()

        # Filter out companies that already have verified records
        failed_companies = []
        for record in unverified_df.to_dict("records"):
            if record["company_name"] not in existing_verified:
                failed_companies.append(record)
            else:
                context.log.info(f"Skipping {record['company_name']} - already has verified record")

        context.log.info(f"Found {len(failed_companies)} companies with unverified URLs to retry")
    except Exception as e:
        context.log.error(f"Error querying unverified companies: {str(e)}")
        context.log.error(traceback.format_exc())
        return None

    if not failed_companies:
        context.log.info("No unverified companies found")
        return None

    # Process companies in batches
    results = []
    failed_retries = []
    batch_size = 5
    total_batches = (len(failed_companies) + batch_size - 1) // batch_size

    context.log.info(f"Starting processing of {len(failed_companies)} companies in {total_batches} batches")

    for i in range(0, len(failed_companies), batch_size):
        batch = failed_companies[i:i + batch_size]
        current_batch = (i // batch_size) + 1
        context.log.info(f"Processing batch {current_batch}/{total_batches} ({len(batch)} companies)")

        company_list_json = json.dumps([{
            "company_name": company["company_name"],
            "company_industry": company["company_industry"]
        } for company in batch], indent=2)

        # Create prompt with URL patterns
        patterns_text = []
        for platform, patterns in url_patterns.items():
            patterns_text.append(f"{platform.title()} patterns:")
            for pattern in patterns:
                patterns_text.append(f"- {pattern}")
            if platform == ats_platform:
                patterns_text.append("Note: Try both exact company name and simplified versions (lowercase, no spaces)")
            patterns_text.append("")

        prompt = f"""
        Find ALL possible job board URLs for these companies across major ATS platforms.
        For each company, check these specific patterns and variations:

        {os.linesep.join(patterns_text)}

        For each company, return ALL potential URLs in this JSON format:
        [
          {{
            "company_name": "Example Inc.",
            "potential_urls": {{
              "greenhouse": [
                "https://boards.greenhouse.io/exampleinc",
                "https://boards.greenhouse.io/example",
                "https://job-boards.greenhouse.io/exampleinc"
              ],
              "lever": [
                "https://jobs.lever.co/exampleinc",
                "https://jobs.lever.co/example"
              ],
              // ... other platforms ...
            }},
            "career_url": "https://example.com/careers"  // Company's own careers page
          }}
        ]

        Companies to process:
        {company_list_json}
        """

        try:
            with gemini.get_model(context) as model:
                context.log.info(f"Sending batch of {len(batch)} companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Log the raw response for debugging
                context.log.debug(f"Raw Gemini response:\n{response.text}")

                # Parse the response to get potential URLs
                try:
                    # First try direct JSON parsing
                    potential_urls = json.loads(response.text)
                except json.JSONDecodeError as e:
                    context.log.warning(f"Failed to parse direct JSON response: {str(e)}")
                    context.log.info("Attempting to extract JSON from response")

                    try:
                        # Try to extract JSON array from the response
                        import re
                        # Look for array pattern with relaxed matching
                        json_match = re.search(r'\[\s*{.*?}\s*\]', response.text, re.DOTALL)

                        if json_match:
                            json_str = json_match.group()
                            context.log.debug(f"Extracted JSON string:\n{json_str}")
                            try:
                                potential_urls = json.loads(json_str)
                                context.log.info("Successfully parsed extracted JSON")
                            except json.JSONDecodeError as e:
                                context.log.error(f"Failed to parse extracted JSON: {str(e)}")
                                # Try to parse individual objects
                                json_objects = re.findall(r'{[^}]+}', json_str)
                                if json_objects:
                                    context.log.info(f"Found {len(json_objects)} individual JSON objects")
                                    # Try to combine them into a valid array
                                    combined_json = f"[{','.join(json_objects)}]"
                                    try:
                                        potential_urls = json.loads(combined_json)
                                        context.log.info("Successfully parsed combined JSON objects")
                                    except json.JSONDecodeError as e:
                                        context.log.error(f"Failed to parse combined JSON objects: {str(e)}")
                                        raise ValueError(f"Could not parse JSON from response: {str(e)}")
                                else:
                                    raise ValueError("No valid JSON objects found in response")
                        else:
                            # If no array found, look for individual objects
                            json_objects = re.findall(r'{[^}]+}', response.text)
                            if json_objects:
                                context.log.info(f"Found {len(json_objects)} individual JSON objects")
                                # Try to combine them into a valid array
                                combined_json = f"[{','.join(json_objects)}]"
                                try:
                                    potential_urls = json.loads(combined_json)
                                    context.log.info("Successfully parsed combined JSON objects")
                                except json.JSONDecodeError as e:
                                    context.log.error(f"Failed to parse combined JSON objects: {str(e)}")
                                    context.log.debug(f"Attempted to parse:\n{combined_json}")
                                    raise ValueError(f"Could not parse JSON from response: {str(e)}")
                            else:
                                context.log.error("No JSON-like structures found in response")
                                raise ValueError("Response contains no valid JSON structures")
                    except Exception as e:
                        context.log.error(f"Error processing response: {str(e)}")
                        context.log.error(f"Full response text:\n{response.text}")
                        raise ValueError(f"Failed to process Gemini response: {str(e)}")

                # Log the parsed result for debugging
                context.log.debug(f"Parsed URLs structure:\n{json.dumps(potential_urls, indent=2)}")

                # Validate the structure of parsed JSON
                if not isinstance(potential_urls, list):
                    context.log.error(f"Parsed result is not a list: {type(potential_urls)}")
                    raise ValueError("Parsed JSON is not a list")

                for item in potential_urls:
                    if not isinstance(item, dict):
                        context.log.error(f"Found non-dict item in results: {type(item)}")
                        raise ValueError("Parsed JSON contains non-object items")
                    if "company_name" not in item:
                        context.log.error(f"Missing company_name in item: {item}")
                        raise ValueError("Parsed JSON objects missing required 'company_name' field")
                    if "potential_urls" not in item:
                        # Try to construct potential_urls from direct ats_url if present
                        if "ats_url" in item:
                            item["potential_urls"] = {
                                ats_platform: [item["ats_url"]]
                            }
                            context.log.info(f"Converted direct ats_url to potential_urls format for {item['company_name']}")
                        else:
                            context.log.error(f"Missing URL information in item: {item}")
                            raise ValueError("Parsed JSON objects missing both 'potential_urls' and 'ats_url' fields")

                # Process each company's potential URLs
                batch_updates = []
                for company_data in potential_urls:
                    company_name = company_data["company_name"]
                    career_url = company_data.get("career_url")

                    # Try each potential URL until we find a valid one
                    found_valid_url = False
                    all_attempts = []

                    for platform, urls in company_data["potential_urls"].items():
                        for url in urls:
                            result = {
                                "company_name": company_name,
                                "company_industry": next((c["company_industry"] for c in batch if c["company_name"] == company_name), ""),
                                "platform": platform,
                                "ats_url": url,
                                "career_url": career_url,
                                "url_verified": False
                            }

                            # Validate the URL
                            validated = validate_urls(context, [result])[0]
                            all_attempts.append(validated)

                            if validated.get("url_verified", False):
                                found_valid_url = True
                                # Track platform switch if applicable
                                if platform != ats_platform:
                                    switched_entry = validated.copy()
                                    switched_entry["original_platform"] = ats_platform
                                    switched_entry["new_platform"] = platform
                                    if not any(s.get("company_name") == company_name for s in ats_switched_companies):
                                        ats_switched_companies.append(switched_entry)

                                batch_updates.append(validated)
                                results.append(validated)
                                break  # Found a valid URL, no need to try others

                        if found_valid_url:
                            break

                    if not found_valid_url:
                        # If no valid URL was found, record the failure with all attempted URLs
                        failed_entry = {
                            "company_name": company_name,
                            "company_industry": next((c["company_industry"] for c in batch if c["company_name"] == company_name), ""),
                            "attempted_urls": json.dumps(company_data["potential_urls"]),
                            "last_retry_time": pd.Timestamp.now().isoformat(),
                            "error": "No valid URLs found"
                        }
                        failed_retries.append(failed_entry)

                        # Add the best attempt to results (prefer one with working career_url)
                        best_attempt = next(
                            (a for a in all_attempts if a.get("career_url_verified", False)),
                            all_attempts[0] if all_attempts else {
                                "company_name": company_name,
                                "company_industry": next((c["company_industry"] for c in batch if c["company_name"] == company_name), ""),
                                "platform": "",
                                "ats_url": None,
                                "career_url": career_url,
                                "url_verified": False
                            }
                        )
                        batch_updates.append(best_attempt)
                        results.append(best_attempt)

                # Save batch results to checkpoint
                if batch_updates:
                    if batch_checkpoint.exists():
                        checkpoint_df = pd.read_csv(batch_checkpoint)
                        for update in batch_updates:
                            mask = checkpoint_df["company_name"] == update["company_name"]
                            if mask.any():
                                for key, value in update.items():
                                    checkpoint_df.loc[mask, key] = value
                            else:
                                checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame([update])], ignore_index=True)
                    else:
                        checkpoint_df = pd.DataFrame(batch_updates)

                    checkpoint_df.to_csv(batch_checkpoint, index=False)
                    context.log.info(f"Saved checkpoint with {len(checkpoint_df)} companies")

                # Save platform switches
                if ats_switched_companies:
                    pd.DataFrame(ats_switched_companies).to_csv(url_discovery_checkpoint, index=False)

            time.sleep(2)  # Rate limiting

        except Exception as e:
            context.log.error(f"Error processing batch: {str(e)}")
            context.log.error(traceback.format_exc())

            # Record failures
            for company in batch:
                failed_entry = {
                    "company_name": company["company_name"],
                    "company_industry": company["company_industry"],
                    "error": str(e),
                    "last_retry_time": pd.Timestamp.now().isoformat()
                }
                failed_retries.append(failed_entry)

    # Save failed retries
    if failed_retries:
        pd.DataFrame(failed_retries).to_csv(failed_retries_checkpoint, index=False)
        context.log.info(f"Saved {len(failed_retries)} failed retries to {failed_retries_checkpoint}")

    # Update BigQuery with results
    if results:
        try:
            results_df = pd.DataFrame(results)
            verified_records = results_df[results_df["url_verified"] == True]

            if not verified_records.empty:
                context.log.info(f"Updating BigQuery with {len(verified_records)} verified records")

                # Add timestamp for when the record was processed
                verified_records['date_processed'] = pd.Timestamp.now()

                # Create a temporary table for batch updates
                temp_table_name = f"{dataset_name}.{table_name}_temp_updates"

                # Drop the temp table if it exists
                query_job = client.query(f"DROP TABLE IF EXISTS `{temp_table_name}`")
                query_job.result()

                # Create the temporary table
                create_temp_sql = f"""
                CREATE TABLE `{temp_table_name}` (
                    company_name STRING NOT NULL,
                    company_industry STRING,
                    platform STRING NOT NULL,
                    ats_url STRING,
                    career_url STRING,
                    url_verified BOOL DEFAULT FALSE,
                    date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                query_job = client.query(create_temp_sql)
                query_job.result()
                context.log.info("Created temporary table for batch updates")

                # Set up job configuration
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    schema=[
                        bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("company_industry", "STRING"),
                        bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("ats_url", "STRING"),
                        bigquery.SchemaField("career_url", "STRING"),
                        bigquery.SchemaField("url_verified", "BOOL"),
                        bigquery.SchemaField("date_processed", "TIMESTAMP"),
                    ]
                )

                # Fill any null values in string columns with empty strings to avoid type errors
                for col in verified_records.select_dtypes(include=['object']).columns:
                    verified_records[col] = verified_records[col].fillna('').astype(str)

                # Handle boolean column explicitly
                if 'url_verified' in verified_records.columns:
                    verified_records['url_verified'] = verified_records['url_verified'].fillna(False)

                # Load the dataframe into the temporary table
                job = client.load_table_from_dataframe(
                    verified_records,
                    temp_table_name,
                    job_config=job_config
                )
                # Wait for the load job to complete
                job.result()
                context.log.info(f"Successfully loaded {len(verified_records)} records into temporary table")

                # Update main table from temporary table
                update_sql = f"""
                UPDATE `{dataset_name}.{table_name}` main
                SET
                    platform = temp.platform,
                    ats_url = temp.ats_url,
                    career_url = temp.career_url,
                    url_verified = temp.url_verified,
                    date_processed = temp.date_processed
                FROM `{temp_table_name}` temp
                WHERE main.company_name = temp.company_name
                """
                query_job = client.query(update_sql)
                query_job.result()
                context.log.info(f"Updated {table_name} table with verified records")

                # Clean up - drop the temporary table
                query_job = client.query(f"DROP TABLE IF EXISTS `{temp_table_name}`")
                query_job.result()
                context.log.info("Dropped temporary table")
            else:
                context.log.info("No verified records to update in BigQuery")
        except Exception as e:
            context.log.error(f"Error updating BigQuery: {str(e)}")
            context.log.error(traceback.format_exc())

    return None













