import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from google.cloud import bigquery
from pathlib import Path
import re
import html
from dagster import (
    asset, AssetExecutionContext, Config, MetadataValue,
    get_dagster_logger, Output, Definitions, define_asset_job, RunConfig
)

logger = get_dagster_logger()

class JobSearchConfig(Config):
    """Configuration parameters for job search."""
    # Search parameters
    keywords: List[str] = []  # List of keywords to search for
    job_titles: List[str] = []  # Specific job titles to search for
    excluded_keywords: List[str] = []  # Keywords to exclude
    locations: List[str] = []  # Locations to include
    remote: bool = None  # Include remote jobs (None = don't filter)

    # ATS platforms to include
    platforms: List[str] = ["all"]  # Options: "all", "greenhouse", "bamboohr", etc.

    # Date range
    days_back: int = 3  # Default to last 3 days
    date_from: Optional[str] = None  # Format: "YYYY-MM-DD"
    date_to: Optional[str] = None  # Format: "YYYY-MM-DD"

    # Results control
    max_results: int = 500  # Limit number of results
    min_match_score: float = 0.4  # Minimum relevance score (0.0 - 1.0)
    output_format: str = "dataframe"  # Output format: "dataframe", "dict", "csv", "html"
    output_file: Optional[str] = None  # Path to save results to file, with optional {date} placeholder

    # Advanced options
    include_descriptions: bool = True  # Include full job descriptions in output
    include_raw_data: bool = False  # Include raw API response data in output

@asset(
    group_name="job_search",
    kinds={"bigquery", "python"},
    required_resource_keys={"bigquery"},
    deps=["greenhouse_company_jobs_discovery", "bamboohr_company_jobs_discovery"]
)
def search_jobs(context: AssetExecutionContext, config: JobSearchConfig) -> pd.DataFrame:
    """
    Search for jobs across multiple platforms with flexible filtering options.

    This asset can search across different job tables (Greenhouse, BambooHR, etc.)
    with filtering by keywords, job titles, locations, and date ranges.
    """
    # Initialize BigQuery client
    client = context.resources.bigquery
    dataset_name = os.getenv("GCP_DATASET_ID")

    # Track execution stats
    stats = {
        "query_platforms": list(config.platforms),
        "total_results": 0,
        "filtered_results": 0,
        "execution_time_ms": 0,
        "search_params": {
            "keywords": config.keywords,
            "job_titles": config.job_titles,
            "locations": config.locations,
            "date_range": f"{config.days_back} days" if not config.date_from else f"{config.date_from} to {config.date_to or 'now'}"
        }
    }

    # Log search parameters
    context.log.info(f"Searching for jobs with parameters: {stats['search_params']}")

    # Determine which tables to query based on platforms
    tables_to_query = []
    if "all" in config.platforms or "greenhouse" in config.platforms:
        tables_to_query.append(f"{dataset_name}.greenhouse_jobs")
    if "all" in config.platforms or "bamboohr" in config.platforms:
        tables_to_query.append(f"{dataset_name}.bamboohr_jobs")
    # Add additional platform tables as they become available

    context.log.info(f"Querying tables: {tables_to_query}")

    # Prepare date filters
    date_from = None
    date_to = datetime.now().strftime("%Y-%m-%d")

    if config.date_from:
        date_from = config.date_from
    elif config.days_back > 0:
        date_from = (datetime.now() - timedelta(days=config.days_back)).strftime("%Y-%m-%d")

    if config.date_to:
        date_to = config.date_to

    # Build the combined search results
    combined_results = pd.DataFrame()
    start_time = datetime.now()

    # Process each table
    for table in tables_to_query:
        platform = table.split(".")[-1].replace("_jobs", "")
        context.log.info(f"Searching {platform} jobs...")

        # Determine date column based on platform
        date_col = "published_at" if platform == "greenhouse" else "date_posted"

        # Start building query
        query = f"""
        SELECT
            '{platform}' as platform,
            j.job_id,
            j.company_id,
            c.company_name,
            j.job_title,
            {"j.job_description," if config.include_descriptions else ""}
            j.job_url,
            j.location,
            {"j.department," if platform in ["greenhouse", "bamboohr"] else ""}
            {date_col} as posting_date,
            j.date_retrieved,
            {"j.raw_data," if config.include_raw_data else ""}
            j.is_active
        FROM {table} j
        LEFT JOIN {dataset_name}.master_company_urls c ON j.company_id = c.company_id
        WHERE j.is_active = TRUE
        """

        # Add date filters
        if date_from:
            query += f" AND {date_col} >= '{date_from}'"
        if date_to:
            query += f" AND {date_col} <= '{date_to}'"

        # Add keyword filters
        if config.keywords:
            # We want to match ANY keyword in EITHER title OR description
            keyword_title_conditions = []
            keyword_desc_conditions = []

            for keyword in config.keywords:
                keyword_title_conditions.append(f"LOWER(job_title) LIKE LOWER('%{keyword}%')")
                if config.include_descriptions:
                    keyword_desc_conditions.append(f"LOWER(job_description) LIKE LOWER('%{keyword}%')")

            # Combine title and description conditions with OR
            all_keyword_conditions = keyword_title_conditions + keyword_desc_conditions
            query += f" AND ({' OR '.join(all_keyword_conditions)})"

            # Debug info
            context.log.info(f"Keyword conditions: {' OR '.join(all_keyword_conditions)}")

        # Add job title filters - at least one must match
        if config.job_titles:
            title_conditions = []
            for title in config.job_titles:
                title_conditions.append(f"LOWER(job_title) LIKE LOWER('%{title}%')")

            # We want an OR between all job title conditions (any match is good)
            query += f" AND ({' OR '.join(title_conditions)})"

            # Debug info
            context.log.info(f"Title conditions: {' OR '.join(title_conditions)}")

        # Add location filters - at least one must match
        location_or_remote_conditions = []

        if config.locations:
            for location in config.locations:
                # Properly escape single quotes in location names
                safe_location = location.replace("'", "''")

                # Special case for empty string - use equality check
                if location == "":
                    location_or_remote_conditions.append("LOWER(location) = ''")
                else:
                    location_or_remote_conditions.append(f"LOWER(location) LIKE LOWER('%{safe_location}%')")

        # Add remote filter
        if config.remote is not None and config.remote:
            # Include jobs that mention remote in title or location
            location_or_remote_conditions.append("LOWER(location) LIKE LOWER('%remote%')")
            location_or_remote_conditions.append("LOWER(job_title) LIKE LOWER('%remote%')")
            context.log.info("Adding remote filter: Include remote jobs")
        elif config.remote is not None and not config.remote:
            # Exclude jobs that mention remote in title or location
            query += f" AND NOT (LOWER(location) LIKE LOWER('%remote%') OR LOWER(job_title) LIKE LOWER('%remote%'))"
            context.log.info("Adding remote filter: Exclude remote jobs")

        # Combine location and remote conditions with OR if we have any
        if location_or_remote_conditions:
            query += f" AND ({' OR '.join(location_or_remote_conditions)})"
            context.log.info(f"Location or remote conditions: {' OR '.join(location_or_remote_conditions)}")

        # Add exclusion filters - exclude jobs that match ANY excluded keyword
        if config.excluded_keywords:
            exclusion_conditions = []
            for keyword in config.excluded_keywords:
                # Combine job title and description exclusions
                exclusion_title = f"LOWER(job_title) LIKE LOWER('%{keyword}%')"
                if config.include_descriptions:
                    exclusion_desc = f"LOWER(job_description) LIKE LOWER('%{keyword}%')"
                    # Exclude if keyword appears in EITHER title OR description
                    exclusion_conditions.append(f"({exclusion_title} OR {exclusion_desc})")
                else:
                    exclusion_conditions.append(exclusion_title)

            # Add NOT condition - exclude jobs that match ANY exclusion condition
            if exclusion_conditions:
                query += f" AND NOT ({' OR '.join(exclusion_conditions)})"
                context.log.info(f"Exclusion conditions: NOT ({' OR '.join(exclusion_conditions)})")

        # Add result limit
        query += f" ORDER BY {date_col} DESC LIMIT {config.max_results}"

        # Print the full query for debugging
        context.log.info(f"Full query for {platform}:\n{query}")

        try:
            # Execute query
            context.log.info(f"Executing query for {platform}...")
            query_job = client.query(query)
            results_df = query_job.to_dataframe()

            context.log.info(f"Found {len(results_df)} results from {platform}")

            # Add platform column and combine with other results
            results_df["platform"] = platform
            combined_results = pd.concat([combined_results, results_df], ignore_index=True)

        except Exception as e:
            context.log.error(f"Error querying {platform} jobs: {str(e)}")

    # Calculate execution time
    execution_time = (datetime.now() - start_time).total_seconds() * 1000
    stats["execution_time_ms"] = int(execution_time)

    # Sort combined results by date
    if not combined_results.empty:
        combined_results = combined_results.sort_values(by="posting_date", ascending=False)

    # Calculate relevance scores if keywords are provided
    if config.keywords and not combined_results.empty:
        combined_results["relevance_score"] = combined_results.apply(
            lambda row: calculate_relevance_score(row, config.keywords, config.job_titles, config.locations),
            axis=1
        )

        # Filter by minimum score if needed
        if config.min_match_score > 0:
            prev_count = len(combined_results)
            combined_results = combined_results[combined_results["relevance_score"] >= config.min_match_score]
            context.log.info(f"Filtered {prev_count - len(combined_results)} results below minimum relevance score")

    # Update stats
    stats["total_results"] = len(combined_results)

    # Limit results if needed
    if len(combined_results) > config.max_results:
        combined_results = combined_results.head(config.max_results)

    stats["filtered_results"] = len(combined_results)

    # Save to file if requested
    if config.output_file:
        try:
            # Create the output directory if it doesn't exist
            output_file = config.output_file

            # Replace {date} placeholder with current date if present
            if "{date}" in output_file:
                current_date = datetime.now().strftime("%Y%m%d")
                output_file = output_file.replace("{date}", current_date)

            # Ensure directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                context.log.info(f"Created directory: {output_dir}")

            # Save in the appropriate format
            if config.output_format.lower() == "csv":
                combined_results.to_csv(output_file, index=False)
                context.log.info(f"Saved results to CSV: {output_file}")
            elif config.output_format.lower() == "html":
                context.log.info(f"Generating HTML report for {len(combined_results)} results")
                # Create a nicely formatted HTML file
                html_content = generate_html_report(combined_results, stats)

                # Log the first 100 chars of the HTML to verify content
                context.log.info(f"HTML report generated, length: {len(html_content)} characters")
                context.log.info(f"HTML preview: {html_content[:100]}...")

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                context.log.info(f"Saved results to HTML: {output_file}")
            elif config.output_format.lower() == "json":
                combined_results.to_json(output_file, orient="records", indent=2)
                context.log.info(f"Saved results to JSON: {output_file}")
            else:
                # Default to CSV
                combined_results.to_csv(output_file, index=False)
                context.log.info(f"Saved results to {output_file}")

            # Add file path to metadata
            stats["output_file"] = output_file

        except Exception as e:
            context.log.error(f"Error saving results to file: {str(e)}")
            context.log.error(f"Error details: {type(e).__name__}")
            import traceback
            context.log.error(f"Traceback: {traceback.format_exc()}")

    # Add metadata
    context.add_output_metadata({
        "total_results": MetadataValue.int(stats["total_results"]),
        "filtered_results": MetadataValue.int(stats["filtered_results"]),
        "platforms_searched": MetadataValue.json(stats["query_platforms"]),
        "execution_time_ms": MetadataValue.int(stats["execution_time_ms"]),
        "preview": MetadataValue.md(generate_results_preview(combined_results))
    })

    # Return in requested format
    if config.output_format == "dict":
        return combined_results.to_dict(orient="records")
    else:  # Default to dataframe
        return combined_results

def calculate_relevance_score(row: pd.Series, keywords: List[str], job_titles: List[str], locations: List[str]) -> float:
    """
    Calculate a relevance score between 0.0 and 1.0 for a job based on how well it matches search criteria.
    """
    score = 0.0
    max_score = 0.0

    # Get the text fields to search in
    job_title = str(row.get("job_title", "")).lower()
    job_description = str(row.get("job_description", "")).lower()
    location = str(row.get("location", "")).lower()

    # Score matching keywords (50% of total score)
    if keywords:
        max_score += 0.5
        keyword_matches = sum(1 for kw in keywords if kw.lower() in job_title or kw.lower() in job_description)
        if keyword_matches > 0:
            score += 0.5 * (keyword_matches / len(keywords))

    # Score matching job titles (30% of total score)
    if job_titles:
        max_score += 0.3
        title_matches = sum(1 for title in job_titles if title.lower() in job_title)
        if title_matches > 0:
            score += 0.3 * (title_matches / len(job_titles))

    # Score matching locations (20% of total score)
    if locations:
        max_score += 0.2
        location_matches = sum(1 for loc in locations if loc.lower() in location)
        if location_matches > 0:
            score += 0.2 * (location_matches / len(locations))

    # Normalize the score if we had any scoring criteria
    return score / max_score if max_score > 0 else 0.0

def generate_results_preview(results: pd.DataFrame) -> str:
    """Generate a markdown preview of the results for the Dagster UI."""
    if results.empty:
        return "No results found matching the search criteria."

    preview = "## Job Search Results\n\n"
    preview += f"Found {len(results)} matching jobs.\n\n"

    # Add a table of the first 5 results
    preview += "| Platform | Job Title | Location | Date Posted |\n"
    preview += "|----------|-----------|----------|-------------|\n"

    for _, row in results.head(5).iterrows():
        platform = row.get("platform", "")
        title = row.get("job_title", "")
        location = row.get("location", "")
        date = row.get("posting_date", "")

        if isinstance(date, pd.Timestamp):
            date = date.strftime("%Y-%m-%d")

        preview += f"| {platform} | {title[:50] + '...' if len(title) > 50 else title} "
        preview += f"| {location[:30] + '...' if len(location) > 30 else location} | {date} |\n"

    if len(results) > 5:
        preview += f"\n... and {len(results) - 5} more results."

    return preview

def sanitize_html_description(description: str) -> str:
    """
    Sanitize and prepare job description HTML for safe display.
    This handles common issues with HTML content in job descriptions.
    """
    if not description:
        return "<p><em>No description available</em></p>"

    # Unescape HTML entities
    sanitized = html.unescape(description)

    # Replace any double-escaped entities that might remain
    sanitized = sanitized.replace("&amp;", "&")

    # Fix common issues with HTML formatting
    sanitized = sanitized.replace("\n", " ")
    sanitized = sanitized.replace("\r", " ")

    # Clean up excessive whitespace
    sanitized = re.sub(r'\s+', ' ', sanitized)

    return sanitized

def generate_html_report(results: pd.DataFrame, stats: Dict) -> str:
    """Generate a nicely formatted HTML report from the search results."""
    # Define a clean, modern CSS style
    css = """
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f9f9f9;
        }
        h1, h2, h3 {
            color: #2c3e50;
        }
        .header {
            background-color: #2c3e50;
            color: white;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 20px;
        }
        .stats {
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            margin-bottom: 20px;
        }
        .stat-card {
            background-color: white;
            border-radius: 5px;
            padding: 15px;
            flex: 1;
            min-width: 180px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .job-card {
            background-color: white;
            border-radius: 5px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-left: 5px solid #3498db;
        }
        .job-title {
            color: #3498db;
            font-size: 20px;
            margin-top: 0;
        }
        .job-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            font-size: 14px;
            color: #7f8c8d;
            margin-bottom: 15px;
        }
        .job-meta span {
            display: inline-flex;
            align-items: center;
        }
        .job-meta svg {
            margin-right: 5px;
        }
        .job-description {
            border-top: 1px solid #eee;
            padding-top: 15px;
            max-height: 300px;
            overflow-y: auto;
            line-height: 1.7;
        }
        /* Add styles for proper rendering of HTML content in job descriptions */
        .job-description p {
            margin-bottom: 1em;
        }
        .job-description ul, .job-description ol {
            margin-left: 1.5em;
            margin-bottom: 1em;
        }
        .job-description li {
            margin-bottom: 0.5em;
        }
        .job-description h1, .job-description h2, .job-description h3,
        .job-description h4, .job-description h5 {
            margin-top: 1em;
            margin-bottom: 0.5em;
            color: #2c3e50;
        }
        .job-description a {
            color: #3498db;
            text-decoration: none;
        }
        .job-description a:hover {
            text-decoration: underline;
        }
        .job-description strong, .job-description b {
            font-weight: 600;
        }
        .tag {
            display: inline-block;
            background-color: #e0f7fa;
            color: #00838f;
            padding: 3px 8px;
            border-radius: 3px;
            margin-right: 5px;
            font-size: 12px;
        }
        .relevance {
            position: absolute;
            right: 20px;
            top: 20px;
            background-color: #3498db;
            color: white;
            padding: 5px 10px;
            border-radius: 20px;
            font-weight: bold;
        }
        .remote-tag {
            background-color: #e8f5e9;
            color: #388e3c;
        }
        a {
            color: #3498db;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        .filters {
            background-color: white;
            border-radius: 5px;
            padding: 15px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .filter-group {
            margin-bottom: 10px;
        }
        .filter-title {
            font-weight: bold;
            margin-bottom: 5px;
        }
        .filter-content {
            font-size: 14px;
        }
        .no-results {
            background-color: white;
            border-radius: 5px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
    </style>
    """

    # Generate the HTML
    today = datetime.now().strftime("%B %d, %Y")
    search_date_range = f"{stats['search_params']['date_range']}"
    platforms = ", ".join(stats["query_platforms"])

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Engineering Job Search Results - {today}</title>
        {css}
    </head>
    <body>
        <div class="header">
            <h1>Data Engineering Job Search Results</h1>
            <p>Search performed on {today}</p>
        </div>

        <div class="filters">
            <h2>Search Filters</h2>
            <div class="filter-group">
                <div class="filter-title">Keywords:</div>
                <div class="filter-content">{', '.join(stats['search_params']['keywords']) if stats['search_params']['keywords'] else 'None'}</div>
            </div>
            <div class="filter-group">
                <div class="filter-title">Job Titles:</div>
                <div class="filter-content">{', '.join(stats['search_params']['job_titles']) if stats['search_params']['job_titles'] else 'None'}</div>
            </div>
            <div class="filter-group">
                <div class="filter-title">Locations:</div>
                <div class="filter-content">{', '.join(stats['search_params']['locations']) if stats['search_params']['locations'] else 'None'}</div>
            </div>
            <div class="filter-group">
                <div class="filter-title">Date Range:</div>
                <div class="filter-content">{search_date_range}</div>
            </div>
            <div class="filter-group">
                <div class="filter-title">Platforms:</div>
                <div class="filter-content">{platforms}</div>
            </div>
        </div>
    """

    if results.empty:
        html += """
        <div class="no-results">
            <h2>No Results Found</h2>
            <p>No job postings were found matching your search criteria. Try adjusting your filters or expanding your search.</p>
        </div>
        """
    else:
        html += f"""
        <div class="stats">
            <div class="stat-card">
                <h3>Total Results</h3>
                <p style="font-size: 24px; font-weight: bold;">{stats['total_results']}</p>
            </div>
            <div class="stat-card">
                <h3>Filtered Results</h3>
                <p style="font-size: 24px; font-weight: bold;">{stats['filtered_results']}</p>
            </div>
            <div class="stat-card">
                <h3>Date Range</h3>
                <p>{search_date_range}</p>
            </div>
        </div>

        <h2>Job Listings</h2>
        """

        # Add each job card
        for _, job in results.iterrows():
            # Format date
            if isinstance(job.get('posting_date'), pd.Timestamp):
                posting_date = job['posting_date'].strftime("%B %d, %Y")
            else:
                posting_date = str(job.get('posting_date', 'Unknown date'))

            # Format location and look for remote
            location = job.get('location', 'Unknown location')
            is_remote = False
            if isinstance(location, str) and re.search(r'remote|virtual|work from home|wfh', location.lower()):
                is_remote = True

            # Format job description
            job_description = job.get('job_description', '')
            job_description = sanitize_html_description(job_description)

            # Format platform name for display
            platform_name = job.get('platform', '').capitalize()

            # Score formatted to percentage
            score = job.get('relevance_score', 0)
            score_pct = int(score * 100)

            html += f"""
            <div class="job-card" style="position: relative;">
                <div class="relevance">{score_pct}%</div>
                <h3 class="job-title">{job.get('job_title', 'Unknown title')}{f" - {job.get('company_name')}" if job.get('company_name') else ""}</h3>
                <div class="job-meta">
                    <span>
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M12 2C6.48 2 2 6.48 2 12C2 17.52 6.48 22 12 22C17.52 22 22 17.52 22 12C22 6.48 17.52 2 12 2ZM12 20C7.59 20 4 16.41 4 12C4 7.59 7.59 4 12 4C16.41 4 20 7.59 20 12C20 16.41 16.41 20 12 20Z" fill="currentColor"/>
                            <path d="M12.5 7H11V13L16.2 16.2L17 14.9L12.5 12.2V7Z" fill="currentColor"/>
                        </svg>
                        {posting_date}
                    </span>
                    <span>
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M12 2C8.13 2 5 5.13 5 9C5 14.25 12 22 12 22C12 22 19 14.25 19 9C19 5.13 15.87 2 12 2ZM12 11.5C10.62 11.5 9.5 10.38 9.5 9C9.5 7.62 10.62 6.5 12 6.5C13.38 6.5 14.5 7.62 14.5 9C14.5 10.38 13.38 11.5 12 11.5Z" fill="currentColor"/>
                        </svg>
                        {location}
                    </span>
                    {f'''
                    <span>
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M12 7V3H2v18h20V7H12zM6 19H4v-2h2v2zm0-4H4v-2h2v2zm0-4H4V9h2v2zm0-4H4V5h2v2zm4 12H8v-2h2v2zm0-4H8v-2h2v2zm0-4H8V9h2v2zm0-4H8V5h2v2zm10 12h-8v-2h2v-2h-2v-2h2v-2h-2V9h8v10zm-2-8h-2v2h2v-2zm0 4h-2v2h2v-2z" fill="currentColor"/>
                        </svg>
                        {job.get('company_name', 'Unknown company')}
                    </span>
                    ''' if job.get('company_name') else ''}
                    <span>
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M20 6H16V4C16 2.89 15.11 2 14 2H10C8.89 2 8 2.89 8 4V6H4C2.89 6 2 6.89 2 8V19C2 20.11 2.89 21 4 21H20C21.11 21 22 20.11 22 19V8C22 6.89 21.11 6 20 6ZM10 4H14V6H10V4ZM20 19H4V8H20V19Z" fill="currentColor"/>
                        </svg>
                        {platform_name}
                    </span>
                </div>
                <div>
                    {f'<span class="tag remote-tag">Remote</span>' if is_remote else ''}
                    {f'<span class="tag">{job.get("department", "")}</span>' if job.get("department") else ''}
                </div>
                <p><a href="{job.get('job_url', '#')}" target="_blank">Apply on company website</a></p>
                <div class="job-description">
            """
            # Add job description directly without string interpolation to avoid escaping
            html += job_description
            html += """
                </div>
            </div>
            """

    html += """
    </body>
    </html>
    """

    return html

# Create Dagster Definitions object for deployment
defs = Definitions(
    assets=[search_jobs]
)