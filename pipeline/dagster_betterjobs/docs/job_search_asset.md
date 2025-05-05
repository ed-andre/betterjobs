# Job Search Asset

This document explains how to use the `search_jobs` asset for searching and filtering jobs across multiple ATS platforms.

## Overview

The job search asset provides a flexible way to search for job postings across different ATS platforms (Greenhouse, BambooHR, etc.) with various filtering options. It's designed to help users quickly find relevant job postings based on keywords, job titles, locations, and other criteria.

## Configuration Options

The asset accepts the following configuration parameters:

### Search Parameters

- `keywords`: List of keywords to search for in job titles and descriptions
- `job_titles`: Specific job titles to search for
- `excluded_keywords`: Keywords to exclude from results
- `locations`: Locations to include in search (use empty string "" to match jobs with empty location fields)
- `remote`: Filter for remote jobs (True = include remote, False = exclude remote, None = don't filter)

### ATS Platform Selection

- `platforms`: List of platforms to search (default: ["all"])
  - Options: "all", "greenhouse", "bamboohr", etc.

### Date Range

- `days_back`: Default number of days to look back (default: 30)
- `date_from`: Start date in "YYYY-MM-DD" format
- `date_to`: End date in "YYYY-MM-DD" format

### Results Control

- `max_results`: Maximum number of results to return (default: 100)
- `min_match_score`: Minimum relevance score threshold (0.0-1.0)
- `output_format`: Format for returned data ("dataframe", "dict", "csv", "html")
- `output_file`: Path to save results to file

### Advanced Options

- `include_descriptions`: Include full job descriptions in output (default: True)
- `include_raw_data`: Include raw API response data in output (default: False)

## Special Location Handling

The asset includes special handling for location values:

- Regular location strings are matched using a `LIKE '%location%'` pattern
- Empty string location values ("") are matched using an exact equality check (`location = ''`)
- When `remote` is set to True, positions mentioning "remote" in either title or location are included
- You can combine specific locations, empty location matches, and remote filtering for comprehensive results

This approach allows for matching remote jobs that might be indicated by an empty location field, while preventing over-inclusion from wildcard patterns.

## Example Usage

### Basic Search for Software Engineering Jobs

```python
result = search_jobs(
    config={"keywords": ["software engineer", "developer"], "days_back": 14}
)
```

### Search for Remote Data Engineering Jobs

```python
result = search_jobs(
    config={
        "keywords": ["data engineer", "ETL", "python"],
        "locations": ["", "Remote"],  # Match empty locations and "Remote"
        "remote": True,
        "platforms": ["greenhouse", "bamboohr"]
    }
)
```

### Advanced Search with Multiple Filters

```python
result = search_jobs(
    config={
        "keywords": ["machine learning", "AI", "data science"],
        "job_titles": ["engineer", "scientist"],
        "excluded_keywords": ["senior", "lead", "manager"],
        "locations": ["New York", "Boston", "Remote", ""],
        "date_from": "2023-01-01",
        "date_to": "2023-04-01",
        "max_results": 50,
        "min_match_score": 0.4,
        "output_format": "html",
        "output_file": "ml_jobs_q1_2023.html"
    }
)
```

## How Relevance Scoring Works

The asset calculates a relevance score (0.0-1.0) for each job based on:

- Keyword matches (50% of score)
- Job title matches (30% of score)
- Location matches (20% of score)

Jobs are ranked by this score, and results below the `min_match_score` threshold are filtered out.

## Output Format

The asset returns a pandas DataFrame by default, with the following columns:

- `platform`: Source platform (greenhouse, bamboohr, etc.)
- `job_id`: Unique job identifier
- `company_id`: Company identifier
- `job_title`: Job title
- `job_description`: Full job description (if `include_descriptions` is True)
- `job_url`: URL to the original job posting
- `location`: Job location
- `department`: Department (if available)
- `posting_date`: Date the job was posted
- `date_retrieved`: Date the job was retrieved
- `relevance_score`: Calculated relevance score
- `is_active`: Whether the job is still active

## Adding as a Dependency

To run this asset after job discovery assets, update the `deps` parameter in the asset decorator:

```python
@asset(
    group_name="job_search",
    kinds={"bigquery", "python"},
    required_resource_keys={"bigquery"},
    deps=["greenhouse_company_jobs_discovery", "bamboohr_company_jobs_discovery"]
)
```

## Example Materializations

### Daily Job Alert for Data Science Positions

```python
@daily_schedule(
    job=job_search_job,
    hour=8,
    minute=0,
)
def daily_data_science_jobs():
    return RunRequest(
        run_key=f"data_science_jobs_{datetime.now().strftime('%Y%m%d')}",
        run_config={
            "ops": {
                "search_jobs": {
                    "config": {
                        "keywords": ["data science", "machine learning", "AI"],
                        "days_back": 1,
                        "remote": True,
                        "output_format": "html",
                        "output_file": f"data_science_jobs_{datetime.now().strftime('%Y%m%d')}.html"
                    }
                }
            }
        }
    )
```