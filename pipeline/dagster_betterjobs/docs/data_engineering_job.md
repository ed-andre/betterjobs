# Data Engineering Job Search

This document explains how to use the preconfigured data engineering job search in the BetterJobs project.

## Overview

The `data_engineering_job` is a preconfigured Dagster job that searches for data and database engineering positions that match your criteria. It's designed to find recent job postings in New York, New Jersey, or remote locations.

## Job Configuration

The job uses the following search parameters:

- **Keywords**: SQL, database, ETL, pipeline, data engineer
- **Job Titles**: SQL, Database, Data, Software, BI, Developer, Engineer, Analyst
- **Excluded Keywords**: overseas only, non-US, offshore
- **Locations**: New York, New Jersey, NY, NJ (plus remote positions and empty location values)
- **Time Frame**: Jobs posted within the last 20 days
- **Relevance Threshold**: 0.1 (lower threshold to include more potential matches)
- **Maximum Results**: 500

This configuration is optimized to find a wide range of data engineering positions while excluding roles that specifically require overseas/offshore locations. The granular job titles allow for more precise matching with different position naming conventions.

## Output Format

The job creates a well-formatted HTML report with all job listings organized in a visually appealing format. The report includes:

- Search parameters summary
- Statistics about the search results
- Job listings with:
  - Job title and relevance score
  - Location and posting date
  - Platform information
  - Full job description
  - Direct link to apply

The HTML output is saved to the `output` directory with a date-stamped filename: `output/data_engineering_jobs_{date}.html`.

## Running the Job

### From the Dagster UI

1. Navigate to the Dagster UI
2. Go to the "Jobs" tab
3. Find and select the "data_engineering_job"
4. Click "Launch Run"

### From the Command Line

```bash
dagster job execute -m dagster_betterjobs.jobs -j data_engineering_job
```

## Customizing the Job Configuration

If you want to modify the search parameters for a specific run, you can do so by providing a run config:

```yaml
ops:
  search_jobs:
    config:
      keywords: ["SQL", "database", "ETL", "pipeline", "data engineer"]
      job_titles: ["SQL", "Database", "Data", "Software", "BI ", "Developer", "Engineer", "Analyst"]
      excluded_keywords: ["overseas only", "non-US", "offshore"]
      locations: ["New York", "New Jersey", "NY", "NJ", ""]
      remote: true
      days_back: 20
      max_results: 500
      min_match_score: 0.1
      output_format: "html"
      output_file: "output/data_engineering_jobs_{date}.html"
      include_descriptions: true
```

Save this config to a file (e.g., `custom_config.yaml`) and run:

```bash
dagster job execute -m dagster_betterjobs.jobs -j data_engineering_job --config custom_config.yaml
```

## Understanding Results

The relevance score (0.0-1.0) is calculated based on:

- Keyword matches in job titles and descriptions (50% of score)
- Job title matches (30% of score)
- Location matches (20% of score)

With the current configuration (`min_match_score: 0.1`), the job will include a wide range of potential matches while still filtering out completely irrelevant positions.

## Location Handling

The job has been configured to handle various location scenarios:

- Specific location matches (New York, New Jersey, NY, NJ)
- Empty location fields (which may indicate remote positions)
- Remote keywords in title or location

The SQL query specifically uses an equality check (`LOWER(location) = ''`) for empty location values rather than a wildcard pattern to avoid over-inclusion.

## Setting Up a Daily Job Alert

You can set up a daily scheduled run to receive fresh job postings every morning:

```python
@daily_schedule(
    job=data_engineering_job,
    hour=8,
    minute=0,
)
def daily_data_engineering_jobs():
    return RunRequest(
        run_key=f"data_engineering_jobs_{datetime.now().strftime('%Y%m%d')}",
    )
```

Add this schedule to your Dagster definitions to receive daily updates.

## Interpreting Job Descriptions

The asset does not perform advanced NLP analysis on job descriptions. However, you might want to look for the following in the results:

- **Required technical skills**: SQL proficiency level, database technologies, ETL tools
- **Years of experience**: Look for appropriate seniority level
- **Work environment**: Remote, hybrid, or on-site
- **Application process**: Direct application links