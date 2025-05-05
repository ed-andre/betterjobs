# Greenhouse Embedded Job Board Scraper

This document explains the implementation and usage of the Greenhouse Embedded Job Board Scraper within the BetterJobs project.

## Overview

The Greenhouse Embedded Job Board Scraper is designed to extract job listings from companies that use Greenhouse's embedded job board format. These boards follow the URL pattern: `https://boards.greenhouse.io/embed/job_board?for=[tenant]`.

This implementation handles two key aspects:

1. **Job Listing Discovery**: Parsing HTML from embedded boards to extract basic job information
2. **Job Detail Extraction**: Using a specialized URL format to obtain structured job data in JSON format

## Implementation Details

### Job Listing Discovery

For embedded job boards, the scraper:

1. Makes a request to the embedded job board URL
2. Parses the HTML using BeautifulSoup
3. Finds job listings within elements with the class "opening"
4. Extracts:
   - Job title from the link text
   - Job URL from the link href attribute
   - Location from the span with class "location"
   - Job ID from the gh_jid parameter in the URL

### Job Detail Extraction

For job details, the scraper:

1. Extracts the job ID from the original URL's gh_jid parameter
2. Constructs a specialized URL: `https://job-boards.greenhouse.io/embed/job_app?for={tenant}&token={job_id}`
3. Makes a request to this URL, which returns structured JSON data
4. Extracts key fields:
   - Job title
   - Location
   - Description content
   - Department information
   - Publishing and update timestamps
5. Includes a fallback to HTML parsing if JSON parsing fails

## Usage

### Within the Dagster Pipeline

The scraper is fully integrated with the existing Greenhouse scraper class and is selected automatically when an embedded board URL is detected.

```python
from dagster_betterjobs.scrapers import create_scraper

# The factory function detects embedded boards automatically
scraper = create_scraper(
    platform="greenhouse",
    career_url="https://boards.greenhouse.io/embed/job_board?for=companyname",
    cutoff_date=cutoff_date
)

# Use standard methods
jobs = scraper.search_jobs(keyword="engineer")
for job in jobs:
    details = scraper.get_job_details(job["job_url"])
    # Process job details...
```

### Testing with the Test Script

A test script is provided to try out the embedded scraper:

```bash
# Run the test script with an embedded board URL
python tests/greenhouse_embedded_test.py --url "https://boards.greenhouse.io/embed/job_board?for=dayonebiopharmaceuticals" --keyword "engineer" --detail

# Additional options
python tests/greenhouse_embedded_test.py --url "https://boards.greenhouse.io/embed/job_board?for=dayonebiopharmaceuticals" --keyword "data" --location "remote" --detail --max-jobs 5 --show-raw
```

## URL Patterns

### Embedded Board URLs
- Main board: `https://boards.greenhouse.io/embed/job_board?for=[tenant]`
- Job listing: Various formats, but always contains a `gh_jid` parameter
  - Example: `https://www.dayonebio.com/careers/open-positions/?gh_jid=4496186008`

### Job Detail API
- Format: `https://job-boards.greenhouse.io/embed/job_app?for=[tenant]&token=[job_id]`
- This returns a structured JSON response with complete job details

## Common Issues and Troubleshooting

1. **Tenant extraction failure**:
   - Make sure the URL follows the proper format with the "for" parameter

2. **No jobs found**:
   - Verify the embedded board URL is correct
   - Check if the company has any active job listings

3. **Job ID extraction issues**:
   - Ensure the job URLs contain the "gh_jid" parameter

4. **JSON parsing errors**:
   - The fallback HTML parsing will be used automatically
   - Check if Greenhouse has changed their API format

## Future Improvements

1. Add support for pagination for companies with many job listings
2. Improve error handling for various URL formats
3. Add support for additional job filters
4. Implement more robust date parsing for different formats