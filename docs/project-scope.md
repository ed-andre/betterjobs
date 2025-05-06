# BetterJobs Project Scope

## Project Overview
BetterJobs aims to develop a comprehensive job search platform that retrieves job postings directly from company career portals across various Applicant Tracking Systems (ATS) such as Workday, Greenhouse, Jobvite, Lever, SmartRecruiters, ICIMS, and BambooHR. This approach allows users to discover job opportunities as soon as they are posted, rather than waiting for them to appear on aggregator sites like LinkedIn or Indeed.

## Goals
1. Create a robust data pipeline using Dagster to:
   - Retrieve job listings from company career sites
   - Process and standardize job data from different platforms
   - Store structured job data in BigQuery database
   - Schedule regular updates to maintain fresh listings

2. Develop a user interface to:
   - Search and browse job listings
   - Filter by job title, company, location, etc.
   - Set up alerts for specific job searches
   - View detailed job information including requirements, descriptions, and application links

## Minimum Viable Product (MVP)
The MVP will focus on delivering a working system that can:

1. Generate a list of job listings for a specific search term (e.g., "SQL Developer") including:
   - Job title
   - Company name
   - Direct link to the job posting
   - Basic job description
   - Date posted (if available)

2. Support at least three ATS platforms initially (e.g., Workday, Greenhouse, and BambooHR)

3. Provide a simple interface for performing searches and viewing results

## Development Plan

### Phase 1: Data Collection Infrastructure
1. **Company URL Discovery** ✅
   - ✅ Create a Dagster asset to use Google Gemini API to find career site URLs for each company in the CSV files
   - ✅ Store company data (name, industry, platform, URL) in BigQuery
   - ✅ Implement validation to ensure URLs are correct and accessible
   - ✅ Create master company URLs table to consolidate data from all platforms

2. **Basic Scraper Framework** ✅
   - ✅ Develop platform-specific scrapers for the initial target platforms
   - ✅ Create base classes and utilities for common scraping functions
   - ✅ Implement rate limiting and error handling to prevent blocking

### Phase 2: Job Data Extraction
1. **Platform-Specific Scrapers**
   - BambooHR Job Discovery Implementation (Current Focus):
     - Create BambooHR-specific scraper class extending BaseScraper
     - Implement JSON API handling for efficient data retrieval
     - Develop efficient API request pattern to manage rate limits
     - Create filtering mechanism for recent job postings only
     - Store complete job information including detailed descriptions
     - Implement date validation to ensure freshness of listings
   - Create scrapers for each supported ATS (starting with 3 for MVP)
   - Handle authentication and navigation through each platform
   - Extract consistent job data across different platforms

2. **Job Data Processing**
   - Standardize job data from different sources
   - Clean and normalize text fields
   - Extract key information from job descriptions

3. **Data Storage**
   - Design and implement BigQuery schema for storing job data
   - Create indexes for efficient querying
   - Implement update/merge logic to handle new and changed listings

### Phase 3: Search and Interface
1. **Search Implementation**
   - Create search functionality across job title, description, requirements
   - Implement filtering by company, date posted, etc.
   - Support for basic boolean queries

2. **User Interface**
   - Develop a simple web interface for job searching
   - Create views for search results and job details
   - Implement basic user preferences

### Phase 4: Alerts and Enhancements
1. **Alert System**
   - Create functionality for users to save searches
   - Implement notification system for new matching jobs
   - Support email or in-app notifications

2. **Additional Platforms**
   - Extend support to remaining ATS platforms
   - Refine scrapers based on initial experience

## Architecture Overview
- **Dagster Pipeline**: Orchestrates the entire data workflow
- **BigQuery**: Provides scalable cloud storage for job and company data
- **Web Frontend**: Simple interface for interaction with the system
- **Scraper Modules**: Platform-specific code for data extraction
- **API Layer**: Services the frontend and provides data access

## Technical Considerations
- Use asynchronous programming for efficient data discovery
- Implement proper error handling and logging
- Follow ethical data discovery practices (respect robots.txt, reasonable request rates)
- Ensure data privacy compliance
- Design for extensibility to add more platforms over time

## BambooHR Implementation Plan

### BambooHR Discovery Process
1. **Asset Development**
   - Create `bamboohr_company_jobs_discovery` asset in Dagster
   - Implement efficient API requests to BambooHR endpoints
   - Store job data with minimal transformation to preserve JSON structure

2. **Data Collection Strategy**
   - For each company using BambooHR:
     - Request job listings from `https://[company-domain].bamboohr.com/careers/list`
     - Process listing JSON to extract basic job information
     - For each job, request detailed information from `https://[company-domain].bamboohr.com/careers/[job-id]/detail`
     - Validate job posting dates against cutoff date (April 19, 2025)
     - Store job data in structured format

3. **Rate Limiting & Performance**
   - Implement adaptive rate limiting based on server response
   - Use batch processing to reduce number of requests
   - Implement proper error handling for failed requests
   - Add retry logic with exponential backoff

4. **Data Storage**
   - Store job metadata in standard format in BigQuery
   - Preserve original JSON for detailed fields
   - Implement efficient update mechanisms for existing jobs

5. **Monitoring & Validation**
   - Track success/failure rates for each company
   - Log detailed error information for troubleshooting
   - Validate data integrity before storage

### Implementation Milestones
1. Create BambooHR scraper class
2. Implement job listing discovery
3. Develop job detail retrieval
4. Add date filtering functionality
5. Integrate with existing database schema
6. Test with sample of companies
7. Optimize for performance and reliability
8. Deploy at scale to process all ~900 BambooHR companies

## Greenhouse Implementation Plan

### Overview
For Greenhouse job discovery, we need to handle two distinct scenarios:
1. Direct Greenhouse API integration for companies with standard Greenhouse boards (`https://boards.greenhouse.io/[tenant]`)
2. Beautiful Soup HTML parsing for embedded Greenhouse boards (`https://boards.greenhouse.io/embed/job_board?for=[tenant]`)

### Greenhouse Direct API Implementation
1. **Scraper Development**
   - Create `GreenhouseScraper` class extending `BaseScraper`
   - Implement specialized request handling for Greenhouse API endpoints
   - Extract job listings and details from JSON response data

2. **Data Collection Strategy**
   - For companies with standard Greenhouse boards:
     - Access the initial board URL (e.g., `https://job-boards.greenhouse.io/[tenant]`)
     - Extract JSON data from the embedded JavaScript in the response
     - Parse job listings from the `routes/$url_token` section containing job IDs, titles, locations, departments, and URLs
     - For each job, capture detailed information including posting date, job description, and requirements
     - Validate job posting dates against cutoff date
     - Store job data in structured format

3. **Data Extraction Fields**
   - Primary job fields to extract:
     - `id`: Unique job identifier
     - `title`: Job title
     - `location`: Job location
     - `department`: Department name and ID
     - `absolute_url`: Direct URL to job posting
     - `published_at`: Date job was published
     - `updated_at`: Last update timestamp
     - `content`: Full job description and requirements
     - `requisition_id`: External reference ID when available

4. **Performance & Reliability**
   - Implement robust error handling for API rate limits
   - Add retry logic with exponential backoff
   - Use batch processing to handle large job volumes
   - Validate response integrity before processing

### Greenhouse Embedded Implementation
1. **Scraper Development**
   - Create `GreenhouseEmbeddedScraper` class for embedded job boards
   - Implement Beautiful Soup-based HTML parsing
   - Extract job listings and details from DOM elements

2. **Data Collection Strategy**
   - For companies with embedded Greenhouse boards:
     - Request the HTML page containing job listings
     - Parse the DOM to extract job sections, departments, and individual listings
     - For each job, request the detail page to get full job description
     - Normalize data to match format from direct API implementation
     - Validate job dates when available

3. **Embedded URL Scraping Process Details**
   - **For Job Listings**:
     - Parse HTML from URLs following pattern: `https://boards.greenhouse.io/embed/job_board?for=[tenant]`
     - Extract job listings from HTML elements with class="opening"
     - Obtain job title from `<a>` tags, location from `<span class="location">` tags
     - Extract job IDs from URL's `gh_jid` parameter in href attributes

   - **For Job Details**:
     - Instead of using direct job URLs which don't provide structured data
     - Extract the `gh_jid` from the URL and construct a modified URL:
       - Original: `https://www.dayonebio.com/careers/open-positions/?gh_jid=4496186008`
       - Modified: `https://job-boards.greenhouse.io/embed/job_app?for=dayonebiopharmaceuticals&token=4496186008`
     - This modified URL returns structured job details in JSON format

   - **Implementation Approach**:
     - Use BeautifulSoup for HTML parsing
     - Extract job listings with their IDs, titles, and locations
     - Construct proper URLs for job details
     - Make secondary requests to retrieve detailed job information
     - Ensure consistent data format compatible with direct API implementation

3. **Integration & Testing**
   - Develop unified interface for both scraper types
   - Implement detection logic to determine which scraper to use
   - Test with sample companies from both categories
   - Validate data quality and completeness

### Dagster Asset Implementation
1. **Asset Development**
   - Create `greenhouse_company_jobs_discovery` asset in Dagster
   - Implement partitioning strategy similar to BambooHR implementation
   - Support incremental processing with checkpoints

2. **Data Storage**
   - Design BigQuery schema for Greenhouse job data
   - Implement data transformation and loading logic
   - Ensure schema compatibility with BambooHR data for unified querying

3. **Monitoring & Validation**
   - Track processing metrics for each company
   - Log detailed information for troubleshooting
   - Implement data quality checks

### Implementation Milestones
1. Create base Greenhouse scraper class
2. Implement direct API job listing discovery
3. Implement embedded board HTML parser
4. Develop job detail retrieval logic
5. Add date filtering functionality
6. Create Dagster asset for job discovery
7. Integrate with existing database schema
8. Test with sample companies from both categories
9. Optimize for performance and reliability
10. Deploy at scale for all Greenhouse companies

## SmartRecruiters Implementation Plan

### Overview
For SmartRecruiters job discovery, we need to parse HTML responses using BeautifulSoup, as SmartRecruiters doesn't expose a public API for job listings. The implementation focuses on extracting job listings from the main careers page and then fetching detailed information from individual job pages.

### SmartRecruiters Implementation
1. **Scraper Development**
   - Create `SmartRecruitersJobScraper` class extending `BaseScraper`
   - Implement BeautifulSoup-based HTML parsing for both job listings and details
   - Handle proper request formatting with appropriate headers for SmartRecruiters sites

2. **Data Collection Strategy**
   - For career pages using SmartRecruiters:
     - Request the main careers page HTML (e.g., `https://careers.smartrecruiters.com/ServiceNow/`)
     - Parse job listings using `h4.details-title.job-title.link--block-target` CSS selector
     - Extract job title from the element text and job URL from the parent `<a>` tag
     - For each job, request the detail page to extract comprehensive job information
     - Extract metadata from HTML tags and meta tags in job detail pages
     - Store standardized job data with consistent fields matching other scrapers

3. **Data Extraction Fields**
   - Primary job fields to extract:
     - `job_id`: Extracted from URL path segments or generated as fallback
     - `job_title`: Job title text
     - `location`: Job location from `span[itemprop="address"]`
     - `job_description`: Concatenated text from `div.wysiwyg` elements
     - `published_at`: Date from `meta[itemprop="datePosted"]`
     - `updated_at`: From meta tags or same as published_at if not available
     - `requisition_id`: From `meta[name="sr:job-ad-id"]`
     - `department`: From job detail elements when available

4. **Date Handling & Freshness**
   - Parse ISO format dates from meta tags
   - Implement timezone handling for proper date comparisons
   - Filter jobs based on cutoff date (default 14 days)
   - Maintain consistent date format across all job records

5. **Request Management**
   - Implement specialized headers required for SmartRecruiters sites
   - Use rate limiting to prevent blocking
   - Add retry logic with proper error handling
   - Provide detailed logging for troubleshooting

### Implementation Approach
1. **For Job Listings**:
   - Parse HTML from main career pages
   - Extract job listings based on consistent SmartRecruiters DOM structure
   - Handle both absolute and relative URLs for job detail pages
   - Generate reliable job IDs from URL paths or as fallback from content hash

2. **For Job Details**:
   - Request individual job detail pages
   - Parse structured HTML to extract comprehensive job information
   - Extract metadata from meta tags for dates and identifiers
   - Combine all information into a standardized job record

3. **Integration & Testing**
   - Ensure consistent data format compatible with other scrapers
   - Test with sample companies using SmartRecruiters
   - Validate data quality and completeness

### Dagster Asset Implementation
1. **Asset Development**
   - Create `smartrecruiters_company_jobs_discovery` asset in Dagster
   - Implement consistent partitioning strategy
   - Support incremental processing with appropriate checkpoints

2. **Data Storage**
   - Use BigQuery schema compatible with other ATS platforms
   - Ensure consistent field naming and data types
   - Implement efficient update/merge logic for new and changed listings

### Implementation Milestones
1. Create SmartRecruiters scraper class
2. Implement job listing discovery
3. Develop job detail parsing
4. Add metadata extraction and date filtering
5. Create Dagster asset for job discovery
6. Integrate with existing database schema
7. Test with sample companies
8. Optimize for performance and reliability
9. Deploy at scale for all SmartRecruiters companies
