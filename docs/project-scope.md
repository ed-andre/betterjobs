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
