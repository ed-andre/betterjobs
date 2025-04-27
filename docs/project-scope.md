# BetterJobs Project Scope

## Project Overview
BetterJobs aims to develop a comprehensive job search platform that retrieves job postings directly from company career portals across various Applicant Tracking Systems (ATS) such as Workday, Greenhouse, Jobvite, Lever, SmartRecruiters, and ICIMS. This approach allows users to discover job opportunities as soon as they are posted, rather than waiting for them to appear on aggregator sites like LinkedIn or Indeed.

## Goals
1. Create a robust data pipeline using Dagster to:
   - Retrieve job listings from company career sites
   - Process and standardize job data from different platforms
   - Store structured job data in a DuckDB database
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

2. Support at least two ATS platforms initially (e.g., Workday and Greenhouse)

3. Provide a simple interface for performing searches and viewing results

## Development Plan

### Phase 1: Data Collection Infrastructure
1. **Company URL Discovery**
   - Create a Dagster asset to use Google Gemini API to find career site URLs for each company in the CSV files
   - Store company data (name, industry, platform, URL) in DuckDB
   - Implement validation to ensure URLs are correct and accessible

2. **Basic Scraper Framework**
   - Develop platform-specific scrapers for the initial target platforms
   - Create base classes and utilities for common scraping functions
   - Implement rate limiting and error handling to prevent blocking

### Phase 2: Job Data Extraction
1. **Platform-Specific Scrapers**
   - Create scrapers for each supported ATS (starting with 2 for MVP)
   - Handle authentication and navigation through each platform
   - Extract consistent job data across different platforms

2. **Job Data Processing**
   - Standardize job data from different sources
   - Clean and normalize text fields
   - Extract key information from job descriptions

3. **Data Storage**
   - Design and implement DuckDB schema for storing job data
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
- **DuckDB**: Provides lightweight but powerful storage for job and company data
- **Web Frontend**: Simple interface for interaction with the system
- **Scraper Modules**: Platform-specific code for data extraction
- **API Layer**: Services the frontend and provides data access

## Technical Considerations
- Use asynchronous programming for efficient scraping
- Implement proper error handling and logging
- Follow ethical scraping practices (respect robots.txt, reasonable request rates)
- Ensure data privacy compliance
- Design for extensibility to add more platforms over time
