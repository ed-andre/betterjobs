-- Companies table to store company information
CREATE TABLE IF NOT EXISTS companies (
    company_id INTEGER PRIMARY KEY,
    company_name TEXT NOT NULL,
    company_industry TEXT,
    employee_count_range TEXT,
    city TEXT,
    platform TEXT NOT NULL,
    ats_url TEXT,
    career_url TEXT,
    url_verified BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_name, platform)
);

-- Raw job listings table to store jobs scraped from API
CREATE TABLE IF NOT EXISTS raw_job_listings (
    raw_job_id INTEGER PRIMARY KEY,
    company_name TEXT NOT NULL,
    job_title TEXT NOT NULL,
    date_posted TEXT,
    job_url TEXT,
    job_url_verified BOOLEAN DEFAULT FALSE,
    date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Processed jobs table to store detailed job information
CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL,
    raw_job_id INTEGER,
    job_title TEXT NOT NULL,
    job_description TEXT,
    job_url TEXT NOT NULL,
    location TEXT,
    date_posted TIMESTAMP,
    date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (company_id) REFERENCES companies(company_id),
    FOREIGN KEY (raw_job_id) REFERENCES raw_job_listings(raw_job_id)
);

-- Create indexes for faster searching
CREATE INDEX IF NOT EXISTS idx_companies_platform ON companies(platform);
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(company_name);
CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies(company_industry);
CREATE INDEX IF NOT EXISTS idx_companies_city ON companies(city);
CREATE INDEX IF NOT EXISTS idx_companies_verified ON companies(url_verified);

CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON raw_job_listings(company_name);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_title ON raw_job_listings(job_title);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_processed ON raw_job_listings(processed);

CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id);
CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs(job_title);
CREATE INDEX IF NOT EXISTS idx_jobs_date ON jobs(date_posted);
CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);