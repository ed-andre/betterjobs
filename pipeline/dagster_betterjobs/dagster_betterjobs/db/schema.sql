-- Companies table to store company information
CREATE TABLE IF NOT EXISTS companies (
    company_id INT64,
    company_name STRING NOT NULL,
    company_industry STRING,
    employee_count_range STRING,
    city STRING,
    platform STRING NOT NULL,
    ats_url STRING,
    career_url STRING,
    url_verified BOOL DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_companies PRIMARY KEY (company_id),
    CONSTRAINT unique_company_platform UNIQUE(company_name, platform)
);

-- Processed jobs table to store detailed job information
CREATE TABLE IF NOT EXISTS jobs (
    job_id INT64,
    company_id INT64 NOT NULL,
    job_title STRING NOT NULL,
    job_description STRING,
    job_url STRING NOT NULL,
    location STRING,
    date_posted TIMESTAMP,
    date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    is_active BOOL DEFAULT TRUE,
    CONSTRAINT pk_jobs PRIMARY KEY (job_id),
    CONSTRAINT fk_jobs_company FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- Create indexes for faster searching
CREATE INDEX IF NOT EXISTS idx_companies_platform ON companies(platform);
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(company_name);
CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies(company_industry);
CREATE INDEX IF NOT EXISTS idx_companies_city ON companies(city);
CREATE INDEX IF NOT EXISTS idx_companies_verified ON companies(url_verified);

CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id);
CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs(job_title);
CREATE INDEX IF NOT EXISTS idx_jobs_date ON jobs(date_posted);
CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);