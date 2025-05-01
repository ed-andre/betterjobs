from dagster import AssetSelection, define_asset_job, OpExecutionContext, op, job, Config, In, Out

# Define jobs based on asset selection
job_scraping_job = define_asset_job(
    name="job_scraping_job",
    selection=AssetSelection.groups("job_scraping"),
    description="Job that scrapes job listings for configured companies"
)

# Define a job specifically for discovering Workday company URLs
workday_url_discovery_job = define_asset_job(
    name="workday_url_discovery_job",
    selection=AssetSelection.assets("workday_company_urls", "retry_failed_workday_company_urls"),
    description="Job that specifically discovers URLs for companies using Workday ATS"
)

# Define a job specifically for discovering Greenhouse company URLs
greenhouse_url_discovery_job = define_asset_job(
    name="greenhouse_url_discovery_job",
    selection=AssetSelection.assets("greenhouse_company_urls", "retry_failed_greenhouse_company_urls"),
    description="Job that specifically discovers URLs for companies using Greenhouse ATS"
)

# Define a job specifically for discovering BambooHR company URLs
bamboohr_url_discovery_job = define_asset_job(
    name="bamboohr_url_discovery_job",
    selection=AssetSelection.assets("bamboohr_company_urls", "retry_failed_bamboohr_company_urls"),
    description="Job that specifically discovers URLs for companies using BambooHR ATS"
)

# Define a job specifically for discovering iCIMS company URLs
icims_url_discovery_job = define_asset_job(
    name="icims_url_discovery_job",
    selection=AssetSelection.assets("icims_company_urls", "retry_failed_icims_company_urls"),
    description="Job that specifically discovers URLs for companies using iCIMS ATS"
)

# Define a job specifically for discovering Jobvite company URLs
jobvite_url_discovery_job = define_asset_job(
    name="jobvite_url_discovery_job",
    selection=AssetSelection.assets("jobvite_company_urls", "retry_failed_jobvite_company_urls"),
    description="Job that specifically discovers URLs for companies using Jobvite ATS"
)

# Define a job specifically for discovering Lever company URLs
lever_url_discovery_job = define_asset_job(
    name="lever_url_discovery_job",
    selection=AssetSelection.assets("lever_company_urls", "retry_failed_lever_company_urls"),
    description="Job that specifically discovers URLs for companies using Lever ATS"
)

# Define a job specifically for discovering SmartRecruiters company URLs
smartrecruiters_url_discovery_job = define_asset_job(
    name="smartrecruiters_url_discovery_job",
    selection=AssetSelection.assets("smartrecruiters_company_urls", "retry_failed_smartrecruiters_company_urls"),
    description="Job that specifically discovers URLs for companies using SmartRecruiters ATS"
)

# Define a job for all URL discovery
full_url_discovery_job = define_asset_job(
    name="full_url_discovery_job",
    selection=AssetSelection.groups("url_discovery"),
    description="Job that discovers URLs for all companies across all ATS platforms"
)

# Define a job for maintaining the master company URLs table
master_company_urls_job = define_asset_job(
    name="master_company_urls_job",
    selection=AssetSelection.assets("master_company_urls"),
    description="Job that maintains the master table of all company URLs"
)
