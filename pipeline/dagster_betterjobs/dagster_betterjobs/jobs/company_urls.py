from dagster import job, AssetSelection

# Job that runs all company URL discovery assets
company_urls_job = job(
    name="company_urls_job",
    description="Discovers company URLs for all supported ATS platforms",
    selection=AssetSelection.groups("company_urls") - AssetSelection.keys("retry_failed_*"),
)

# Job that retries failed company URLs
retry_urls_job = job(
    name="retry_urls_job",
    description="Retries failed company URL discoveries",
    selection=AssetSelection.keys("retry_failed_*"),
)