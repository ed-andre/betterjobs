from dagster import (
    AssetSelection,
    define_asset_job,
    OpExecutionContext,
    op,
    job,
    Config,
    In,
    Out,
    RunConfig,
    Definitions,
    static_partitioned_config
)
from dagster_betterjobs.assets.bamboohr_jobs_discovery import alpha_partitions as bamboo_partitions_def
from dagster_betterjobs.assets.greenhouse_jobs_discovery import alpha_partitions as greenhouse_partitions_def
from dagster_betterjobs.assets.workday_jobs_discovery import alpha_partitions as workday_partitions_def
from dagster_betterjobs.assets.smartrecruiters_jobs_discovery import alpha_partitions as smartrecruiters_partitions_def
from dagster_betterjobs.assets.icims_jobs_discovery import alpha_partitions as icims_partitions_def
import os

# Use bamboo_partitions_def for alpha_partitions
alpha_partitions = bamboo_partitions_def

# Create partitioned configs for each platform
@static_partitioned_config(partition_keys=bamboo_partitions_def.get_partition_keys())
def bamboohr_partitioned_config(partition_key: str):
    return {
        "ops": {
            "bamboohr_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=greenhouse_partitions_def.get_partition_keys())
def greenhouse_partitioned_config(partition_key: str):
    return {
        "ops": {
            "greenhouse_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=workday_partitions_def.get_partition_keys())
def workday_partitioned_config(partition_key: str):
    return {
        "ops": {
            "workday_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=smartrecruiters_partitions_def.get_partition_keys())
def smartrecruiters_partitioned_config(partition_key: str):
    return {
        "ops": {
            "smartrecruiters_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

@static_partitioned_config(partition_keys=icims_partitions_def.get_partition_keys())
def icims_partitioned_config(partition_key: str):
    return {
        "ops": {
            "icims_company_jobs_discovery": {
                "config": {
                    # The asset already gets partition_key from context
                    # No need to pass it in config
                }
            }
        }
    }

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

# Define jobs for job discovery by platform
bamboohr_jobs_discovery_job = define_asset_job(
    name="bamboohr_jobs_discovery_job",
    selection=AssetSelection.assets("bamboohr_company_jobs_discovery"),
    description="Job that discovers and collects job listings from BambooHR career sites",
    partitions_def=bamboo_partitions_def,
    config=bamboohr_partitioned_config
)

greenhouse_jobs_discovery_job = define_asset_job(
    name="greenhouse_jobs_discovery_job",
    selection=AssetSelection.assets("greenhouse_company_jobs_discovery"),
    description="Job that discovers and collects job listings from Greenhouse career sites",
    partitions_def=greenhouse_partitions_def,
    config=greenhouse_partitioned_config
)

smartrecruiters_jobs_discovery_job = define_asset_job(
    name="smartrecruiters_jobs_discovery_job",
    selection=AssetSelection.assets("smartrecruiters_company_jobs_discovery"),
    description="Job that discovers and collects job listings from SmartRecruiters career sites",
    partitions_def=smartrecruiters_partitions_def,
    config=smartrecruiters_partitioned_config
)

workday_jobs_discovery_job = define_asset_job(
    name="workday_jobs_discovery_job",
    selection=AssetSelection.assets("workday_company_jobs_discovery"),
    description="Job that discovers and collects job listings from Workday career sites",
    partitions_def=workday_partitions_def,
    config=workday_partitioned_config
)

icims_jobs_discovery_job = define_asset_job(
    name="icims_jobs_discovery_job",
    selection=AssetSelection.assets("icims_company_jobs_discovery"),
    description="Job that discovers and collects job listings from ICIMS career sites",
    partitions_def=icims_partitions_def,
    config=icims_partitioned_config
)

# For Supabase transport assets
supabase_transport_job = define_asset_job(
    name ="supabase_transport_job",
    selection=AssetSelection.assets("bamboohr_jobs_to_supabase", "greenhouse_jobs_to_supabase", "workday_jobs_to_supabase", "smartrecruiters_jobs_to_supabase"),
    description="Job that transports job info from bigquery data to Supabase"
)



# Define a job for all job discovery across platforms
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_partitioned_config(partition_key: str):
    return {
        "ops": {
            "bamboohr_company_jobs_discovery": {"config": {}},
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}},
            "icims_company_jobs_discovery": {"config": {}}
        }
    }

full_jobs_discovery_job = define_asset_job(
    name="full_jobs_discovery_job",
    selection=AssetSelection.groups("job_discovery"),
    description="Job that discovers and collects job listings from all supported platforms",
    partitions_def=alpha_partitions,
    config=full_jobs_partitioned_config
)

# For all job discovery except ICIMS
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_except_icims_partitioned_config(partition_key: str):
    return {
        "ops": {
            "bamboohr_company_jobs_discovery": {"config": {}},
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}}
        }
    }

full_jobs_discovery_except_icims_job = define_asset_job(
    name="full_jobs_discovery_except_icims_job",
    selection=AssetSelection.assets(
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery",
        "bamboohr_company_jobs_discovery",
    ),
    partitions_def=alpha_partitions,
    config=full_jobs_except_icims_partitioned_config
)

# Define a job for data/database engineering position search
data_engineering_job = define_asset_job(
    name="data_engineering_job",
    selection=AssetSelection.assets("search_jobs"),
    description="Job that searches for SQL Developer, Database Developer, and Data Engineer positions in NY, NJ, or remote",
    config=RunConfig(
        ops={
            "search_jobs": {
                "config": {
                    "keywords": ["SQL", "database", "ETL", "pipeline", "data engineer"],
                    "job_titles": ["SQL", "Database", "Data", "Software", "BI ", "Developer", "Engineer", "Analyst"],
                    "excluded_keywords": ["overseas only", "non-US", "offshore"],
                    "locations": ["New York", "New Jersey", "NY", "NJ", "Location", ""],
                    "remote": True,
                    "days_back": 10,
                    "max_results": 500,
                    "min_match_score": 0.1,
                    "platforms": ["greenhouse", "bamboohr", "smartrecruiters", "workday"],
                    "output_format": "html",
                    "output_file": os.path.join(os.getenv("JOB_SEARCH_OUTPUT_FOLDER", "output"), "data_engineering_jobs_{date}.html"),
                    "include_descriptions": True
                }
            }
        }
    )
)

# Define a job for all job discovery across platforms (except iCIMS) plus job search
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def full_jobs_discovery_and_search_partitioned_config(partition_key: str):
    return {
        "ops": {
            # "bamboohr_company_jobs_discovery": {"config": {}},
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}},
            "search_jobs": {
                "config": {
                    "keywords": ["SQL", "database", "ETL", "pipeline", "data engineer"],
                    "job_titles": ["SQL", "Database", "Data", "Software", "BI ", "Developer", "Engineer", "Analyst"],
                    "excluded_keywords": ["overseas only", "non-US", "offshore"],
                    "locations": ["New York", "New Jersey", "NY", "NJ", "Location", ""],
                    "remote": True,
                    "days_back": 5,
                    "max_results": 500,
                    "min_match_score": 0.1,
                    "platforms": ["greenhouse", "workday", "bamboohr", "smartrecruiters"],
                    "output_format": "html",
                    "output_file": os.path.join(os.getenv("JOB_SEARCH_OUTPUT_FOLDER", "output"), "data_engineering_jobs_{date}.html"),
                    "include_descriptions": True
                }
            }
        }
    }

full_jobs_discovery_and_search_job = define_asset_job(
    name="full_jobs_discovery_and_search_job",
    selection=[
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery",
        "bamboohr_company_jobs_discovery",
        "search_jobs"
    ],
    description="Job that discovers and collects job listings from all supported platforms plus job search",
    partitions_def=alpha_partitions,
    config=full_jobs_discovery_and_search_partitioned_config
)

# Define a job that combines job discovery and Supabase transport
@static_partitioned_config(partition_keys=alpha_partitions.get_partition_keys())
def discovery_and_transport_partitioned_config(partition_key: str):
    return {
        "ops": {
            "bamboohr_company_jobs_discovery": {"config": {}},
            "greenhouse_company_jobs_discovery": {"config": {}},
            "workday_company_jobs_discovery": {"config": {}},
            "smartrecruiters_company_jobs_discovery": {"config": {}}
        }
    }

discovery_and_transport_job = define_asset_job(
    name="discovery_and_transport_job",
    selection=AssetSelection.assets(
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery",
        "bamboohr_company_jobs_discovery",
        "bamboohr_jobs_to_supabase",
        "greenhouse_jobs_to_supabase",
        "workday_jobs_to_supabase",
        "smartrecruiters_jobs_to_supabase"
    ),
    description="Job that discovers jobs (except iCIMS) and transports them to Supabase",
    partitions_def=alpha_partitions,
    config=discovery_and_transport_partitioned_config
)



