
from typing import Dict, List
import requests
from urllib.parse import urlparse

from dagster import AssetExecutionContext, get_dagster_logger


logger = get_dagster_logger()

def validate_urls(context: AssetExecutionContext, companies: List[Dict]) -> List[Dict]:
    """
    Validate URLs by checking if they return a successful response.

    Args:
        context: Dagster execution context
        companies: List of company dictionaries with URLs

    Returns:
        List of companies with validated URLs and url_verified flag
    """
    validated_companies = []
    timeout = 5  # Short timeout to avoid long waits

    for company in companies:
        company_copy = company.copy()
        company_copy["url_verified"] = False

        # Try to validate ATS URL
        ats_url = company.get("ats_url")
        if ats_url:
            try:
                # Check URL format first
                parsed_url = urlparse(ats_url)
                if not (parsed_url.scheme and parsed_url.netloc):
                    context.log.warning(f"Invalid ATS URL format for {company['company_name']}: {ats_url}")
                    company_copy["ats_url"] = None
                else:
                    # Make a HEAD request to check if URL exists
                    response = requests.head(ats_url, timeout=timeout, allow_redirects=True)

                    if response.status_code < 400:
                        context.log.info(f"Verified ATS URL for {company['company_name']}: {ats_url}")
                        company_copy["url_verified"] = True
                    else:
                        context.log.warning(f"Invalid ATS URL for {company['company_name']}: {ats_url} (Status: {response.status_code})")
                        company_copy["ats_url"] = None
            except Exception as e:
                context.log.warning(f"Error validating ATS URL for {company['company_name']}: {str(e)}")
                company_copy["ats_url"] = None

        # If ATS URL is invalid, try the career URL
        if not company_copy["ats_url"] and company.get("career_url"):
            career_url = company.get("career_url")
            try:
                # Check URL format first
                parsed_url = urlparse(career_url)
                if not (parsed_url.scheme and parsed_url.netloc):
                    context.log.warning(f"Invalid career URL format for {company['company_name']}: {career_url}")
                    company_copy["career_url"] = None
                else:
                    # Make a HEAD request to check if URL exists
                    response = requests.head(career_url, timeout=timeout, allow_redirects=True)

                    if response.status_code < 400:
                        context.log.info(f"Verified career URL for {company['company_name']}: {career_url}")
                        company_copy["url_verified"] = True
                    else:
                        context.log.warning(f"Invalid career URL for {company['company_name']}: {career_url} (Status: {response.status_code})")
                        company_copy["career_url"] = None
            except Exception as e:
                context.log.warning(f"Error validating career URL for {company['company_name']}: {str(e)}")
                company_copy["career_url"] = None

        validated_companies.append(company_copy)

    return validated_companies