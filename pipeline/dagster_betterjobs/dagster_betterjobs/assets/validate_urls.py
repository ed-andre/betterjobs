from typing import Dict, List
import requests
from urllib.parse import urlparse

from dagster import AssetExecutionContext, get_dagster_logger


logger = get_dagster_logger()

# Set to True to enable verbose debug logging
DEBUG_ENABLED = False

def validate_urls(context: AssetExecutionContext, companies: List[Dict]) -> List[Dict]:
    """
    Validate URLs by checking if they return a successful response.

    Detects cases where URLs redirect to generic ATS platform pages rather than
    actual company job boards.

    Args:
        context: Dagster execution context
        companies: List of company dictionaries with URLs

    Returns:
        List of companies with validated URLs and url_verified flag
    """
    validated_companies = []
    timeout = 5  # Short timeout to avoid long waits
    total_companies = len(companies)

    context.log.info(f"Starting URL validation for {total_companies} companies")

    for idx, company in enumerate(companies):
        company_copy = company.copy()
        company_copy["url_verified"] = False
        company_name = company.get("company_name", "Unknown")

        # Log progress every 10 companies or at beginning/end
        if idx % 10 == 0 or idx == total_companies - 1:
            context.log.info(f"Validating {idx+1}/{total_companies} - Current: {company_name}")

        # Try to validate ATS URL
        ats_url = company.get("ats_url")
        ats_platform = company.get("platform", "").lower()

        if DEBUG_ENABLED:
            context.log.debug(f"Validating company: {company_name}, Platform: {ats_platform}, ATS URL: {ats_url}")

        # Flag to track if we've confirmed the ATS platform
        ats_platform_confirmed = False

        if ats_url:
            try:
                # Check URL format first
                parsed_url = urlparse(ats_url)
                if not (parsed_url.scheme and parsed_url.netloc):
                    context.log.warning(f"Invalid ATS URL format for {company_name}: {ats_url}")
                    company_copy["ats_url"] = None
                else:
                    # Make a GET request to check if URL exists and to follow redirects
                    if DEBUG_ENABLED:
                        context.log.debug(f"Sending GET request to {ats_url} for {company_name}")
                    response = requests.get(ats_url, timeout=timeout, allow_redirects=True)

                    # Check for redirect to generic ATS platform pages
                    final_url = response.url
                    if DEBUG_ENABLED:
                        context.log.debug(f"Final URL after redirection for {company_name}: {final_url}")
                    is_generic_redirect = False

                    # Debug log for BambooHR detection
                    if "bamboohr.com" in ats_url.lower() and DEBUG_ENABLED:
                        context.log.debug(f"URL contains bamboohr.com for {company_name} but platform is {ats_platform}")

                    # Handle BambooHR redirects
                    if ats_platform == "bamboohr" and "bamboohr.com" in ats_url.lower():
                        if DEBUG_ENABLED:
                            context.log.debug(f"Detected BambooHR URL for {company_name}")
                        # Detect if redirected to the main BambooHR site or any generic page
                        if "www.bamboohr.com" in final_url.lower() or final_url.lower() == "https://bamboohr.com/":
                            context.log.warning(
                                f"BambooHR URL for {company_name} redirects to generic BambooHR page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None
                        # Check if netloc has changed - could be redirect to a different site
                        elif parsed_url.netloc != urlparse(final_url).netloc:
                            context.log.warning(
                                f"BambooHR URL for {company_name} redirects to different domain: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None
                        # Check if the URL contains specific BambooHR career paths
                        elif "/careers" not in final_url.lower():
                            context.log.warning(
                                f"BambooHR URL for {company_name} doesn't lead to a careers page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # Special case for BambooHR URLs regardless of specified platform
                    elif "bamboohr.com" in ats_url.lower():
                        if DEBUG_ENABLED:
                            context.log.debug(f"Found bamboohr.com URL but platform is {ats_platform} for {company_name}")
                        # Check for redirects to generic BambooHR site
                        if "www.bamboohr.com" in final_url.lower() or final_url.lower() == "https://bamboohr.com/":
                            context.log.warning(
                                f"BambooHR URL in non-BambooHR company {company_name} redirects to generic page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None
                        # Check if netloc has changed for BambooHR URLs
                        elif parsed_url.netloc != urlparse(final_url).netloc:
                            context.log.warning(
                                f"BambooHR URL in non-BambooHR company {company_name} redirects to different domain: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # Handle Workday redirects
                    elif ats_platform == "workday" and "myworkdayjobs.com" in ats_url.lower():
                        # If redirected to a login page or generic Workday page
                        if ("login" in final_url.lower() or
                            "myworkday.com" in final_url.lower() and "recruiting" not in final_url.lower()):
                            context.log.warning(
                                f"Workday URL for {company_name} redirects to generic page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # Handle Lever redirects
                    elif ats_platform == "lever" and "lever.co" in ats_url.lower():
                        # If redirected to the main Lever site
                        if final_url.lower() == "https://www.lever.co/" or final_url.lower() == "https://lever.co/":
                            context.log.warning(
                                f"Lever URL for {company_name} redirects to generic page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # Handle Greenhouse redirects
                    elif ats_platform == "greenhouse" and "greenhouse.io" in ats_url.lower():
                        # If redirected to the main Greenhouse site or a generic page
                        if final_url.lower() in ["https://www.greenhouse.io/", "https://greenhouse.io/",
                                               "https://boards.greenhouse.io/", "https://job-boards.greenhouse.io/"]:
                            context.log.warning(
                                f"Greenhouse URL for {company_name} redirects to generic page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None
                        # Check for redirects to error pages
                        elif "error" in final_url.lower() or "404" in final_url.lower():
                            context.log.warning(
                                f"Greenhouse URL for {company_name} redirects to error page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # Handle Jobvite redirects
                    elif ats_platform == "jobvite" and "jobvite" in ats_url.lower():
                        # If redirected to the main Jobvite site or support pages
                        if (
                            final_url.lower() in [
                                "https://www.jobvite.com/",
                                "https://jobvite.com/",
                                "https://www.jobvite.com/support/",
                                "https://app.jobvite.com/admin/info/404.html",
                                "https://app.jobvite.com/"
                            ]
                            or "jobs.jobvite.com/error" in final_url.lower()
                            or "/support/" in final_url.lower()
                            or "/job-seeker-support" in final_url.lower()
                            or "/admin/info/404.html" in final_url.lower()
                        ):
                            context.log.warning(
                                f"Jobvite URL for {company_name} redirects to generic or support page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # Handle SmartRecruiters redirects
                    elif ats_platform == "smartrecruiters" and "smartrecruiters" in ats_url.lower():
                        # If redirected to the main SmartRecruiters site or a login page
                        if final_url.lower() in [
                            "https://www.smartrecruiters.com/",
                            "https://smartrecruiters.com/",
                            "https://jobs.smartrecruiters.com/"
                            ] or "login" in final_url.lower():
                            context.log.warning(
                                f"SmartRecruiters URL for {company_name} redirects to generic page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None
                        # Check if redirected to the SmartRecruiters jobs home without a specific company
                        elif final_url.lower() in ["https://jobs.smartrecruiters.com/", "https://careers.smartrecruiters.com/"]:
                            context.log.warning(
                                f"SmartRecruiters URL for {company_name} redirects to jobs homepage: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # Handle iCIMS redirects
                    elif ats_platform == "icims" and "icims.com" in ats_url.lower():
                        # If redirected to the iCIMS login page
                        if "login.icims.com" in final_url.lower():
                            context.log.warning(
                                f"iCIMS URL for {company_name} redirects to login page: {ats_url} → {final_url}"
                            )
                            is_generic_redirect = True
                            company_copy["ats_url"] = None

                    # General redirect check for significant domain changes
                    elif final_url != ats_url and urlparse(ats_url).netloc != urlparse(final_url).netloc:
                        # Check if we've been redirected to a completely different domain
                        original_domain = urlparse(ats_url).netloc
                        final_domain = urlparse(final_url).netloc

                        # If domain completely changed, log it
                        if original_domain not in final_domain and final_domain not in original_domain:
                            context.log.warning(
                                f"URL for {company_name} redirects to different domain: {ats_url} → {final_url}"
                            )
                            # Check if redirection is to www.bamboohr.com
                            if "www.bamboohr.com" in final_domain:
                                context.log.warning(f"Detected redirection to generic BambooHR page: {final_url}")
                                is_generic_redirect = True
                                company_copy["ats_url"] = None

                    if DEBUG_ENABLED:
                        context.log.debug(f"Before final validation - is_generic_redirect: {is_generic_redirect}, ats_url: {company_copy.get('ats_url')}")

                    if response.status_code < 400 and not is_generic_redirect:
                        # Only log successful verifications at info level
                        context.log.info(f"✓ Verified ATS URL for {company_name}")
                        company_copy["url_verified"] = True
                        ats_platform_confirmed = True

                        # If we detect a valid BambooHR URL but platform is different, update the platform
                        if "bamboohr.com" in ats_url.lower() and ats_platform != "bamboohr":
                            context.log.info(f"Updating platform from {ats_platform} to bamboohr for {company_name}")
                            company_copy["platform"] = "bamboohr"
                        # Handle other platform mismatches similarly
                        elif "lever.co" in ats_url.lower() and ats_platform != "lever":
                            context.log.info(f"Updating platform from {ats_platform} to lever for {company_name}")
                            company_copy["platform"] = "lever"
                        elif "myworkdayjobs.com" in ats_url.lower() and ats_platform != "workday":
                            context.log.info(f"Updating platform from {ats_platform} to workday for {company_name}")
                            company_copy["platform"] = "workday"
                        elif "greenhouse.io" in ats_url.lower() and ats_platform != "greenhouse":
                            context.log.info(f"Updating platform from {ats_platform} to greenhouse for {company_name}")
                            company_copy["platform"] = "greenhouse"
                        elif "jobvite.com" in ats_url.lower() and ats_platform != "jobvite":
                            context.log.info(f"Updating platform from {ats_platform} to jobvite for {company_name}")
                            company_copy["platform"] = "jobvite"
                        elif "smartrecruiters.com" in ats_url.lower() and ats_platform != "smartrecruiters":
                            context.log.info(f"Updating platform from {ats_platform} to smartrecruiters for {company_name}")
                            company_copy["platform"] = "smartrecruiters"
                        elif "icims.com" in ats_url.lower() and ats_platform != "icims":
                            context.log.info(f"Updating platform from {ats_platform} to icims for {company_name}")
                            company_copy["platform"] = "icims"
                    else:
                        if response.status_code >= 400:
                            context.log.warning(f"Invalid ATS URL for {company_name}: {ats_url} (Status: {response.status_code})")
                        if company_copy["ats_url"] is not None:
                            company_copy["ats_url"] = None
            except Exception as e:
                context.log.warning(f"Error validating ATS URL for {company_name}: {str(e)}")
                company_copy["ats_url"] = None

        # If ATS URL is invalid or not provided, clear the platform field
        if not ats_platform_confirmed and not company_copy.get("ats_url"):
            company_copy["platform"] = ""
            if DEBUG_ENABLED:
                context.log.debug(f"Clearing platform for {company_name} as no valid ATS URL was found")

        # If ATS URL is invalid, try the career URL
        if DEBUG_ENABLED:
            context.log.debug(f"After ATS validation - url_verified: {company_copy['url_verified']}, ats_url: {company_copy.get('ats_url')}")

        if not company_copy["url_verified"] and company.get("career_url"):
            career_url = company.get("career_url")
            try:
                # Check URL format first
                parsed_url = urlparse(career_url)
                if not (parsed_url.scheme and parsed_url.netloc):
                    context.log.warning(f"Invalid career URL format for {company_name}: {career_url}")
                    company_copy["career_url"] = None
                else:
                    # Make a request to check if URL exists and capture any redirects
                    if DEBUG_ENABLED:
                        context.log.debug(f"Sending GET request to career URL {career_url} for {company_name}")
                    response = requests.get(career_url, timeout=timeout, allow_redirects=True)

                    # Check for extreme redirects (might indicate a non-working careers page)
                    final_url = response.url
                    if DEBUG_ENABLED:
                        context.log.debug(f"Career URL final redirect: {final_url} for {company_name}")
                    original_domain = urlparse(career_url).netloc
                    final_domain = urlparse(final_url).netloc

                    # If domain completely changed to something unrelated
                    is_invalid_redirect = False
                    if original_domain != final_domain and original_domain not in final_domain:
                        # Check if it redirected to a generic service or login page
                        generic_domains = ["login.", "accounts.", "authorize.", "auth."]

                        if any(term in final_domain.lower() for term in generic_domains):
                            context.log.warning(
                                f"Career URL for {company_name} redirects to auth page: {career_url} → {final_url}"
                            )
                            is_invalid_redirect = True

                    if response.status_code < 400 and not is_invalid_redirect:
                        context.log.info(f"✓ Verified career URL for {company_name}")
                        company_copy["url_verified"] = True
                    else:
                        if response.status_code >= 400:
                            context.log.warning(f"Invalid career URL for {company_name}: {career_url} (Status: {response.status_code})")
                        company_copy["career_url"] = None
            except Exception as e:
                context.log.warning(f"Error validating career URL for {company_name}: {str(e)}")
                company_copy["career_url"] = None

        if DEBUG_ENABLED:
            context.log.debug(f"Final validation result for {company_name} - url_verified: {company_copy['url_verified']}, ats_url: {company_copy.get('ats_url')}, career_url: {company_copy.get('career_url')}")
        validated_companies.append(company_copy)

    # Log final summary
    verified_count = sum(1 for c in validated_companies if c.get("url_verified", False))
    context.log.info(f"URL validation complete: {verified_count}/{len(validated_companies)} URLs verified successfully")

    return validated_companies