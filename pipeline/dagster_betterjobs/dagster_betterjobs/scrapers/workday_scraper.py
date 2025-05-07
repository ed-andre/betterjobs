import re
import json
import logging
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class WorkdayScraper(BaseScraper):
    """
    Scraper for Workday-based career sites.

    This scraper handles the specific patterns and APIs used by Workday
    career portals to extract job listings and details.
    """

    def __init__(self, career_url: str, dagster_log=None, **kwargs):
        """
        Initialize the Workday scraper.

        Args:
            career_url: Base URL of the Workday career site
            dagster_log: Optional Dagster logger for integration
            **kwargs: Additional arguments to pass to BaseScraper
        """
        super().__init__(**kwargs)
        self.career_url = self.normalize_url(career_url)
        self.domain = self.get_domain(self.career_url)
        self.tenant_id = None
        self.site_id = None
        self.dagster_log = dagster_log
        self._extract_workday_ids()

    def log_message(self, level: str, message: str):
        """Log a message using the Dagster logger if available, or the standard logging module."""
        if self.dagster_log:
            if level == "info":
                self.dagster_log.info(message)
            elif level == "warning":
                self.dagster_log.warning(message)
            elif level == "error":
                self.dagster_log.error(message)
            elif level == "debug":
                self.dagster_log.debug(message)
        else:
            if level == "info":
                logging.info(message)
            elif level == "warning":
                logging.warning(message)
            elif level == "error":
                logging.error(message)
            elif level == "debug":
                logging.debug(message)

    def _extract_workday_ids(self) -> None:
        """Extract the Workday tenant ID and site ID from the career URL."""
        try:
            # Parse the career URL to extract tenant and site IDs
            parsed_url = urlparse(self.career_url)
            path_parts = parsed_url.path.strip('/').split('/')

            if 'myworkdayjobs.com' in parsed_url.netloc:
                # Extract tenant from subdomain (e.g., allianceground.wd1.myworkdayjobs.com)
                self.tenant_id = parsed_url.netloc.split('.')[0]

                # Extract site ID from path (e.g., /en-US/agi_careers)
                if len(path_parts) >= 2:
                    # The site ID is typically the last part of the path
                    self.site_id = path_parts[-1]

                self.log_message("info", f"Extracted tenant_id: {self.tenant_id}, site_id: {self.site_id}")
            else:
                self.log_message("warning", f"Not a standard Workday URL: {self.career_url}")
        except Exception as e:
            self.log_message("error", f"Error extracting Workday IDs: {str(e)}")

    def _build_jobs_api_url(self) -> str:
        """Build the API URL for job searching."""
        # The URL format should be: https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{tenant}/{site_id}/jobs
        if not self.tenant_id or not self.site_id:
            self.log_message("warning", "Tenant ID or Site ID not available")
            return None

        parsed_url = urlparse(self.career_url)
        api_url = f"{parsed_url.scheme}://{parsed_url.netloc}/wday/cxs/{self.tenant_id}/{self.site_id}/jobs"
        return api_url

    def _build_job_detail_url(self, external_path: str) -> str:
        """Build the job detail URL using the same base pattern as the API."""
        if not self.tenant_id or not self.site_id:
            self.log_message("warning", "Tenant ID or Site ID not available")
            return None

        parsed_url = urlparse(self.career_url)
        # The job URL should follow the same pattern as the API URL, but external_path already includes /job/
        return f"{parsed_url.scheme}://{parsed_url.netloc}/wday/cxs/{self.tenant_id}/{self.site_id}{external_path}"

    def search_jobs(self, keyword: str = "", location: str = "") -> List[Dict]:
        """
        Search for jobs on a Workday career site.

        Args:
            keyword: Optional search term
            location: Optional location filter

        Returns:
            List of job dictionaries with basic information
        """
        # First, get the initial page to obtain necessary cookies and tokens
        try:
            initial_response = self.make_request(
                url=self.career_url,
                method="GET",
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
                }
            )

            # Extract CSRF token from cookies or response
            csrf_token = None
            for cookie in initial_response.cookies:
                if cookie.name == "CALYPSO_CSRF_TOKEN":
                    csrf_token = cookie.value
                    break

            # Build the API URL
            api_url = self._build_jobs_api_url()
            if not api_url:
                self.log_message("error", f"Could not build API URL for {self.career_url}")
                return []

            self.log_message("info", f"Using Workday API URL: {api_url}")

            # Prepare the request payload - exactly match the format from cURL
            payload = {
                "appliedFacets": {},
                "limit": 20,
                "offset": 0,
                "searchText": ""
            }

            # Set up headers to match the successful cURL request
            headers = {
                "accept": "application/json",
                "accept-language": "en-US",
                "cache-control": "no-cache",
                "content-type": "application/json",
                "dnt": "1",
                "origin": self.career_url.rstrip('/'),
                "pragma": "no-cache",
                "referer": self.career_url,
                "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
            }

            # Add CSRF token if found
            if csrf_token:
                headers["x-calypso-csrf-token"] = csrf_token

            jobs = []

            try:
                # Make the API request
                response = self.make_request(
                    url=api_url,
                    method="POST",
                    headers=headers,
                    json=payload,
                    allow_redirects=True
                )

                # Parse the JSON response
                data = response.json()

                if "jobPostings" not in data:
                    self.log_message("warning", f"No jobPostings found in response: {data.keys()}")
                    return jobs

                job_postings = data.get("jobPostings", [])
                self.log_message("info", f"Found {len(job_postings)} job postings")

                # Process each job posting
                for job in job_postings:
                    job_title = job.get("title", "")
                    external_path = job.get("externalPath", "")
                    time_type = job.get("timeType", "")
                    locations_text = job.get("locationsText", "")
                    posted_on = job.get("postedOn", "")
                    bullet_fields = job.get("bulletFields", [])

                    # Extract job ID from bullet fields (usually the first one) or from externalPath
                    job_id = None
                    if bullet_fields and len(bullet_fields) > 0:
                        job_id = bullet_fields[0]
                    else:
                        # Try to extract job ID from the path
                        id_match = re.search(r'_([A-Z0-9\-]+)$', external_path)
                        if id_match:
                            job_id = id_match.group(1)

                    # Build the full job URL using the API base pattern
                    job_url = self._build_job_detail_url(external_path) if external_path else None

                    # Create job record
                    if job_title and job_url:
                        job_record = {
                            "job_id": job_id,
                            "job_title": job_title,
                            "job_url": job_url,
                            "location": locations_text,
                            "time_type": time_type,
                            "posted_on": posted_on,
                            "raw_data": json.dumps(job)
                        }
                        jobs.append(job_record)

                return jobs

            except Exception as e:
                self.log_message("error", f"Error searching jobs: {str(e)}")
                return []

        except Exception as e:
            self.log_message("error", f"Error in initial request: {str(e)}")
            return []

    def get_job_details(self, job_url: str) -> Dict:
        """
        Get detailed information about a specific job.

        Args:
            job_url: URL of the job posting

        Returns:
            Dictionary containing job details
        """
        job_details = {
            "job_url": job_url
        }

        try:
            # Set up headers for JSON response
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "dnt": "1",
                "pragma": "no-cache",
                "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "cross-site",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
            }

            # Fetch the job details
            response = self.make_request(job_url, headers=headers)

            # Parse the JSON response
            data = response.json()

            # Extract job posting info
            job_posting = data.get("jobPostingInfo", {})

            if job_posting:
                # Extract key details
                job_details["job_title"] = job_posting.get("title", "")
                job_details["job_description"] = job_posting.get("jobDescription", "")
                job_details["location"] = job_posting.get("location", "")
                job_details["time_type"] = job_posting.get("timeType", "")
                job_details["job_id"] = job_posting.get("jobReqId", "")
                job_details["date_posted"] = job_posting.get("startDate", "")
                job_details["valid_through"] = job_posting.get("endDate", "")

                # Extract organization info
                hiring_org = data.get("hiringOrganization", {})
                if hiring_org:
                    job_details["company_name"] = hiring_org.get("name", "")

                # Save the raw data
                job_details["raw_data"] = json.dumps(data)
            else:
                self.log_message("warning", f"No job posting info found in response for {job_url}")

            return job_details

        except Exception as e:
            self.log_message("error", f"Error getting job details: {str(e)}")
            return job_details