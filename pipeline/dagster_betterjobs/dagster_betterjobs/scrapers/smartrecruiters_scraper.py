import json
import logging
import datetime
import requests
import time
import re
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup
from datetime import timezone
from urllib.parse import urlparse

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class SmartRecruitersJobScraper(BaseScraper):
    """
    Scraper for SmartRecruiters-based career sites.

    Extracts job listings from SmartRecruiters job boards using BeautifulSoup.
    """

    def __init__(self, career_url: str, dagster_log=None, cutoff_date=None, **kwargs):
        """
        Initialize the SmartRecruiters scraper.

        Args:
            career_url: Base URL of the SmartRecruiters career site
            dagster_log: Optional Dagster logger for integration
            cutoff_date: Optional datetime for job freshness filter
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(**kwargs)

        self.dagster_log = dagster_log
        self.original_url = career_url
        self.career_url = career_url

        # Set cutoff date (default: 14 days ago) and ensure it's timezone-aware
        if cutoff_date:
            # If cutoff_date is already timezone-aware, use it as is
            if cutoff_date.tzinfo is not None:
                self.cutoff_date = cutoff_date
            else:
                # Otherwise, make it timezone-aware with UTC
                self.cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
        else:
            # Default cutoff date - 14 days ago with UTC timezone
            self.cutoff_date = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=14)

        self.log_message("info", f"Setting cutoff date for jobs to {self.cutoff_date.strftime('%Y-%m-%d')}")

        self._init_session()

    def _init_session(self):
        """Initialize session with SmartRecruiters-specific headers."""
        self.session = requests.Session()

        # Set headers that work well with SmartRecruiters
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })

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

    def make_smartrecruiters_request(self, url: str, method: str = "GET", **kwargs) -> requests.Response:
        """
        Make a request to SmartRecruiters with specialized handling and retries.
        """
        try:
            # Apply rate limiting
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)

            kwargs.setdefault("timeout", self.timeout)

            # Set required headers for SmartRecruiters
            if "headers" not in kwargs:
                kwargs["headers"] = {}

            smartrecruiters_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": self.career_url
            }
            kwargs["headers"].update(smartrecruiters_headers)

            # Implement retry logic
            for attempt in range(self.max_retries):
                try:
                    self.last_request_time = time.time()
                    response = self.session.request(method, url, **kwargs)

                    self.log_message("info", f"Request to {url} returned status {response.status_code}")
                    if response.status_code != 200:
                        self.log_message("warning", f"Non-200 response: {response.status_code} - {response.text[:200]}")

                    response.raise_for_status()
                    return response

                except requests.RequestException as e:
                    self.log_message("warning", f"Request failed (attempt {attempt+1}/{self.max_retries}): {str(e)}")

                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    else:
                        raise

        except Exception as e:
            self.log_message("error", f"Error making SmartRecruiters request to {url}: {str(e)}")
            raise

        return None

    def search_jobs(self, keyword: str = None, location: Optional[str] = None, max_age_days: int = None) -> List[Dict]:
        """
        Search for jobs on a SmartRecruiters career site.

        Args:
            keyword: Optional search term filter
            location: Optional location filter
            max_age_days: Optional max age of job postings in days

        Returns:
            List of job dictionaries with basic information
        """
        jobs = []

        try:
            self.log_message("info", f"Searching for jobs on {self.career_url}")

            # Make request to the career site URL
            response = self.make_smartrecruiters_request(self.career_url)

            if not response:
                self.log_message("error", f"Failed to get response from {self.career_url}")
                return jobs

            # Parse the HTML response
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all job listings - typically in elements with h4 tag containing job title
            job_elements = soup.select('h4.details-title.job-title.link--block-target')

            self.log_message("info", f"Found {len(job_elements)} job elements on the page")

            for job_element in job_elements:
                try:
                    # Get the parent <a> tag that contains the link to job details
                    parent_link = job_element.find_parent('a')

                    if not parent_link:
                        self.log_message("warning", "Could not find parent link for job element")
                        continue

                    # Get job title and job URL
                    job_title = job_element.text.strip()
                    job_url = parent_link.get('href')

                    # Use absolute URL if job_url is relative
                    if job_url and not job_url.startswith(('http://', 'https://')):
                        parsed_url = urlparse(self.career_url)
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        job_url = f"{base_url}{job_url}" if job_url.startswith('/') else f"{base_url}/{job_url}"

                    # Extract job ID from URL
                    job_id = None
                    if job_url:
                        # Try to extract ID from URL path segments
                        path_segments = urlparse(job_url).path.split('/')
                        for segment in path_segments:
                            # Look for numeric or alphanumeric segments that might be IDs
                            if re.match(r'\d+', segment) or re.match(r'^[a-zA-Z0-9-]+$', segment):
                                job_id = segment
                                break

                    if not job_id:
                        self.log_message("warning", f"Could not extract job ID from URL: {job_url}")
                        # Use a fallback ID based on title if no ID can be extracted
                        job_id = f"sr-{hash(job_title + job_url)}"

                    # Create basic job record
                    job = {
                        "job_id": str(job_id),
                        "job_title": job_title,
                        "job_url": job_url,
                        "is_recent": True,  # We'll determine this when we get job details
                    }

                    # Filter by keyword if specified
                    if keyword and job_title and keyword.lower() not in job_title.lower():
                        continue

                    # Get job details in the same search request to reduce API calls
                    job_details = self.get_job_details(job_url)
                    job.update(job_details)

                    # Filter by location if specified
                    if location and job.get("location") and location.lower() not in job["location"].lower():
                        continue

                    jobs.append(job)

                except Exception as e:
                    self.log_message("warning", f"Error processing job listing: {str(e)}")
                    continue

            self.log_message("info", f"Processed {len(jobs)} jobs after filtering")

        except Exception as e:
            self.log_message("error", f"Error searching SmartRecruiters jobs: {str(e)}")

        return jobs

    def get_job_details(self, job_url: str) -> Dict:
        """
        Get detailed information about a specific job.

        Args:
            job_url: URL of the job posting

        Returns:
            Dictionary containing job details
        """
        job_details = {
            'job_url': job_url
        }

        try:
            self.log_message("info", f"Getting details for job URL: {job_url}")

            # Make a request to the job detail page
            response = self.make_smartrecruiters_request(job_url)

            if not response:
                self.log_message("error", f"Failed to get response from {job_url}")
                return job_details

            # Parse the HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract job title if not already available
            if not job_details.get("job_title"):
                job_title_element = soup.select_one('h1.job-title')
                if job_title_element:
                    job_details["job_title"] = job_title_element.text.strip()

            # Extract job location
            location_element = soup.select_one('span[itemprop="address"]')
            if location_element:
                job_details["location"] = location_element.text.strip()

            # Extract job description
            job_description = ""
            description_sections = soup.select('div.wysiwyg')
            for section in description_sections:
                job_description += section.text.strip() + "\n\n"

            if job_description:
                job_details["job_description"] = job_description.strip()
                job_details["content"] = job_description.strip()  # For consistency with other scrapers

            # Extract posted date or updated date from meta tags
            meta_posted_date = soup.select_one('meta[itemprop="datePosted"]')
            if meta_posted_date:
                posted_date_str = meta_posted_date.get('content')
                if posted_date_str:
                    try:
                        posted_date = datetime.datetime.fromisoformat(posted_date_str.replace('Z', '+00:00'))
                        job_details["published_at"] = posted_date.isoformat()
                        # Set updated_at same as published_at if not available
                        job_details["updated_at"] = posted_date.isoformat()

                        # Check if the job is recent based on cutoff date
                        job_details["is_recent"] = posted_date >= self.cutoff_date
                    except (ValueError, TypeError) as e:
                        self.log_message("warning", f"Error parsing posted date '{posted_date_str}': {str(e)}")

            # Extract updated time if available
            meta_updated_time = soup.select_one('meta[itemprop="og:updated_time"]')
            if meta_updated_time:
                updated_time_str = meta_updated_time.get('content')
                if updated_time_str:
                    try:
                        updated_time = datetime.datetime.fromisoformat(updated_time_str.replace('Z', '+00:00'))
                        job_details["updated_at"] = updated_time.isoformat()
                    except (ValueError, TypeError) as e:
                        self.log_message("warning", f"Error parsing updated time '{updated_time_str}': {str(e)}")

            # Extract other metadata
            meta_requisition_id = soup.select_one('meta[name="sr:job-ad-id"]')
            if meta_requisition_id:
                job_details["requisition_id"] = meta_requisition_id.get('content')

            # Extract department/category if available
            department_element = soup.select_one('li.job-detail[itemprop="industry"]')
            if department_element:
                job_details["department"] = department_element.text.strip()

        except Exception as e:
            self.log_message("error", f"Error getting job details for {job_url}: {str(e)}")

        return job_details