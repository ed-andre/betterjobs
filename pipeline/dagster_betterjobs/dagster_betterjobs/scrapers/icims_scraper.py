import re
import json
import logging
import requests
from typing import Dict, List, Optional
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class ICIMSScraper(BaseScraper):
    """
    Scraper for iCIMS-based career sites.

    This scraper handles the specific patterns and APIs used by iCIMS
    career portals to extract job listings and details.
    """

    def __init__(self, career_url: str, dagster_log=None, ats_url=None, **kwargs):
        """
        Initialize the iCIMS scraper.

        Args:
            career_url: Base URL of the iCIMS career site
            dagster_log: Optional Dagster logger for integration
            ats_url: Optional direct ATS URL (careers-[tenant].icims.com)
            **kwargs: Additional arguments to pass to BaseScraper
        """
        super().__init__(**kwargs)
        self.dagster_log = dagster_log
        self.original_url = career_url

        # Use direct ATS URL if provided
        if ats_url and 'icims.com' in ats_url:
            self.log_message("info", f"Using provided ATS URL: {ats_url}")
            self.career_url = ats_url
        else:
            self.career_url = self.normalize_url(career_url)

        # Initialize session with iCIMS-specific headers
        self._init_session()

    def _init_session(self):
        """Initialize session with iCIMS-specific headers."""
        self.session = requests.Session()

        # Set headers that work well with iCIMS
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "DNT": "1",
            "Sec-Fetch-Dest": "iframe",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1"
        })

        # Set required cookies
        self.session.cookies.set("i18next", "en")
        self.session.cookies.set("icimsCookiesEnabledCheck", "1")

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

    def _extract_job_impressions(self, html_content: str) -> List[Dict]:
        """Extract job listings from the jobImpressions variable in the HTML."""
        try:
            # Look for the jobImpressions variable
            match = re.search(r'var\s+jobImpressions\s*=\s*(\[.*?\]);', html_content, re.DOTALL)
            if match:
                json_str = match.group(1)
                return json.loads(json_str)
        except Exception as e:
            self.log_message("error", f"Error extracting job impressions: {str(e)}")
        return []

    def search_jobs(self, keyword: str = None, location: Optional[str] = None) -> List[Dict]:
        """
        Search for jobs on an iCIMS career site.

        Args:
            keyword: Optional search term filter
            location: Optional location filter

        Returns:
            List of job dictionaries with basic information
        """
        jobs = []

        try:
            # Construct the search URL
            search_url = f"{self.career_url.rstrip('/')}/jobs/search?in_iframe=1"
            self.log_message("info", f"Requesting job listings from: {search_url}")

            # Make request to the search page
            response = self.make_request(
                url=search_url,
                method="GET",
                allow_redirects=True
            )

            if not response:
                self.log_message("error", f"Failed to get response from {search_url}")
                return jobs

            # Extract job listings from the jobImpressions variable
            job_listings = self._extract_job_impressions(response.text)
            self.log_message("info", f"Found {len(job_listings)} job listings")

            # Process each job listing
            for job in job_listings:
                try:
                    # Extract basic job info
                    job_id = str(job.get("idRaw"))
                    job_title = job.get("title")
                    position_type = job.get("positionType")
                    category = job.get("category")
                    posted_date = job.get("postedDate")

                    # Extract location info
                    location_data = job.get("location", {})
                    location_str = f"{location_data.get('city', '')}, {location_data.get('state', '')} {location_data.get('zip', '')}"
                    location_str = location_str.strip(", ")

                    # Construct job URL
                    job_url = f"{self.career_url.rstrip('/')}/jobs/{job_id}/{job_title.lower().replace(' ', '-')}/job?in_iframe=1"

                    # Create job record
                    job_record = {
                        "job_id": job_id,
                        "job_title": job_title,
                        "job_url": job_url,
                        "location": location_str,
                        "position_type": position_type,
                        "category": category,
                        "posted_date": posted_date,
                        "raw_data": json.dumps(job)
                    }

                    # Apply filters if specified
                    if keyword and job_title and keyword.lower() not in job_title.lower():
                        continue

                    if location and location_str and location.lower() not in location_str.lower():
                        continue

                    jobs.append(job_record)

                except Exception as e:
                    self.log_message("warning", f"Error processing job listing: {str(e)}")
                    continue

            return jobs

        except Exception as e:
            self.log_message("error", f"Error searching jobs: {str(e)}")
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
            "job_url": job_url
        }

        try:
            # Make request to the job detail page
            response = self.make_request(job_url)

            if not response:
                self.log_message("error", f"Failed to get response from {job_url}")
                return job_details

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract job ID from URL
            job_id_match = re.search(r'/jobs/(\d+)/', job_url)
            if job_id_match:
                job_details["job_id"] = job_id_match.group(1)

            # Extract job title from meta tags
            title_meta = soup.find('meta', property='og:title')
            if title_meta:
                job_details["job_title"] = title_meta.get('content', '').split('|')[0].strip()

            # Extract job description from meta tags
            desc_meta = soup.find('meta', property='og:description')
            if desc_meta:
                job_details["job_description"] = desc_meta.get('content', '')

            # Extract location from meta tags or content
            location_meta = soup.find('meta', property='og:location')
            if location_meta:
                job_details["location"] = location_meta.get('content', '')

            # Extract additional fields from the page content
            # This will depend on the specific structure of the job detail page

            # Store the raw HTML for reference
            job_details["raw_data"] = response.text

            return job_details

        except Exception as e:
            self.log_message("error", f"Error getting job details: {str(e)}")
            return job_details