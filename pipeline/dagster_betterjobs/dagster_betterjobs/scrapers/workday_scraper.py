import re
import json
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time
from bs4 import BeautifulSoup

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class WorkdayScraper(BaseScraper):
    """
    Scraper for Workday-based career sites.

    This scraper handles the specific patterns and APIs used by Workday
    career portals to extract job listings and details.
    """

    def __init__(self, career_url: str, **kwargs):
        """
        Initialize the Workday scraper.

        Args:
            career_url: Base URL of the Workday career site
            **kwargs: Additional arguments to pass to BaseScraper
        """
        super().__init__(**kwargs)
        self.career_url = self.normalize_url(career_url)
        self.domain = self.get_domain(self.career_url)

    def _get_site_id(self) -> Optional[str]:
        """Extract the Workday site ID from the career page."""
        try:
            response = self.make_request(self.career_url)

            # Look for the site ID in the HTML
            match = re.search(r'tenantId=([^&"\']+)', response.text)
            if match:
                return match.group(1)

            # Try to find it in a different format
            match = re.search(r'"siteId":\s*"([^"]+)"', response.text)
            if match:
                return match.group(1)

            # Look for it in script tags
            soup = BeautifulSoup(response.text, 'html.parser')
            scripts = soup.find_all('script')

            for script in scripts:
                if script.string:
                    match = re.search(r'tenantId\s*=\s*[\'"]([^\'"]+)[\'"]', script.string)
                    if match:
                        return match.group(1)

            return None

        except Exception as e:
            logging.error(f"Error extracting site ID: {str(e)}")
            return None

    def _build_search_url(self, keyword: str, location: Optional[str] = None) -> str:
        """Build the API URL for job searching based on site analysis."""
        # Parse the base career URL
        parsed_url = urlparse(self.career_url)

        # Determine if this is a standard Workday URL format
        if 'myworkdayjobs.com' in parsed_url.netloc:
            base_path = '/'.join(parsed_url.path.strip('/').split('/')[:2])
            search_path = f"{base_path}/jobs/search"

            query_params = {
                'locations': location,
                'q': keyword
            }
            # Filter out None values
            query_params = {k: v for k, v in query_params.items() if v is not None}

            # Rebuild the URL
            search_url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                search_path,
                '',
                urlencode(query_params),
                ''
            ))
            return search_url
        else:
            # For custom Workday implementations, fall back to the base URL
            return self.career_url

    def search_jobs(self, keyword: str, location: Optional[str] = None) -> List[Dict]:
        """
        Search for jobs on a Workday career site.

        Args:
            keyword: Search term (e.g., "SQL Developer")
            location: Optional location filter

        Returns:
            List of job dictionaries with basic information
        """
        search_url = self._build_search_url(keyword, location)
        jobs = []

        try:
            response = self.make_request(search_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find job listings - Workday typically uses these patterns
            job_elements = soup.select('.WDVH-job-card, .WL5F-job-card, .job-listing-card, [data-automation-id="jobCard"]')

            if not job_elements:
                # Try alternative selectors
                job_elements = soup.select('.job-card, .jobCard, .WLJD-job-row')

            for job_element in job_elements:
                job = {}

                # Extract job title
                title_elem = job_element.select_one('.job-title, .jobTitle, [data-automation-id="jobTitle"]')
                if title_elem:
                    job['job_title'] = title_elem.text.strip()
                else:
                    # Try alternate patterns
                    title_elem = job_element.select_one('a[data-automation-id]')
                    if title_elem:
                        job['job_title'] = title_elem.text.strip()

                # Extract job link
                link_elem = job_element.select_one('a[href*="job_apply"]')
                if link_elem and 'href' in link_elem.attrs:
                    job_url = link_elem['href']
                    # Handle relative URLs
                    if job_url.startswith('/'):
                        parsed = urlparse(self.career_url)
                        job_url = f"{parsed.scheme}://{parsed.netloc}{job_url}"
                    job['job_url'] = job_url

                # Extract location
                location_elem = job_element.select_one('.job-location, .jobLocation, [data-automation-id="jobLocation"]')
                if location_elem:
                    job['location'] = location_elem.text.strip()

                # Only add job with required fields
                if 'job_title' in job and 'job_url' in job:
                    jobs.append(job)

            return jobs

        except Exception as e:
            logging.error(f"Error searching jobs: {str(e)}")
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
            'job_url': job_url
        }

        try:
            response = self.make_request(job_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract job title
            title_elem = soup.select_one('[data-automation-id="jobTitle"]')
            if title_elem:
                job_details['job_title'] = title_elem.text.strip()

            # Extract job description
            desc_elem = soup.select_one('[data-automation-id="jobDescription"]')
            if desc_elem:
                job_details['job_description'] = desc_elem.get_text(separator='\n').strip()

            # Extract location
            location_elem = soup.select_one('[data-automation-id="jobLocation"]')
            if location_elem:
                job_details['location'] = location_elem.text.strip()

            # Extract posted date
            date_elem = soup.select_one('[data-automation-id="jobPostingDate"]')
            if date_elem:
                job_details['date_posted'] = date_elem.text.strip()

            return job_details

        except Exception as e:
            logging.error(f"Error getting job details: {str(e)}")
            return job_details