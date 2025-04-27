import re
import json
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class GreenhouseScraper(BaseScraper):
    """
    Scraper for Greenhouse-based career sites.

    This scraper handles the specific patterns and APIs used by Greenhouse
    career portals to extract job listings and details.
    """

    def __init__(self, career_url: str, **kwargs):
        """
        Initialize the Greenhouse scraper.

        Args:
            career_url: Base URL of the Greenhouse career site
            **kwargs: Additional arguments to pass to BaseScraper
        """
        super().__init__(**kwargs)
        self.career_url = self.normalize_url(career_url)
        self.domain = self.get_domain(self.career_url)

        # Try to extract the company's Greenhouse board ID
        self.board_id = self._extract_board_id()

    def _extract_board_id(self) -> Optional[str]:
        """Extract the Greenhouse board ID from the career page."""
        try:
            response = self.make_request(self.career_url)

            # Look for board token in meta tags or script tags
            soup = BeautifulSoup(response.text, 'html.parser')

            # Try to find the board token in script tags
            scripts = soup.find_all('script')
            for script in scripts:
                script_text = script.string if script.string else ""
                if script_text:
                    # Look for the board token pattern
                    match = re.search(r'Greenhouse\.jobBoard\s*\(\s*\{\s*url\s*:\s*[\'"]https?://boards\.greenhouse\.io/embed/job_board/js\?for=([^\'"]+)[\'"]', script_text)
                    if match:
                        return match.group(1)

            # Try to find it in the URL
            parsed_url = urlparse(self.career_url)
            path_parts = parsed_url.path.strip('/').split('/')

            # Greenhouse typically uses the format: boards.greenhouse.io/companyname
            if 'boards.greenhouse.io' in parsed_url.netloc and len(path_parts) > 0:
                return path_parts[0]

            return None

        except Exception as e:
            logging.error(f"Error extracting Greenhouse board ID: {str(e)}")
            return None

    def search_jobs(self, keyword: str, location: Optional[str] = None) -> List[Dict]:
        """
        Search for jobs on a Greenhouse career site.

        Args:
            keyword: Search term (e.g., "SQL Developer")
            location: Optional location filter

        Returns:
            List of job dictionaries with basic information
        """
        jobs = []

        try:
            # Get all jobs first, then filter
            response = self.make_request(self.career_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find job listings
            job_elements = soup.select('.opening, .position, .job')

            if not job_elements:
                # Try alternative selectors
                job_elements = soup.select('.card-wrapper, .opening-job, .job-listing')

            if not job_elements:
                # Another common pattern
                job_elements = soup.select('div[data-id], div[id^="job-"]')

            # Process each job element
            for job_element in job_elements:
                job = {}

                # Extract title
                title_elem = job_element.select_one('a.heading, a.title, h3 a, h4 a, h5 a, .opening-title')
                if title_elem:
                    job_title = title_elem.text.strip()
                    job['job_title'] = job_title

                    # Also get the URL from this element
                    if 'href' in title_elem.attrs:
                        job_url = title_elem['href']
                        # Handle relative URLs
                        if job_url.startswith('/'):
                            job_url = urljoin(self.career_url, job_url)
                        job['job_url'] = job_url

                # If we didn't get a URL from the title element, try to find it elsewhere
                if 'job_url' not in job:
                    link_elem = job_element.select_one('a[href*="/jobs/"], a[href*="/job/"]')
                    if link_elem and 'href' in link_elem.attrs:
                        job_url = link_elem['href']
                        if job_url.startswith('/'):
                            job_url = urljoin(self.career_url, job_url)
                        job['job_url'] = job_url

                # Extract location
                location_elem = job_element.select_one('.location, .job-location, .opening-location')
                if location_elem:
                    job['location'] = location_elem.text.strip()

                # Filter jobs if keyword or location is specified
                if keyword or location:
                    matched = True

                    if keyword and 'job_title' in job:
                        matched = matched and keyword.lower() in job['job_title'].lower()

                    if location and 'location' in job:
                        matched = matched and location.lower() in job['location'].lower()

                    if not matched:
                        continue

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
            title_elem = soup.select_one('h1.app-title, h1.opening-title, h1.job-title')
            if title_elem:
                job_details['job_title'] = title_elem.text.strip()

            # Extract job description
            desc_elem = soup.select_one('#content, .job-description, .opening-description')
            if desc_elem:
                job_details['job_description'] = desc_elem.get_text(separator='\n').strip()

            # Extract location
            location_elem = soup.select_one('.location, .job-location, .opening-location')
            if location_elem:
                job_details['location'] = location_elem.text.strip()

            # Extract posted date - Greenhouse often has this in a script tag as JSON
            script_elems = soup.find_all('script', type='application/ld+json')
            for script in script_elems:
                try:
                    job_data = json.loads(script.string)
                    if 'datePosted' in job_data:
                        job_details['date_posted'] = job_data['datePosted']
                    break
                except (json.JSONDecodeError, AttributeError):
                    continue

            return job_details

        except Exception as e:
            logging.error(f"Error getting job details: {str(e)}")
            return job_details