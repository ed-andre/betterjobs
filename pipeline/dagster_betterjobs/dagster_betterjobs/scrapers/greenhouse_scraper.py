import json
import logging
import datetime
import requests
import time
import re
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, parse_qs
from datetime import timezone
from bs4 import BeautifulSoup

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class GreenhouseScraper(BaseScraper):
    """
    Scraper for Greenhouse-based career sites.

    Extracts job listings from Greenhouse job boards using embedded JSON data.
    Supports both direct Greenhouse boards and embedded job boards.
    """

    def __init__(self, career_url: str, dagster_log=None, ats_url=None, cutoff_date=None, **kwargs):
        """
        Initialize the Greenhouse scraper.

        Args:
            career_url: Base URL of the Greenhouse career site
            dagster_log: Optional Dagster logger for integration
            ats_url: Optional direct ATS URL (https://boards.greenhouse.io/[tenant])
            cutoff_date: Optional datetime for job freshness filter
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(**kwargs)

        self.dagster_log = dagster_log
        self.original_url = career_url

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

        # Use direct ATS URL if provided
        if ats_url and 'greenhouse.io' in ats_url:
            self.log_message("info", f"Using provided ATS URL: {ats_url}")
            parsed = urlparse(ats_url)
            domain = parsed.netloc
            path = parsed.path.strip('/')

            self.is_embedded = 'embed' in path or 'embed' in domain

            if domain.endswith('.greenhouse.io'):
                if self.is_embedded:
                    # Handle embedded job board URL format
                    self.tenant = self._extract_tenant_from_embed_url(ats_url)
                    self.log_message("info", f"Detected embedded Greenhouse board with tenant: {self.tenant}")
                    self.career_url = f"https://boards.greenhouse.io/embed/job_board?for={self.tenant}"
                    self.is_direct_api = False
                else:
                    # Direct Greenhouse job board
                    self.tenant = path
                    self.log_message("info", f"Extracted tenant from ATS URL: {self.tenant}")
                    self.career_url = f"https://job-boards.greenhouse.io/{self.tenant}"
                    self.is_direct_api = True

                self.log_message("info", f"Set Greenhouse career URL: {self.career_url}")
                self._init_session()
                return

        # Process URL normally if no direct ATS URL provided
        self.career_url = self.normalize_url(career_url)
        parsed_url = urlparse(self.career_url)
        domain = parsed_url.netloc
        path = parsed_url.path.strip('/')

        # Determine if this is an embedded job board
        self.is_embedded = 'embed' in path or 'embed' in domain

        if 'greenhouse.io' in domain:
            if self.is_embedded:
                # Handle embedded job board
                self.tenant = self._extract_tenant_from_embed_url(self.career_url)
                self.log_message("info", f"Detected embedded Greenhouse board with tenant: {self.tenant}")
                self.career_url = f"https://boards.greenhouse.io/embed/job_board?for={self.tenant}"
                self.is_direct_api = False
            else:
                # Direct Greenhouse job board
                self.tenant = path
                self.log_message("info", f"Set tenant from URL path: {self.tenant}")
                self.career_url = f"https://job-boards.greenhouse.io/{self.tenant}"
                self.is_direct_api = True
        else:
            self.log_message("warning", f"Could not determine Greenhouse tenant for {self.career_url} - scraper may not work")
            self.is_direct_api = False
            self.tenant = None

        self._init_session()

    def _init_session(self):
        """Initialize session with Greenhouse-specific headers."""
        self.session = requests.Session()

        # Set headers that work well with Greenhouse
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

    def _extract_tenant_from_embed_url(self, url: str) -> str:
        """Extract tenant name from an embedded Greenhouse job board URL."""
        # Try to extract from "for" parameter first
        if "for=" in url:
            tenant = url.split("for=")[1].split("&")[0]
            return tenant

        # Otherwise, try to extract from path
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) > 0:
            return path_parts[-1]

        return None

    def _extract_json_from_response(self, response_text: str) -> Optional[Dict]:
        """
        Extract embedded JSON data from the HTML response.

        This looks for the JSON data embedded in the __remixContext variable or other
        specific patterns in the HTML content.
        """
        # Try to extract from __remixContext
        remix_context_pattern = r'window\.__remixContext\s*=\s*({.*?});'
        matches = re.search(remix_context_pattern, response_text, re.DOTALL)

        if matches:
            try:
                json_str = matches.group(1)
                json_data = json.loads(json_str)
                return json_data
            except (json.JSONDecodeError, IndexError) as e:
                self.log_message("warning", f"Failed to parse JSON from __remixContext: {str(e)}")

        # Alternative pattern for job board data
        job_data_pattern = r'routes/\$url_token.*?"data":\s*({.*?])}'
        matches = re.search(job_data_pattern, response_text, re.DOTALL)

        if matches:
            try:
                # Need to process this differently since it's not a complete JSON object
                matched_text = matches.group(0)
                # Find the JSON portion that starts with the job_posts field
                job_posts_match = re.search(r'"jobPosts":\s*({.*?})', matched_text, re.DOTALL)
                if job_posts_match:
                    json_str = job_posts_match.group(1)
                    # Clean up the JSON string to make it valid
                    json_str = json_str.replace('undefined', 'null')
                    json_data = json.loads(json_str)
                    return {"jobPosts": json_data}
            except (json.JSONDecodeError, IndexError) as e:
                self.log_message("warning", f"Failed to parse JSON from job data: {str(e)}")

        return None

    def make_greenhouse_request(self, url: str, method: str = "GET", **kwargs) -> requests.Response:
        """
        Make a request to Greenhouse with specialized handling and retries.
        """
        try:
            # Apply rate limiting
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)

            kwargs.setdefault("timeout", self.timeout)

            # Set required headers for Greenhouse
            if "headers" not in kwargs:
                kwargs["headers"] = {}

            greenhouse_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": self.career_url
            }
            kwargs["headers"].update(greenhouse_headers)

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
            self.log_message("error", f"Error making Greenhouse request to {url}: {str(e)}")
            raise

        return None

    def search_jobs(self, keyword: str = None, location: Optional[str] = None, max_age_days: int = None) -> List[Dict]:
        """
        Search for jobs on a Greenhouse career site.

        Args:
            keyword: Optional search term filter
            location: Optional location filter
            max_age_days: Optional max age of job postings in days

        Returns:
            List of job dictionaries with basic information
        """
        jobs = []

        if not self.is_direct_api and not self.is_embedded:
            self.log_message("warning", "This instance is not properly configured for Greenhouse scraping")
            return []

        # Direct API implementation
        if self.is_direct_api:
            try:
                self.log_message("info", f"Requesting job listings from: {self.career_url}")

                # Make a request to the job board
                response = self.make_greenhouse_request(self.career_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {self.career_url}")
                    return []

                # Extract JSON data from the response
                json_data = self._extract_json_from_response(response.text)

                if not json_data:
                    self.log_message("error", "Could not extract job data from response")
                    return []

                # Log the JSON data structure
                self.log_message("info", f"Found JSON data with keys: {list(json_data.keys())}")

                # Navigate to the job posts data
                job_list = []
                if "state" in json_data and "loaderData" in json_data["state"]:
                    loader_data = json_data["state"]["loaderData"]
                    if "routes/$url_token" in loader_data:
                        job_posts_data = loader_data["routes/$url_token"]
                        if "jobPosts" in job_posts_data:
                            job_posts = job_posts_data["jobPosts"]
                            if "data" in job_posts:
                                job_list = job_posts["data"]
                                self.log_message("info", f"Found {len(job_list)} job listings")
                            else:
                                self.log_message("warning", "No 'data' field in jobPosts")
                                return []
                        else:
                            self.log_message("warning", "No 'jobPosts' field in routes/$url_token")
                            return []
                    else:
                        self.log_message("warning", "No 'routes/$url_token' in loaderData")
                        return []
                elif "jobPosts" in json_data:
                    # Alternative structure
                    job_posts = json_data["jobPosts"]
                    if "data" in job_posts:
                        job_list = job_posts["data"]
                        self.log_message("info", f"Found {len(job_list)} job listings")
                    else:
                        self.log_message("warning", "No 'data' field in jobPosts")
                        return []
                else:
                    self.log_message("warning", "Could not locate job posts data in JSON")
                    return []

                # Process each job
                for job_item in job_list:
                    # Log the job structure for the first item
                    if len(jobs) == 0:
                        self.log_message("info", f"First job item keys: {list(job_item.keys())}")

                    # Extract job details
                    job_id = job_item.get("id")
                    job_title = job_item.get("title")
                    location = job_item.get("location")
                    job_url = job_item.get("absolute_url")
                    published_at = job_item.get("published_at")
                    updated_at = job_item.get("updated_at")
                    requisition_id = job_item.get("requisition_id")

                    # Extract job description/content
                    job_description = job_item.get("content", "")

                    # Skip if missing essential fields
                    if not job_id or not job_title or not job_url:
                        continue

                    # Create department info if available
                    department = None
                    if "department" in job_item and job_item["department"]:
                        department = {
                            "name": job_item["department"].get("name"),
                            "id": job_item["department"].get("id")
                        }

                    # Check if the job posting is recent enough
                    is_recent = True
                    if published_at:
                        try:
                            # Parse the date string and handle timezone
                            # Greenhouse uses ISO format with timezone offset
                            published_date = None

                            # Handle the case where timezone info is already in string
                            if 'Z' in published_at or '+' in published_at or '-' in published_at:
                                if 'Z' in published_at:
                                    # UTC time indicated by Z
                                    published_date = datetime.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                                else:
                                    # Already has timezone offset
                                    published_date = datetime.datetime.fromisoformat(published_at)
                            else:
                                # No timezone info, assume UTC
                                published_date = datetime.datetime.fromisoformat(published_at).replace(tzinfo=timezone.utc)

                            # Now both dates are timezone-aware
                            is_recent = published_date >= self.cutoff_date
                            self.log_message("info", f"Job date: {published_date}, Is recent: {is_recent}")
                        except Exception as e:
                            self.log_message("warning", f"Error parsing date {published_at}: {str(e)}")
                            # When in doubt, include the job
                            is_recent = True

                    # Create job record with all necessary details
                    job = {
                        "job_id": str(job_id),
                        "job_title": job_title,
                        "location": location,
                        "job_url": job_url,
                        "job_description": job_description,
                        "content": job_description,  # Add content field with same value for consistency
                        "published_at": published_at,
                        "updated_at": updated_at,
                        "requisition_id": requisition_id,
                        "department": department,
                        "is_recent": is_recent,
                        "raw_data": job_item
                    }

                    # Filter by keyword if specified
                    if keyword and job_title and keyword.lower() not in job_title.lower():
                        continue

                    # Filter by location if specified
                    if location and job.get("location") and location.lower() not in job["location"].lower():
                        continue

                    jobs.append(job)

                self.log_message("info", f"Processed {len(jobs)} jobs after filtering")
                return jobs

            except Exception as e:
                self.log_message("error", f"Error searching Greenhouse jobs via direct API: {str(e)}")
                return []
        # Embedded job board implementation
        elif self.is_embedded:
            try:
                self.log_message("info", f"Requesting embedded job board listings from: {self.career_url}")

                # Request the embedded job board page
                response = self.make_greenhouse_request(self.career_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {self.career_url}")
                    return []

                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find job listings - embedded Greenhouse boards use elements with class="opening"
                job_elements = soup.select('.opening')

                self.log_message("info", f"Found {len(job_elements)} job listings on embedded board")

                # Process each job listing
                for job_element in job_elements:
                    try:
                        # Extract job title from the link
                        title_element = job_element.select_one('a')
                        if not title_element:
                            continue

                        job_title = title_element.text.strip()
                        job_url = title_element.get('href')

                        # Extract location if available
                        location_element = job_element.select_one('.location')
                        job_location = location_element.text.strip() if location_element else ""

                        # Extract job ID from the URL
                        job_id = None
                        if 'gh_jid=' in job_url:
                            # Parse the gh_jid parameter from the URL
                            parsed_url = urlparse(job_url)
                            query_params = parse_qs(parsed_url.query)
                            if 'gh_jid' in query_params:
                                job_id = query_params['gh_jid'][0]

                        if not job_id:
                            self.log_message("warning", f"Could not extract job ID from URL: {job_url}")
                            continue

                        # Create basic job record
                        job = {
                            "job_id": str(job_id),
                            "job_title": job_title,
                            "location": job_location,
                            "job_url": job_url,
                            "is_recent": True,  # We'll assume it's recent until we get detailed info
                        }

                        # Filter by keyword if specified
                        if keyword and job_title and keyword.lower() not in job_title.lower():
                            continue

                        # Filter by location if specified
                        if location and job_location and location.lower() not in job_location.lower():
                            continue

                        jobs.append(job)

                    except Exception as e:
                        self.log_message("warning", f"Error processing job listing: {str(e)}")
                        continue

                self.log_message("info", f"Processed {len(jobs)} jobs after filtering")
                return jobs

            except Exception as e:
                self.log_message("error", f"Error searching embedded Greenhouse jobs: {str(e)}")
                return []

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

        # For direct API jobs
        if self.is_direct_api:
            try:
                self.log_message("info", f"Getting details for job URL: {job_url}")

                # Make a request to the job detail page
                response = self.make_greenhouse_request(job_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {job_url}")
                    return job_details

                # Extract JSON data from the response
                json_data = self._extract_json_from_response(response.text)

                if not json_data:
                    self.log_message("error", "Could not extract job detail data from response")
                    return job_details

                # Log the JSON data structure
                self.log_message("info", f"Found JSON data with keys: {list(json_data.keys())}")

                # Navigate to the job data
                job_data = None
                if "state" in json_data and "loaderData" in json_data["state"]:
                    loader_data = json_data["state"]["loaderData"]
                    for key, value in loader_data.items():
                        if key.startswith("routes/$url_token/jobs"):
                            job_data = value
                            break

                if not job_data:
                    self.log_message("warning", "Could not locate job detail data in JSON")
                    return job_details

                # Extract job ID from URL if not already present
                if not job_details.get("job_id") and '/jobs/' in job_url:
                    job_id = job_url.split('/jobs/')[1].split('/')[0]
                    job_details["job_id"] = job_id

                # Process job detail fields
                if "title" in job_data:
                    job_details["job_title"] = job_data["title"]

                if "location" in job_data:
                    job_details["location"] = job_data["location"]

                if "content" in job_data:
                    job_details["job_description"] = job_data["content"]

                if "department" in job_data and job_data["department"]:
                    job_details["department"] = {
                        "name": job_data["department"].get("name"),
                        "id": job_data["department"].get("id")
                    }

                if "published_at" in job_data:
                    job_details["published_at"] = job_data["published_at"]

                    # Check if the job posting is recent enough
                    try:
                        # Greenhouse uses ISO format with timezone
                        published_date = datetime.datetime.fromisoformat(job_data["published_at"].replace('Z', '+00:00'))
                        job_details["is_recent"] = published_date >= self.cutoff_date
                    except Exception as e:
                        self.log_message("warning", f"Error parsing date: {str(e)}")
                        job_details["is_recent"] = True
                else:
                    job_details["is_recent"] = True  # Assume recent if no date

                if "updated_at" in job_data:
                    job_details["updated_at"] = job_data["updated_at"]

                if "requisition_id" in job_data:
                    job_details["requisition_id"] = job_data["requisition_id"]

                job_details["raw_data"] = job_data

                return job_details

            except Exception as e:
                self.log_message("error", f"Error getting Greenhouse job details via direct API: {str(e)}")
                return job_details

        # For embedded job board jobs
        elif self.is_embedded and 'gh_jid=' in job_url:
            try:
                self.log_message("info", f"Getting details for embedded job URL: {job_url}")

                # Extract the job ID from the URL
                parsed_url = urlparse(job_url)
                query_params = parse_qs(parsed_url.query)

                if 'gh_jid' not in query_params:
                    self.log_message("error", f"Could not extract job ID from URL: {job_url}")
                    return job_details

                job_id = query_params['gh_jid'][0]
                job_details["job_id"] = job_id

                # Construct the modified URL to get structured job details
                # Format: https://job-boards.greenhouse.io/embed/job_app?for=TENANT&token=JOB_ID
                detail_url = f"https://job-boards.greenhouse.io/embed/job_app?for={self.tenant}&token={job_id}"

                self.log_message("info", f"Requesting job details from: {detail_url}")

                # Make request to the structured job detail endpoint
                response = self.make_greenhouse_request(detail_url)

                if not response:
                    self.log_message("error", f"Failed to get response from {detail_url}")
                    return job_details

                # Try to parse JSON response
                try:
                    job_data = response.json()

                    # Extract relevant fields
                    if "title" in job_data:
                        job_details["job_title"] = job_data["title"]

                    if "location" in job_data:
                        job_details["location"] = job_data["location"]["name"] if isinstance(job_data["location"], dict) else job_data["location"]

                    if "content" in job_data:
                        job_details["job_description"] = job_data["content"]
                        job_details["content"] = job_data["content"]

                    if "departments" in job_data and job_data["departments"] and len(job_data["departments"]) > 0:
                        dept = job_data["departments"][0]
                        job_details["department"] = {
                            "name": dept.get("name"),
                            "id": dept.get("id")
                        }

                    if "metadata" in job_data:
                        metadata = job_data["metadata"]
                        if "posted_at" in metadata:
                            job_details["published_at"] = metadata["posted_at"]

                            # Check if the job posting is recent enough
                            try:
                                # Parse the date string
                                published_date = datetime.datetime.fromisoformat(metadata["posted_at"].replace('Z', '+00:00'))
                                job_details["is_recent"] = published_date >= self.cutoff_date
                            except Exception as e:
                                self.log_message("warning", f"Error parsing date: {str(e)}")
                                job_details["is_recent"] = True

                        if "updated_at" in metadata:
                            job_details["updated_at"] = metadata["updated_at"]

                    # Store raw data for reference
                    job_details["raw_data"] = job_data

                except ValueError as e:
                    self.log_message("warning", f"Error parsing JSON response: {str(e)}")

                    # If JSON parsing fails, try to extract info from HTML
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Extract job title
                    title_elem = soup.select_one('h1.app-title')
                    if title_elem:
                        job_details["job_title"] = title_elem.text.strip()

                    # Extract location
                    location_elem = soup.select_one('.location')
                    if location_elem:
                        job_details["location"] = location_elem.text.strip()

                    # Extract job description
                    content_elem = soup.select_one('#content')
                    if content_elem:
                        job_details["job_description"] = content_elem.get_text(separator='\n').strip()
                        job_details["content"] = job_details["job_description"]

                return job_details

            except Exception as e:
                self.log_message("error", f"Error getting embedded Greenhouse job details: {str(e)}")
                return job_details

        return job_details

    def log_message(self, level: str, message: str):
        """
        Log a message using either the Dagster logger (if provided) or the standard Python logger.

        Args:
            level: Log level (info, warning, error)
            message: Message to log
        """
        # First print to console for direct visibility
        print(f"GREENHOUSE_SCRAPER: {level.upper()} - {message}")

        # Use Dagster logger if available
        if self.dagster_log:
            if level == "info":
                self.dagster_log.info(message)
            elif level == "warning":
                self.dagster_log.warning(message)
            elif level == "error":
                self.dagster_log.error(message)
            elif level == "debug":
                self.dagster_log.debug(message)
        # Fall back to standard logging
        else:
            if level == "info":
                logging.info(message)
            elif level == "warning":
                logging.warning(message)
            elif level == "error":
                logging.error(message)
            elif level == "debug":
                logging.debug(message)