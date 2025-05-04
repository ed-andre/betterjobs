import json
import logging
import datetime
import requests
import time
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

from dagster_betterjobs.scrapers.base_scraper import BaseScraper

class BambooHRScraper(BaseScraper):
    """
    Scraper for BambooHR-based career sites.

    Extracts job listings using BambooHR's API endpoints.
    """

    def __init__(self, career_url: str, dagster_log=None, ats_url=None, cutoff_date=None, **kwargs):
        """
        Initialize the BambooHR scraper.

        Args:
            career_url: Base URL of the BambooHR career site
            dagster_log: Optional Dagster logger for integration
            ats_url: Optional direct ATS URL (https://companyname.bamboohr.com)
            cutoff_date: Optional datetime for job freshness filter
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(**kwargs)

        self.dagster_log = dagster_log
        self.original_url = career_url

        # Set cutoff date (default: 14 days ago)
        self.cutoff_date = cutoff_date or datetime.datetime.now() - datetime.timedelta(days=14)
        self.log_message("info", f"Setting cutoff date for jobs to {self.cutoff_date.strftime('%Y-%m-%d')}")

        # Use direct ATS URL if provided
        if ats_url and 'bamboohr.com' in ats_url:
            self.log_message("info", f"Using provided ATS URL: {ats_url}")
            parsed = urlparse(ats_url)
            domain = parsed.netloc
            if domain.endswith('.bamboohr.com'):
                self.company_domain = domain.split('.')[0]
                self.log_message("info", f"Extracted company domain from ATS URL: {self.company_domain}")
                self.career_url = f"https://{self.company_domain}.bamboohr.com/careers/"
                self.listing_url = f"{self.career_url}list"
                self.log_message("info", f"Set BambooHR career URL: {self.career_url}")
                self.log_message("info", f"Set BambooHR listing URL: {self.listing_url}")

                self._init_session()
                return

        # Process URL normally if no direct ATS URL provided
        self.career_url = self.normalize_url(career_url)
        self.domain = self.get_domain(self.career_url)
        self.company_domain = self.extract_company_domain(self.career_url)

        # Try alternate methods to extract company domain
        if not self.company_domain:
            self.log_message("warning", f"Could not extract company domain from {self.career_url}, trying alternate methods")
            if '.bamboohr.com' in self.domain:
                self.company_domain = self.domain.split('.bamboohr.com')[0]
                self.log_message("info", f"Extracted company domain from URL domain: {self.company_domain}")

        if self.company_domain:
            # Use standard BambooHR URL format
            self.career_url = f"https://{self.company_domain}.bamboohr.com/careers/"
            self.log_message("info", f"Using standardized BambooHR URL: {self.career_url}")
        else:
            # Attempt URL format correction if domain extraction failed
            self.log_message("warning", f"Could not determine BambooHR company domain for {self.career_url} - scraper may not work")

            if not self.career_url.endswith('/'):
                self.career_url += '/'
            if not self.career_url.endswith('careers/'):
                if self.career_url.endswith('careers'):
                    self.career_url += '/'
                else:
                    self.career_url = self.career_url.rstrip('/') + '/careers/'

        # Set API endpoint URLs
        self.listing_url = f"{self.career_url}list"
        self.log_message("info", f"BambooHR scraper initialized with listing URL: {self.listing_url}")
        self._init_session()

    def _init_session(self):
        """Initialize session with BambooHR-specific headers."""
        self.session = requests.Session()

        # Set headers that work well with BambooHR API
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Referer": self.career_url
        })

    def make_bamboohr_request(self, url: str, method: str = "GET", **kwargs) -> requests.Response:
        """
        Make a request to BambooHR with specialized handling and retries.
        """
        try:
            # Apply rate limiting
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)

            kwargs.setdefault("timeout", self.timeout)

            # Set required headers for BambooHR
            if "headers" not in kwargs:
                kwargs["headers"] = {}

            bamboohr_headers = {
                "accept": "application/json, text/plain, */*",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "referer": self.career_url
            }
            kwargs["headers"].update(bamboohr_headers)

            # Implement retry logic
            for attempt in range(self.max_retries):
                try:
                    self.last_request_time = time.time()
                    response = self.session.request(method, url, **kwargs)

                    self.log_message("info", f"Request to {url} returned status {response.status_code}")
                    if response.status_code != 200:
                        self.log_message("warning", f"Non-200 response: {response.status_code} - {response.text[:200]}")

                    response.raise_for_status()

                    # Validate JSON response
                    try:
                        json_data = response.json()
                        return response
                    except json.JSONDecodeError:
                        self.log_message("warning", f"Response is not valid JSON: {response.text[:200]}")
                        if attempt < self.max_retries - 1:
                            time.sleep(self.retry_delay)
                            continue
                        else:
                            raise

                except requests.RequestException as e:
                    self.log_message("warning", f"Request failed (attempt {attempt+1}/{self.max_retries}): {str(e)}")

                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    else:
                        raise

        except Exception as e:
            self.log_message("error", f"Error making BambooHR request to {url}: {str(e)}")
            raise

        return None

    def search_jobs(self, keyword: str = None, location: Optional[str] = None, max_age_days: int = None) -> List[Dict]:
        """
        Search for jobs on a BambooHR career site.

        Args:
            keyword: Optional search term filter
            location: Optional location filter
            max_age_days: Optional max age of job postings in days

        Returns:
            List of job dictionaries with basic information
        """
        jobs = []

        try:
            self.log_message("info", f"Requesting job listings from: {self.listing_url}")
            print(f"DEBUG: Requesting job listings from: {self.listing_url}")  # Console debug

            # Try different approaches to get job listings
            response = None

            # First attempt: Use our specialized request method
            try:
                self.log_message("info", "Attempting first request method with specialized headers")
                response = self.make_bamboohr_request(self.listing_url)
            except Exception as e:
                self.log_message("warning", f"First attempt failed: {str(e)}")
                print(f"DEBUG: First attempt failed: {str(e)}")  # Console debug

            # Second attempt: Try with standard request if first failed
            if not response:
                try:
                    self.log_message("info", "Attempting second request method with standard headers")
                    response = self.make_request(self.listing_url, headers={
                        "accept": "application/json, text/plain, */*",
                        "accept-language": "en-US,en;q=0.9",
                        "cache-control": "no-cache",
                        "pragma": "no-cache"
                    })
                except Exception as e:
                    self.log_message("warning", f"Second attempt failed: {str(e)}")
                    print(f"DEBUG: Second attempt failed: {str(e)}")  # Console debug

            # If we still don't have a response, try a different URL format
            if not response and self.company_domain:
                alt_url = f"https://{self.company_domain}.bamboohr.com/careers/list"
                self.log_message("info", f"Trying alternative URL: {alt_url}")
                print(f"DEBUG: Trying alternative URL: {alt_url}")  # Console debug
                try:
                    response = self.make_request(alt_url, headers={
                        "accept": "application/json, text/plain, */*",
                        "accept-language": "en-US,en;q=0.9",
                        "cache-control": "no-cache",
                        "pragma": "no-cache"
                    })
                except Exception as e:
                    self.log_message("warning", f"Alternative URL attempt failed: {str(e)}")
                    print(f"DEBUG: Alternative URL attempt failed: {str(e)}")  # Console debug

            if not response:
                self.log_message("error", f"All attempts to get job listings failed for {self.career_url}")
                print(f"DEBUG: All attempts to get job listings failed for {self.career_url}")  # Console debug
                return []

            # Parse the JSON response
            try:
                data = response.json()
                self.log_message("info", f"Successfully parsed JSON response from {self.listing_url}")
                print(f"DEBUG: Response status: {response.status_code}")  # Console debug

                # Log a sample of the response content
                response_sample = str(response.text)[:500] + "..." if len(response.text) > 500 else response.text
                self.log_message("info", f"Response sample: {response_sample}")
                print(f"DEBUG: Response sample: {response_sample}")  # Console debug
            except json.JSONDecodeError as e:
                self.log_message("error", f"Failed to parse JSON response: {str(e)}")
                self.log_message("error", f"Response content: {response.text[:500]}...")
                print(f"DEBUG: Failed to parse JSON response: {str(e)}")  # Console debug
                print(f"DEBUG: Response content: {response.text[:500]}...")  # Console debug
                return []

            # Log the response structure for debugging
            if isinstance(data, dict):
                keys = list(data.keys())
                self.log_message("info", f"Response keys: {keys}")
                print(f"DEBUG: Response keys: {keys}")  # Console debug
            else:
                self.log_message("warning", f"Response is not a dictionary: {type(data)}")
                print(f"DEBUG: Response is not a dictionary: {type(data)}")  # Console debug
                return []

            # Check if the response has the expected structure
            if "result" not in data:
                self.log_message("warning", f"Response missing 'result' key. Keys: {list(data.keys())}")
                print(f"DEBUG: Response missing 'result' key. Keys: {list(data.keys())}")  # Console debug

                # Some BambooHR sites might return a different structure
                # Try to find the job listings in the response
                if isinstance(data, list):
                    self.log_message("info", f"Response is a list with {len(data)} items - trying to parse directly")
                    print(f"DEBUG: Response is a list with {len(data)} items")  # Console debug
                    result_data = data
                elif "data" in data:
                    self.log_message("info", "Found 'data' key instead of 'result'")
                    print(f"DEBUG: Found 'data' key with {len(data['data'])} items")  # Console debug
                    result_data = data["data"]
                elif "jobs" in data:
                    self.log_message("info", "Found 'jobs' key instead of 'result'")
                    print(f"DEBUG: Found 'jobs' key with {len(data['jobs'])} items")  # Console debug
                    result_data = data["jobs"]
                else:
                    self.log_message("error", f"Unexpected response format from BambooHR: {data}")
                    print(f"DEBUG: Unexpected response format from BambooHR")  # Console debug
                    return []
            else:
                result_data = data["result"]

                # Check if result is a list as expected
                if not isinstance(result_data, list):
                    self.log_message("warning", f"'result' is not a list: {type(result_data)}")
                    print(f"DEBUG: 'result' is not a list: {type(result_data)}")  # Console debug

                    # Try to handle different result structures
                    if isinstance(result_data, dict) and "data" in result_data:
                        self.log_message("info", "Found nested 'data' key in 'result'")
                        print(f"DEBUG: Found nested 'data' key in 'result'")  # Console debug
                        result_data = result_data["data"]
                    elif isinstance(result_data, dict) and "jobs" in result_data:
                        self.log_message("info", "Found nested 'jobs' key in 'result'")
                        print(f"DEBUG: Found nested 'jobs' key in 'result'")  # Console debug
                        result_data = result_data["jobs"]
                    else:
                        self.log_message("error", f"Cannot process 'result' data: {result_data}")
                        print(f"DEBUG: Cannot process 'result' data")  # Console debug
                        return []

            # Log number of job listings found
            self.log_message("info", f"Found {len(result_data)} job listings")
            print(f"DEBUG: Found {len(result_data)} job listings")  # Console debug

            # Log sample job item if available
            if result_data and len(result_data) > 0:
                sample_job = result_data[0]
                self.log_message("info", f"Sample job item: {json.dumps(sample_job, indent=2)[:500]}...")
                print(f"DEBUG: Sample job item: {json.dumps(sample_job, indent=2)[:500]}...")  # Console debug

            # Process job listings
            for job_item in result_data:
                # Log start of processing for this job item
                self.log_message("info", f"Processing job item: {job_item.get('id', 'Unknown ID')}")
                print(f"DEBUG: Processing job item: {job_item.get('id', 'Unknown ID')}")  # Console debug

                # Log the job item structure for debugging the first one
                if len(jobs) == 0:
                    job_keys = list(job_item.keys() if isinstance(job_item, dict) else [])
                    self.log_message("info", f"First job item keys: {job_keys}")
                    print(f"DEBUG: First job item keys: {job_keys}")  # Console debug

                # Skip non-dictionary items
                if not isinstance(job_item, dict):
                    self.log_message("warning", "Skipping non-dictionary job item")
                    print("DEBUG: Skipping non-dictionary job item")  # Console debug
                    continue

                # Extract job ID with different possible key names
                job_id = None
                id_keys = ["id", "jobId", "job_id", "jobOpeningId"]
                for key in id_keys:
                    if key in job_item:
                        job_id = job_item[key]
                        self.log_message("info", f"Found job ID using key '{key}': {job_id}")
                        print(f"DEBUG: Found job ID using key '{key}': {job_id}")  # Console debug
                        break

                if not job_id:
                    self.log_message("warning", f"Could not find job ID in item: {job_item}")
                    print(f"DEBUG: Could not find job ID in item, keys: {list(job_item.keys())}")  # Console debug
                    continue

                # Extract job title with different possible key names
                job_title = None
                title_keys = ["jobOpeningName", "title", "name", "jobTitle", "job_title"]
                for key in title_keys:
                    if key in job_item and job_item[key]:
                        job_title = job_item[key]
                        logging.info(f"Found job title using key '{key}': {job_title}")
                        print(f"DEBUG: Found job title using key '{key}': {job_title}")  # Console debug
                        break

                if not job_title:
                    logging.warning(f"Could not find job title in item with ID {job_id}")
                    print(f"DEBUG: Could not find job title for ID {job_id}, keys: {list(job_item.keys())}")  # Console debug
                    continue

                # Create job record with common fields
                job = {
                    "job_id": job_id,
                    "job_title": job_title,
                    "raw_data": job_item
                }

                # Extract other fields if available
                if "departmentId" in job_item:
                    job["department_id"] = job_item["departmentId"]
                if "departmentLabel" in job_item:
                    job["department"] = job_item["departmentLabel"]
                elif "department" in job_item:
                    job["department"] = job_item["department"]

                if "employmentStatusLabel" in job_item:
                    job["employment_status"] = job_item["employmentStatusLabel"]
                elif "employmentStatus" in job_item:
                    job["employment_status"] = job_item["employmentStatus"]

                # Extract location with different possible structures
                if "location" in job_item and job_item["location"]:
                    location_data = job_item["location"]

                    # Handle location as either dict or string
                    if isinstance(location_data, dict):
                        location_dict = {
                            "city": location_data.get("city"),
                            "state": location_data.get("state")
                        }
                        job["location"] = location_dict

                        # Create formatted location string
                        loc_parts = []
                        if location_data.get("city"):
                            loc_parts.append(location_data["city"])
                        if location_data.get("state"):
                            loc_parts.append(location_data["state"])
                        if loc_parts:
                            job["location_string"] = ", ".join(loc_parts)
                    elif isinstance(location_data, str):
                        job["location_string"] = location_data

                # Construct job URL
                job["job_url"] = f"{self.career_url}{job['job_id']}"

                # Filter by keyword if specified
                if keyword and job["job_title"] and keyword.lower() not in job["job_title"].lower():
                    continue

                # Filter by location if specified
                if location and job.get("location_string") and location.lower() not in job["location_string"].lower():
                    continue

                jobs.append(job)

            logging.info(f"Processed {len(jobs)} jobs after filtering")
            return jobs

        except Exception as e:
            logging.error(f"Error searching BambooHR jobs: {str(e)}", exc_info=True)
            return []

    def get_job_details(self, job_url: str) -> Dict:
        """
        Get detailed information about a specific job.

        Args:
            job_url: URL of the job posting or job ID

        Returns:
            Dictionary containing job details
        """
        job_details = {
            'job_url': job_url
        }

        try:
            # Extract job ID from URL if necessary
            job_id = job_url
            if '/' in job_url:
                job_id = job_url.rstrip('/').split('/')[-1]

            self.log_message("info", f"Getting details for job ID: {job_id}")
            print(f"DEBUG: Getting details for job ID: {job_id}")  # Console debug

            # Construct detail URL
            detail_url = f"{self.career_url}{job_id}/detail"
            self.log_message("info", f"Requesting job details from: {detail_url}")
            print(f"DEBUG: Requesting job details from: {detail_url}")  # Console debug

            # Make request to detail endpoint
            try:
                self.log_message("info", "Attempting to get job details with specialized request")
                response = self.make_bamboohr_request(detail_url)
            except Exception as e:
                self.log_message("warning", f"Failed to get job details with specialized request: {str(e)}")
                print(f"DEBUG: Failed to get job details with specialized request: {str(e)}")  # Console debug
                self.log_message("info", "Falling back to standard request for job details")
                response = self.make_request(detail_url, headers={
                    "accept": "application/json, text/plain, */*",
                    "accept-language": "en-US,en;q=0.9",
                    "cache-control": "no-cache",
                    "pragma": "no-cache"
                })

            # Parse the JSON response
            try:
                data = response.json()
                self.log_message("info", f"Successfully parsed JSON details response")
                print(f"DEBUG: Job details response status: {response.status_code}")  # Console debug

                # Log response keys
                if isinstance(data, dict):
                    self.log_message("info", f"Job details response keys: {list(data.keys())}")
                    print(f"DEBUG: Job details response keys: {list(data.keys())}")  # Console debug

            except json.JSONDecodeError as e:
                self.log_message("error", f"Failed to parse job details JSON: {str(e)}")
                self.log_message("error", f"Response content: {response.text[:500]}...")
                print(f"DEBUG: Failed to parse job details JSON: {str(e)}")  # Console debug
                return job_details

            # Check if the response has the expected structure
            if "result" not in data:
                logging.warning(f"Details response missing 'result' key. Keys: {list(data.keys())}")

                # Some BambooHR sites might have a different structure
                if "data" in data:
                    job_data = data["data"]
                elif "job" in data:
                    job_data = data["job"]
                else:
                    logging.error(f"Unexpected detail response format from BambooHR: {data}")
                    return job_details
            else:
                job_data = data["result"]

            # Extract job opening details - handle different possible structures
            if isinstance(job_data, dict) and "jobOpening" in job_data:
                job_opening = job_data.get("jobOpening", {})
                logging.info("Found 'jobOpening' structure in job data")
                print(f"DEBUG: Found 'jobOpening' structure with keys: {list(job_opening.keys())}")  # Console debug
            else:
                job_opening = job_data  # Assume the result itself is the job data
                logging.info("Using job_data directly as job_opening")
                print(f"DEBUG: Using job_data directly as job_opening, type: {type(job_data)}")  # Console debug
                if isinstance(job_data, dict):
                    print(f"DEBUG: job_data keys: {list(job_data.keys())}")  # Console debug

            # Log job opening structure
            if isinstance(job_opening, dict):
                logging.info(f"Job opening keys: {list(job_opening.keys())}")
                print(f"DEBUG: Job opening keys: {list(job_opening.keys())}")  # Console debug
            else:
                logging.warning(f"Job opening is not a dictionary: {type(job_opening)}")
                print(f"DEBUG: Job opening is not a dictionary: {type(job_opening)}")  # Console debug

            # Map fields to our standard format with fallbacks for different key names
            job_details['job_id'] = job_id

            # Title
            if "jobOpeningName" in job_opening:
                job_details['job_title'] = job_opening.get("jobOpeningName")
            elif "title" in job_opening:
                job_details['job_title'] = job_opening.get("title")
            elif "name" in job_opening:
                job_details['job_title'] = job_opening.get("name")

            # URL
            if "jobOpeningShareUrl" in job_opening:
                job_details['job_url'] = job_opening.get("jobOpeningShareUrl", job_url)
            elif "shareUrl" in job_opening:
                job_details['job_url'] = job_opening.get("shareUrl", job_url)

            # Description
            if "description" in job_opening:
                job_details['job_description'] = job_opening.get("description")
            elif "jobDescription" in job_opening:
                job_details['job_description'] = job_opening.get("jobDescription")

            # Status
            if "jobOpeningStatus" in job_opening:
                job_details['job_status'] = job_opening.get("jobOpeningStatus")
            elif "status" in job_opening:
                job_details['job_status'] = job_opening.get("status")

            # Other fields
            if "employmentStatusLabel" in job_opening:
                job_details['employment_status'] = job_opening.get("employmentStatusLabel")
            elif "employmentStatus" in job_opening:
                job_details['employment_status'] = job_opening.get("employmentStatus")

            if "departmentLabel" in job_opening:
                job_details['department'] = job_opening.get("departmentLabel")
            elif "department" in job_opening:
                job_details['department'] = job_opening.get("department")

            if "departmentId" in job_opening:
                job_details['department_id'] = job_opening.get("departmentId")

            # Check for date fields
            logging.info("Checking for date fields")
            print(f"DEBUG: Checking for date fields in job opening")  # Console debug

            if "datePosted" in job_opening:
                job_details['date_posted'] = job_opening.get("datePosted")
                logging.info(f"Found datePosted field: {job_opening.get('datePosted')}")
                print(f"DEBUG: Found datePosted field: {job_opening.get('datePosted')}")  # Console debug
            elif "postedDate" in job_opening:
                job_details['date_posted'] = job_opening.get("postedDate")
                logging.info(f"Found postedDate field: {job_opening.get('postedDate')}")
                print(f"DEBUG: Found postedDate field: {job_opening.get('postedDate')}")  # Console debug
            else:
                logging.warning(f"No date field found in job data. Available fields: {list(job_opening.keys())}")
                print(f"DEBUG: No date field found. Available fields: {list(job_opening.keys())}")  # Console debug

                # Try to find date field with case-insensitive search
                for key in job_opening.keys():
                    if 'date' in key.lower() or 'posted' in key.lower():
                        logging.info(f"Found potential date field with key '{key}': {job_opening.get(key)}")
                        print(f"DEBUG: Found potential date field with key '{key}': {job_opening.get(key)}")  # Console debug

            # Check if the job posting is recent enough
            if job_details.get("date_posted"):
                try:
                    # Log the date format we're trying to parse
                    logging.info(f"Trying to parse date: {job_details['date_posted']}")
                    print(f"DEBUG: Trying to parse date: {job_details['date_posted']}")  # Console debug
                    print(f"DEBUG: Cutoff date for comparison: {self.cutoff_date}")  # Console debug

                    # Try multiple date formats
                    date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]
                    posted_date = None

                    for date_format in date_formats:
                        try:
                            posted_date = datetime.datetime.strptime(job_details["date_posted"], date_format)
                            logging.info(f"Successfully parsed date with format {date_format}")
                            print(f"DEBUG: Successfully parsed date with format {date_format}: {posted_date}")  # Console debug
                            break
                        except ValueError:
                            logging.info(f"Failed to parse date with format {date_format}")
                            print(f"DEBUG: Failed to parse date with format {date_format}")  # Console debug
                            continue

                    if posted_date:
                        job_details["is_recent"] = posted_date >= self.cutoff_date
                        logging.info(f"Job date: {posted_date}, Cutoff date: {self.cutoff_date}, Is recent: {job_details['is_recent']}")
                        print(f"DEBUG: Job date: {posted_date}, Is recent: {job_details['is_recent']}")  # Console debug
                    else:
                        # If we couldn't parse with any format, assume it's recent
                        logging.warning(f"Could not parse date {job_details['date_posted']} with any format")
                        print(f"DEBUG: Could not parse date {job_details['date_posted']} with any format")  # Console debug
                        job_details["is_recent"] = True
                except Exception as e:
                    # If we can't parse the date, assume it's recent
                    logging.warning(f"Error parsing date: {str(e)}")
                    print(f"DEBUG: Error parsing date: {str(e)}")  # Console debug
                    job_details["is_recent"] = True
            else:
                # If no date is available, assume it's recent
                logging.warning("No date_posted field available")
                print("DEBUG: No date_posted field available")  # Console debug
                job_details["is_recent"] = True

            # Extract compensation field
            if "compensation" in job_opening:
                job_details['compensation'] = job_opening.get("compensation")
                logging.info(f"Found compensation field: {job_opening.get('compensation')}")
                print(f"DEBUG: Found compensation field: {job_opening.get('compensation')}")  # Console debug

            job_details['raw_data'] = job_data

            # Extract location with different possible structures
            if "location" in job_opening and job_opening["location"]:
                location_data = job_opening["location"]

                # Handle location as either dict or string
                if isinstance(location_data, dict):
                    job_details["location"] = {
                        "city": location_data.get("city"),
                        "state": location_data.get("state"),
                        "postal_code": location_data.get("postalCode"),
                        "country": location_data.get("addressCountry")
                    }

                    # Create formatted location string
                    loc_parts = []
                    if location_data.get("city"):
                        loc_parts.append(location_data["city"])
                    if location_data.get("state"):
                        loc_parts.append(location_data["state"])
                    if loc_parts:
                        job_details["location_string"] = ", ".join(loc_parts)
                elif isinstance(location_data, str):
                    job_details["location_string"] = location_data

            return job_details

        except Exception as e:
            logging.error(f"Error getting BambooHR job details: {str(e)}", exc_info=True)
            return job_details

    @staticmethod
    def extract_company_domain(url: str) -> str:
        """Extract the company subdomain from a BambooHR URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc

            # Extract subdomain from bamboohr.com domain
            if domain.endswith('.bamboohr.com'):
                return domain.split('.')[0]

            # Handle cases where the URL might be structured differently
            if 'bamboohr.com' in domain:
                parts = domain.split('.')
                for i, part in enumerate(parts):
                    if part == 'bamboohr' and i > 0:
                        return parts[i-1]

            return None
        except Exception as e:
            logging.error(f"Error extracting company domain: {str(e)}")
            return None

    def log_message(self, level: str, message: str):
        """
        Log a message using either the Dagster logger (if provided) or the standard Python logger.

        Args:
            level: Log level (info, warning, error)
            message: Message to log
        """
        # First print to console for direct visibility
        print(f"BAMBOOHR_SCRAPER: {level.upper()} - {message}")

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