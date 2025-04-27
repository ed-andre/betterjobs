import requests
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from urllib.parse import urlparse

class BaseScraper(ABC):
    """
    Base class for all job platform scrapers.

    This class provides common functionality for scraping job listings
    from various platforms, including request handling, rate limiting,
    and error handling.
    """

    def __init__(
        self,
        rate_limit: float = 1.0,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 2,
        user_agent: Optional[str] = None,
    ):
        """
        Initialize the scraper with common parameters.

        Args:
            rate_limit: Minimum seconds between requests
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            user_agent: Custom user agent string (or None to use default)
        """
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
        )

        self.session = self._create_session()
        self.last_request_time = 0

    def _create_session(self) -> requests.Session:
        """Create and configure a requests session."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        return session

    def make_request(self, url: str, method: str = "GET", **kwargs) -> requests.Response:
        """
        Make an HTTP request with rate limiting and retries.

        Args:
            url: URL to request
            method: HTTP method (GET, POST, etc.)
            **kwargs: Additional arguments to pass to requests

        Returns:
            Response object
        """
        # Apply rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

        kwargs.setdefault("timeout", self.timeout)

        # Try the request with retries
        for attempt in range(self.max_retries):
            try:
                self.last_request_time = time.time()
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logging.warning(f"Request failed (attempt {attempt+1}/{self.max_retries}): {str(e)}")

                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    @staticmethod
    def normalize_url(url: str) -> str:
        """Normalize a URL by ensuring it has a scheme."""
        if not url.startswith(('http://', 'https://')):
            return f"https://{url}"
        return url

    @staticmethod
    def get_domain(url: str) -> str:
        """Extract the domain from a URL."""
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain

    @abstractmethod
    def search_jobs(self, keyword: str, location: Optional[str] = None) -> List[Dict]:
        """
        Search for jobs matching the given keyword and optional location.

        Args:
            keyword: Search term (e.g., "SQL Developer")
            location: Optional location to filter by

        Returns:
            List of job dictionaries
        """
        pass

    @abstractmethod
    def get_job_details(self, job_url: str) -> Dict:
        """
        Get detailed information about a specific job.

        Args:
            job_url: URL of the job posting

        Returns:
            Dictionary containing job details
        """
        pass