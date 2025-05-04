from typing import Optional
import logging

from dagster_betterjobs.scrapers.base_scraper import BaseScraper
from dagster_betterjobs.scrapers.workday_scraper import WorkdayScraper
from dagster_betterjobs.scrapers.greenhouse_scraper import GreenhouseScraper
from dagster_betterjobs.scrapers.bamboohr_scraper import BambooHRScraper

def create_scraper(platform: str, career_url: str, **kwargs) -> Optional[BaseScraper]:
    """
    Factory function to create the appropriate scraper based on platform.

    Args:
        platform: The platform identifier (e.g., 'workday', 'greenhouse', 'bamboohr')
        career_url: URL to the company's career page
        **kwargs: Additional arguments to pass to the scraper constructor

    Returns:
        An initialized scraper or None if the platform is not supported
    """
    platform = platform.lower()

    if not career_url:
        logging.error(f"Cannot create scraper for platform '{platform}' without a career URL")
        return None

    try:
        if platform == 'workday':
            return WorkdayScraper(career_url=career_url, **kwargs)
        elif platform == 'greenhouse':
            return GreenhouseScraper(career_url=career_url, **kwargs)
        elif platform == 'bamboohr':
            return BambooHRScraper(career_url=career_url, **kwargs)
        else:
            logging.warning(f"Unsupported platform: {platform}")
            return None
    except Exception as e:
        logging.error(f"Error creating scraper for platform '{platform}': {str(e)}")
        return None

__all__ = ['create_scraper', 'BaseScraper', 'WorkdayScraper', 'GreenhouseScraper', 'BambooHRScraper']