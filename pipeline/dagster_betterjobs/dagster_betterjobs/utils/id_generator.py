import hashlib
from typing import Optional

def generate_company_id(company_name: str) -> str:
    """
    Generate a stable, unique company ID from company name.
    Uses first 8 characters of SHA-256 hash to create a compact ID.
    """
    # Normalize company name (lowercase, remove extra spaces)
    normalized_name = " ".join(company_name.lower().split())
    # Generate hash
    hash_object = hashlib.sha256(normalized_name.encode())
    # Take first 8 characters for a compact but still unique ID
    return hash_object.hexdigest()[:8]

def generate_job_id(company_name: str, job_title: str, date_posted: Optional[str] = None) -> str:
    """
    Generate a stable, unique job ID from company name, job title, and optionally date posted.
    Uses first 12 characters of SHA-256 hash to create a compact ID.
    """
    # Normalize inputs
    normalized_company = " ".join(company_name.lower().split())
    normalized_title = " ".join(job_title.lower().split())

    # Create a unique string combining company and job title
    unique_string = f"{normalized_company}|{normalized_title}"
    if date_posted:
        unique_string += f"|{date_posted}"

    # Generate hash
    hash_object = hashlib.sha256(unique_string.encode())
    # Take first 12 characters for a compact but still unique ID
    return hash_object.hexdigest()[:12]