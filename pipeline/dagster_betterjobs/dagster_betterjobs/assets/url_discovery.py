import os
import csv
import json
import pandas as pd
from typing import Dict, List, Tuple, Set, Optional, Any
import logging
import time
from pathlib import Path
import traceback
import requests
from urllib.parse import urlparse
import re

from dagster import asset, AssetExecutionContext, Config, get_dagster_logger, Output, AssetMaterialization, MaterializeResult
from dagster_gemini import GeminiResource

from .validate_urls import validate_urls
logger = get_dagster_logger()






def process_other_companies(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    companies: List[Dict]
) -> List[Dict]:
    """Process companies using ATS platforms other than Greenhouse."""
    company_list_json = json.dumps([{
        "company_name": company["company_name"],
        "company_industry": company["company_industry"],
        "platform": company["platform"]
    } for company in companies], indent=2)

    prompt = f"""
    Find accurate job board URLs for these companies using various ATS (Applicant Tracking Systems).

    For each company, I've provided:
    - Company name
    - Industry
    - The ATS they use (e.g., Workday, Lever, etc.)

    IMPORTANT: DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists and you've verified it.
    It's better to return null than an incorrect URL.

    For each company, I need TWO distinct URLs:

    1. ATS_URL: The specific URL to their jobs on the third-party ATS platform they use
       For example:
       - Workday: https://company.wd5.myworkdayjobs.com/careers
       - Lever: https://jobs.lever.co/company

    2. CAREER_URL: Direct URL to their own company careers/jobs page as a fallback

    Return your results as a valid JSON array with this exact structure:
    [
      {{
        "company_name": "Example Inc.",
        "ats_url": "https://jobs.lever.co/exampleinc",
        "career_url": "https://example.com/careers"
      }},
      {{
        "company_name": "Another Company",
        "ats_url": null,
        "career_url": "https://anothercompany.com/jobs"
      }}
    ]

    Use null for any URL you cannot find with high confidence.

    Companies to process:
    {company_list_json}
    """

    max_retries = 2
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            with gemini.get_model(context) as model:
                context.log.info(f"Sending batch of {len(companies)} non-Greenhouse companies to Gemini")
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time
                context.log.info(f"Received response from Gemini in {elapsed_time:.2f} seconds")

                # Parse response and extract URLs
                return parse_gemini_response(context, response.text, companies)

        except Exception as e:
            context.log.error(f"Error processing non-Greenhouse companies (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    # If all retries failed
    raise ValueError("Failed to process non-Greenhouse companies after multiple attempts")

def parse_gemini_response(
    context: AssetExecutionContext,
    response_text: str,
    companies: List[Dict]
) -> List[Dict]:
    """Parse the Gemini response to extract URLs."""
    try:
        # Try to find JSON array in the response
        json_start = response_text.find('[')
        json_end = response_text.rfind(']') + 1

        if json_start >= 0 and json_end > json_start:
            json_text = response_text[json_start:json_end]

            # Clean malformed JSON - handle Gemini adding explanatory text inside JSON objects
            # Find all objects and clean them individually
            cleaned_objects = []
            depth = 0
            current_obj = ""
            in_quotes = False
            escape_next = False

            for char in json_text:
                if escape_next:
                    current_obj += char
                    escape_next = False
                    continue

                if char == '\\':
                    current_obj += char
                    escape_next = True
                    continue

                if char == '"' and not escape_next:
                    in_quotes = not in_quotes

                if not in_quotes:
                    if char == '{':
                        depth += 1
                    elif char == '}':
                        depth -= 1
                        # When we find closing brace, clean the object to ensure valid JSON
                        if depth == 0:
                            # Extract just the valid JSON fields using regex
                            matches = re.findall(r'"([^"]+)"\s*:\s*("([^"]|\\"|\\\\)*"|null|true|false|\d+)', current_obj)
                            clean_obj = "{"
                            for i, match in enumerate(matches):
                                field_name = match[0]
                                field_value = match[1]
                                clean_obj += f'"{field_name}":{field_value}'
                                if i < len(matches) - 1:
                                    clean_obj += ","
                            clean_obj += "}"
                            cleaned_objects.append(clean_obj)
                            current_obj = ""
                            continue

                current_obj += char

            # Reassemble the cleaned objects into a JSON array
            cleaned_json = "[" + ",".join(cleaned_objects) + "]"

            try:
                url_data = json.loads(cleaned_json)

                # Map results back to companies
                results = []
                for company in companies:
                    result = {
                        "company_name": company["company_name"],
                        "company_industry": company["company_industry"],
                        "platform": "smartrecruiters"  # Ensure platform is set explicitly
                    }

                    # Find matching result from Gemini
                    found = False
                    for item in url_data:
                        if item.get("company_name", "").strip() == company["company_name"].strip():
                            result["ats_url"] = item.get("ats_url")
                            result["career_url"] = item.get("career_url")
                            found = True
                            break

                    if not found:
                        # If no match, initialize with None values
                        result["ats_url"] = None
                        result["career_url"] = None

                    results.append(result)

                return results
            except json.JSONDecodeError as e:
                context.log.error(f"Failed to parse cleaned JSON: {e}")
                raise

        else:
            context.log.error("No JSON array found in the response")
            raise ValueError("No JSON array found in the response")

    except Exception as e:
        context.log.error(f"Failed to parse response: {str(e)}")
        context.log.error(f"Raw response: {response_text}")

        # Fallback: extract data using regex patterns
        results = []
        for company in companies:
            result = {
                "company_name": company["company_name"],
                "company_industry": company["company_industry"],
                "platform": "smartrecruiters"  # Ensure platform is set explicitly
            }

            # Try to find URLs in the text for this company
            company_name_pattern = re.escape(company["company_name"])
            company_section = re.search(
                rf'{company_name_pattern}.*?(?="company_name"|$)',
                response_text,
                re.DOTALL
            )

            if company_section:
                # Extract ATS URL
                ats_match = re.search(r'"ats_url":\s*"(https:[^"]+)"', company_section.group(0))
                result["ats_url"] = ats_match.group(1) if ats_match else None

                # Extract career URL
                career_match = re.search(r'"career_url":\s*"(https:[^"]+)"', company_section.group(0))
                result["career_url"] = career_match.group(1) if career_match else None
            else:
                result["ats_url"] = None
                result["career_url"] = None

            results.append(result)

        return results

def find_company_urls_individual(
    context: AssetExecutionContext,
    gemini: GeminiResource,
    company_name: str,
    company_industry: str,
    platform: str,
    max_retries: int = 2,
    retry_delay: int = 2
) -> Dict[str, Optional[str]]:
    """
    Use Gemini API to find both ATS job board URL and career URL for a single company.

    Returns a dictionary with both URL types.
    """

    # Different prompt based on the platform
    if platform.lower() == "greenhouse":
        prompt = f"""
        Find accurate job board URLs for this company that uses Greenhouse as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: Greenhouse job board URLs follow one of these specific patterns:
        - https://job-boards.greenhouse.io/[tenant]/jobs/
        - https://boards.greenhouse.io/[tenant]/jobs/
        - https://boards.greenhouse.io/embed/job_board?for=[tenant]

        For example:
        - Instacart: https://boards.greenhouse.io/embed/job_board?for=Instacart
        - Dropbox: https://boards.greenhouse.io/embed/job_board?for=dropbox
        - Other companies use direct tenant URLs like: https://boards.greenhouse.io/company/jobs/

        The key is to identify the correct tenant ID in the URL, which is usually
        a simplified version of the company name (lowercase, no spaces).

        Some companies integrate Greenhouse into their own career sites, so also check
        for embedded Greenhouse job boards on company career pages.

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Greenhouse job board URL (following one of the patterns above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://boards.greenhouse.io/companyname/jobs/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "workday":
        prompt = f"""
        Find accurate job board URLs for this company that uses Workday as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: Workday job board URLs follow this specific pattern:
        https://[tenant].wd1.myworkdayjobs.com/[locale]/[site-name]

        For example:
        - CoStar: https://costar.wd1.myworkdayjobs.com/en-US/CoStarCareers
        - TBK Bank: https://tbkbank.wd1.myworkdayjobs.com/en-US/tpay
        - Go Limitless: https://golimitless.wd1.myworkdayjobs.com/en-US/Staff_External_career_site

        The key is to identify the correct tenant name (usually derived from the company name)
        and the site name (often a variation of the company name or careers).

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Workday job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://tenant.wd1.myworkdayjobs.com/en-US/SiteName",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "bamboohr":
        prompt = f"""
        Find accurate job board URLs for this company that uses BambooHR as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: BambooHR job board URLs follow this specific pattern:
        https://[tenant].bamboohr.com/careers

        For example:
        - Example Company: https://examplecompany.bamboohr.com/careers
        - Tech Startup: https://techstartup.bamboohr.com/careers

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (lowercase, no spaces, no special characters).

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific BambooHR job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://tenant.bamboohr.com/careers",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "icims":
        prompt = f"""
        Find accurate job board URLs for this company that uses iCIMS as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: iCIMS job board URLs follow this specific pattern:
        https://[tenant].icims.com/jobs/

        For example:
        - Example Company: https://examplecompany.icims.com/jobs/
        - Tech Company: https://techcompany.icims.com/jobs/

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (lowercase, no spaces, no special characters).

        Some companies may use:
        - Careers.[company].com that redirects to an iCIMS portal
        - jobs.[company].com that uses iCIMS integration
        - Variations like careers-icims.[company].com

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific iCIMS job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://tenant.icims.com/jobs/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "jobvite":
        prompt = f"""
        Find accurate job board URLs for this company that uses Jobvite as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: Jobvite job board URLs follow this specific pattern:
        https://jobs.jobvite.com/[tenant]/

        For example:
        - Example Company: https://jobs.jobvite.com/examplecompany/
        - Tech Startup: https://jobs.jobvite.com/techstartup/

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (lowercase, no spaces, no special characters).

        Some companies may use:
        - Careers.[company].com that redirects to a Jobvite portal
        - jobs.[company].com that integrates with Jobvite
        - Company websites with embedded Jobvite job boards

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Jobvite job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://jobs.jobvite.com/tenant/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "lever":
        prompt = f"""
        Find accurate job board URLs for this company that uses Lever as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: Lever job board URLs follow this specific pattern:
        https://jobs.lever.co/[tenant]/

        For example:
        - Example Company: https://jobs.lever.co/examplecompany/
        - Tech Startup: https://jobs.lever.co/techstartup/

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (lowercase, no spaces, no special characters).

        Some companies may use:
        - Careers.[company].com that redirects to a Lever portal
        - jobs.[company].com that integrates with Lever
        - Company websites with embedded Lever job boards

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific Lever job board URL (following the pattern above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://jobs.lever.co/tenant/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    elif platform.lower() == "smartrecruiters":
        prompt = f"""
        Find accurate job board URLs for this company that uses SmartRecruiters as its ATS.

        Company Name: "{company_name}"
        Industry: {company_industry}

        IMPORTANT: SmartRecruiters job board URLs follow one of these specific patterns:
        - https://jobs.smartrecruiters.com/[tenant]/
        - https://careers.smartrecruiters.com/[tenant]/

        For example:
        - Example Company: https://jobs.smartrecruiters.com/ExampleCompany/
        - Tech Startup: https://careers.smartrecruiters.com/TechStartup/

        The key is to identify the correct tenant name, which is usually a simplified version
        of the company name (may include capitalization, and may not have spaces).

        Some companies may use:
        - Careers.[company].com that redirects to a SmartRecruiters portal
        - jobs.[company].com that integrates with SmartRecruiters
        - Company websites with embedded SmartRecruiters job boards

        DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.

        I need TWO distinct URLs:
        1. The specific SmartRecruiters job board URL (following one of the patterns above)
        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://jobs.smartrecruiters.com/Tenant/",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """
    else:
        prompt = f"""
        Find accurate job board URLs for this company.

        Company Name: "{company_name}"
        Industry: {company_industry}
        ATS Platform Used: {platform.upper()}

        IMPORTANT: DO NOT guess or invent URLs. Only return a URL if you are CERTAIN it exists.
        It's better to return null than an incorrect URL.

        I need TWO distinct URLs:

        1. The specific URL to their jobs on the {platform.upper()} platform
           For example:
           - Workday: https://company.wd5.myworkdayjobs.com/careers
           - Lever: https://jobs.lever.co/company
           - BambooHR: https://company.bamboohr.com/careers
           - Greenhouse: https://boards.greenhouse.io/company/jobs/
           - iCIMS: https://tenant.icims.com/jobs/
           - Jobvite: https://jobs.jobvite.com/tenant/
           - SmartRecruiters: https://jobs.smartrecruiters.com/Tenant/

        2. Direct URL to their own company careers/jobs page

        Return your answer as a valid JSON object with this exact format:
        {{
          "ats_url": "https://job-boards.platform.io/companyname",
          "career_url": "https://company.com/careers"
        }}

        Use null for any URL you cannot find with high confidence.
        """

    for attempt in range(max_retries):
        try:
            with gemini.get_model(context) as model:
                response = model.generate_content(prompt)
                response_text = response.text.strip()

                try:
                    # Try to find and extract JSON from the response
                    json_start = response_text.find('{')
                    json_end = response_text.rfind('}') + 1

                    if json_start != -1 and json_end > json_start:
                        json_str = response_text[json_start:json_end]
                        data = json.loads(json_str)

                        # Normalize URLs
                        ats_url = data.get("ats_url")
                        career_url = data.get("career_url")

                        if ats_url and ats_url != "null" and not isinstance(ats_url, bool) and not ats_url.startswith(("http://", "https://")):
                            ats_url = "https://" + ats_url

                        if career_url and career_url != "null" and not isinstance(career_url, bool) and not career_url.startswith(("http://", "https://")):
                            career_url = "https://" + career_url

                        # Replace "null" string with None
                        if ats_url == "null" or ats_url is None or isinstance(ats_url, bool):
                            ats_url = None

                        if career_url == "null" or career_url is None or isinstance(career_url, bool):
                            career_url = None

                        return {
                            "ats_url": ats_url,
                            "career_url": career_url
                        }
                    else:
                        raise ValueError("Could not find JSON object in response")

                except (json.JSONDecodeError, ValueError) as e:
                    if attempt == max_retries - 1:
                        context.log.error(f"Failed to parse response for {company_name}: {str(e)}")
                        context.log.error(f"Response: {response_text}")
                        return {"ats_url": None, "career_url": None}

        except Exception as e:
            context.log.error(f"Error on attempt {attempt+1} for {company_name}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return {"ats_url": None, "career_url": None}

    return {"ats_url": None, "career_url": None}

