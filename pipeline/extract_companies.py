import json
import os
import csv
import glob
from pathlib import Path
from collections import defaultdict

# Paths
raw_json_dir = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/raw_json"
output_dir = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/datasource"

# Create output directory explicitly
output_path = Path(output_dir)
output_path.mkdir(parents=True, exist_ok=True)
print(f"Output directory: {output_path.absolute()}")

def extract_data_from_json_files():
    """Extract company and job data from raw JSON files."""
    # Store companies by ATS platform
    companies_by_platform = defaultdict(list)
    # Store all jobs
    all_jobs = []
    # Track unique companies to avoid duplicates
    unique_companies = set()

    # Process all JSON files in raw_json directory
    json_files = glob.glob(os.path.join(raw_json_dir, "*.json"))
    print(f"Found {len(json_files)} JSON files to process")

    for file_path in json_files:
        platform = os.path.basename(file_path).split('_')[2].split('.')[0].split('_')[0]
        print(f"Processing {os.path.basename(file_path)} for {platform}...")

        try:
            # Load the JSON file
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                # Debug information about the JSON structure
                print(f"  Keys in JSON file: {', '.join(data.keys())}")

                # Check if the data has a 'results' key (typical structure)
                results_key = None
                if 'results' in data:
                    results_key = 'results'
                elif 'data' in data:
                    results_key = 'data'

                if results_key:
                    print(f"  Found {len(data[results_key])} companies in {results_key} array")

                    for company_data in data[results_key]:
                        # Extract company info
                        company_name = None

                        # Check for technology info
                        if 'technologies_found' in company_data:
                            for tech in company_data.get('technologies_found', []):
                                if 'company_name' in tech:
                                    company_name = tech.get('company_name')
                                    break

                        # If no company name found in technologies, try direct name field
                        if not company_name and 'name' in company_data:
                            company_name = company_data.get('name')

                        # Only process if we have a company name and it's not a duplicate
                        if company_name and company_name not in unique_companies:
                            unique_companies.add(company_name)

                            company_info = {
                                'company_name': company_name,
                                'company_industry': company_data.get('industry', ''),
                                'employee_count_range': company_data.get('employee_count_range', ''),
                                'city': company_data.get('city', ''),
                                'platform': platform
                            }

                            companies_by_platform[platform].append(company_info)

                            # Extract job info if available
                            if 'jobs_found' in company_data:
                                for job in company_data.get('jobs_found', []):
                                    job_title = job.get('job_title', '') or job.get('title', '')
                                    job_info = {
                                        'company_name': company_name,
                                        'job_title': job_title,
                                        'date_posted': job.get('date_posted', '')
                                    }
                                    all_jobs.append(job_info)
                else:
                    print(f"  Could not find 'results' or 'data' key in {file_path}")

        except Exception as e:
            print(f"  Error processing {file_path}: {str(e)}")

    print(f"Total unique companies found: {len(unique_companies)}")
    print(f"Companies by platform: {', '.join([f'{k}: {len(v)}' for k, v in companies_by_platform.items()])}")
    print(f"Total jobs found: {len(all_jobs)}")

    # Write company data by platform
    for platform, companies in companies_by_platform.items():
        if companies:  # Only create file if we have data
            output_file = os.path.join(output_dir, f"{platform}_companies.csv")
            print(f"Writing {len(companies)} companies to {output_file}")
            try:
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['company_name', 'company_industry', 'employee_count_range', 'city', 'platform']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for company in companies:
                        writer.writerow(company)
                print(f"Successfully wrote {platform}_companies.csv")
            except Exception as e:
                print(f"Error writing {output_file}: {str(e)}")

    # Write job data
    if all_jobs:
        output_file = os.path.join(output_dir, "companies_jobs.csv")
        print(f"Writing {len(all_jobs)} jobs to {output_file}")
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['company_name', 'job_title', 'date_posted']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for job in all_jobs:
                    writer.writerow(job)
            print(f"Successfully wrote companies_jobs.csv")
        except Exception as e:
            print(f"Error writing {output_file}: {str(e)}")

if __name__ == "__main__":
    extract_data_from_json_files()