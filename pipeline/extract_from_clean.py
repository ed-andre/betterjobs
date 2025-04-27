import json
import csv
import os
import glob
from pathlib import Path

# Path to cleaned JSON files and output file
clean_dir = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/cleaned_json"
output_csv = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/lever_companies.csv"

def extract_companies():
    """Extract companies from cleaned JSON files and create a combined CSV."""
    # Dictionary to store unique companies
    companies = {}

    # Get all cleaned JSON files
    clean_files = glob.glob(os.path.join(clean_dir, "clean_*.json"))

    # Process each clean JSON file
    for file_path in clean_files:
        print(f"Processing {os.path.basename(file_path)}...")

        try:
            # Load the JSON file
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                # Process each company
                for company in data:
                    company_name = company.get('company_name', '').strip()
                    if company_name:
                        # Add to dictionary (overwrites if company already exists)
                        companies[company_name] = company.get('company_industry', '')

        except Exception as e:
            print(f"  Error processing {file_path}: {str(e)}")

    # Write combined data to output CSV
    with open(output_csv, 'w', encoding='utf-8', newline='') as outfile:
        fieldnames = ['company_name', 'company_industry']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        writer.writeheader()
        for company_name, industry in companies.items():
            writer.writerow({
                'company_name': company_name,
                'company_industry': industry
            })

    print(f"Combined {len(companies)} unique companies into {output_csv}")
    return len(companies)

if __name__ == "__main__":
    # Ensure output directory exists
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    extract_companies()