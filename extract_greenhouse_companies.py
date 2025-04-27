import json
import csv
import os

# Path to the JSON file
json_file_path = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/companies_using_greenhouse.json"
# Output CSV file path
csv_file_path = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/greenhouse_companies.csv"

def extract_companies():
    # Read the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Extract company data
    companies = []
    for hit in data['hits']['hits']:
        company_name = hit['_source'].get('name', '')

        # Handle industry - might be a list or might be missing
        industry = ''
        if 'industry' in hit['_source'] and hit['_source']['industry']:
            # Take the first industry if there are multiple
            industry = hit['_source']['industry'][0].get('name', '')

        companies.append({
            'company_name': company_name,
            'company_industry': industry
        })

    # Write to CSV
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['company_name', 'company_industry']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for company in companies:
            writer.writerow(company)

    print(f"Extracted {len(companies)} companies to {csv_file_path}")

if __name__ == "__main__":
    extract_companies()
