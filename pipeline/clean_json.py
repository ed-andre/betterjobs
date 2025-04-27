import json
import os
import re
from pathlib import Path

# Path to JSON files and output directory
data_load_dir = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/raw_json"
clean_dir = "pipeline/dagster_betterjobs/dagster_betterjobs/data_load/cleaned_json"

# Create output directory if it doesn't exist
os.makedirs(clean_dir, exist_ok=True)

# List of JSON files to process
json_files = [
    "companies_using_lever.json",
    # "companies_using_icims_2.json",
    # "companies_using_jobvite_1x.json",
    # "companies_using_workdays_1x.json",
    # "companies_using_workdays_2.json",
    # "companies_using_workdays_2x.json",
    # "companies_using_workdays_3.json",
    # "companies_using_workdays_3x.json",
    # "companies_using_workdays_4.json",
    # "companies_using_workdays_5.json"
]

def clean_json_file(input_path, output_path):
    """Extract company data using direct regex matching."""
    print(f"Cleaning {input_path}...")

    # Direct pattern to match company_name and enclosing object with US country
    pattern = re.compile(
        r'"country"\s*:\s*"United States".*?"industry"\s*:\s*"([^"]*)".*?"company_name"\s*:\s*"([^"]*)"',
        re.DOTALL
    )

    # Alternative pattern that might match in different order
    alt_pattern = re.compile(
        r'"industry"\s*:\s*"([^"]*)".*?"country"\s*:\s*"United States".*?"company_name"\s*:\s*"([^"]*)"',
        re.DOTALL
    )

    # Dictionary to track unique companies and avoid duplicates
    companies_dict = {}

    # Read the file content in segments to handle very large files
    with open(input_path, 'r', encoding='utf-8', errors='ignore') as file:
        # Read the file in chunks
        chunk_size = 1024 * 1024  # 1MB chunks
        overlap = 1000  # Overlap between chunks to avoid missing matches at boundaries

        file_chunk = ""
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break

            # Add chunk to our search content with overlap
            file_chunk = file_chunk[-overlap:] + chunk

            # Find all matches in this chunk
            for match in pattern.finditer(file_chunk):
                industry = match.group(1)
                company_name = match.group(2)
                if company_name and company_name not in companies_dict:
                    companies_dict[company_name] = industry

            # Also try the alternative pattern
            for match in alt_pattern.finditer(file_chunk):
                industry = match.group(1)
                company_name = match.group(2)
                if company_name and company_name not in companies_dict:
                    companies_dict[company_name] = industry

    # Convert dictionary to list of company objects
    cleaned_companies = [
        {"company_name": name, "company_industry": industry}
        for name, industry in companies_dict.items()
    ]

    # Write the cleaned data to a new JSON file
    with open(output_path, 'w', encoding='utf-8') as outfile:
        json.dump(cleaned_companies, outfile, indent=2)

    print(f"  Extracted {len(cleaned_companies)} companies to {output_path}")
    return len(cleaned_companies)

def main():
    """Process all JSON files and create cleaned versions."""
    total_companies = 0

    for json_file in json_files:
        input_path = os.path.join(data_load_dir, json_file)
        if os.path.exists(input_path):
            output_path = os.path.join(clean_dir, f"clean_{json_file}")
            try:
                count = clean_json_file(input_path, output_path)
                total_companies += count
            except Exception as e:
                print(f"Error processing {json_file}: {str(e)}")
        else:
            print(f"File not found: {input_path}")

    print(f"Total companies extracted: {total_companies}")

if __name__ == "__main__":
    main()