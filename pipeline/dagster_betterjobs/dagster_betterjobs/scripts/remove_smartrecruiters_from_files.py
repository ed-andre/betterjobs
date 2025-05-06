"""
# This script removes smartrecruiters entries from a CSV file.
# It reads the input file, checks for smartrecruiters entries, and writes the results to a temporary file.
# If changes are made, it replaces the original file with the temporary file.
# To remove for all files: python "pipeline/dagster_betterjobs/dagster_betterjobs/scripts/remove_smartrecruiters_from_files.py" pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints/*_url_discovery_checkpoint.csv
"""

import csv
import os
import sys
from pathlib import Path

def remove_smartrecruiters_from_file(file_path):
    file_path = Path(file_path)

    if not file_path.exists():
        print(f"File not found: {file_path}")
        return 0

    temp_file = file_path.with_suffix('.tmp')
    removed_count = 0

    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as infile, \
             open(temp_file, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                if row.get('platform') == 'smartrecruiters':
                    removed_count += 1
                    print(f"Removing smartrecruiters entry: {row.get('company_name', 'Unknown')} - {row.get('ats_url', 'No URL')}")
                else:
                    writer.writerow(row)

        # Replace original file with temp file if changes were made
        if removed_count > 0:
            os.replace(temp_file, file_path)
            print(f"Updated {file_path.name}, removed {removed_count} entries")
        else:
            os.unlink(temp_file)
            print(f"No smartrecruiters entries found in {file_path.name}")

        return removed_count

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        if temp_file.exists():
            os.unlink(temp_file)
        return 0

def main():
    if len(sys.argv) < 2:
        print("Usage: python remove_smartrecruiters_from_files.py <file1> [file2] ...")
        return

    total_removed = 0

    for file_path in sys.argv[1:]:
        print(f"\nProcessing {file_path}...")
        removed = remove_smartrecruiters_from_file(file_path)
        total_removed += removed

    print(f"\nSummary: Processed {len(sys.argv)-1} files, removed {total_removed} smartrecruiters entries")

if __name__ == "__main__":
    main()