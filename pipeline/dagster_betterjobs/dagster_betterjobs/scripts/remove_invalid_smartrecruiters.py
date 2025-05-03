import csv
import os
import re
import sys
from pathlib import Path

def main():
    """Main function to process all CSV files in the checkpoints directory."""
    try:
        # Print Python version and current directory for debugging
        print(f"Python version: {sys.version}")
        print(f"Current directory: {os.getcwd()}")

        # Path to checkpoints directory
        script_dir = Path(__file__).parent
        checkpoints_dir = script_dir.parent / "checkpoints"
        print(f"Script directory: {script_dir}")
        print(f"Checkpoints directory: {checkpoints_dir}")
        print(f"Checkpoints directory exists: {checkpoints_dir.exists()}")

        # Find all CSV files
        csv_files = list(checkpoints_dir.glob("url_discovery_checkpoint.csv"))
        print(f"Found {len(csv_files)} CSV files:")
        for csv_file in csv_files:
            print(f"  - {csv_file}")

        if not csv_files:
            print(f"No checkpoint CSV files found in {checkpoints_dir}")
            return

        total_removed = 0

        for csv_file in csv_files:
            print(f"\nProcessing {csv_file.name}...")
            if not csv_file.exists():
                print(f"File not found: {csv_file}")
                continue

            temp_file = csv_file.with_suffix('.tmp')
            removed_count = 0

            with open(csv_file, 'r', newline='', encoding='utf-8') as infile, \
                 open(temp_file, 'w', newline='', encoding='utf-8') as outfile:

                reader = csv.DictReader(infile)
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()

                for row in reader:
                    # Check if the row has a platform of "smartrecruiters" and ats_url
                    is_invalid = False

                    if row.get('platform') == 'smartrecruiters':
                        # If platform is SmartRecruiters, check for specific invalid URL patterns
                        ats_url = row.get('ats_url', '')

                        # Check for the specific pattern from the request
                        if ats_url == 'https://jobs.smartrecruiters.com/?company=[tenant]':
                            is_invalid = True
                            print(f"Removing due to exact match: {row.get('company_name')} - {ats_url}")

                        # Check for URLs with ?company= param that might be invalid
                        elif '?company=' in ats_url and (
                            # URLs with square brackets
                            ('[' in ats_url and ']' in ats_url) or
                            # URLs with template placeholders
                            'tenant' in ats_url.lower() or
                            # Generic URLs without specific company
                            (ats_url.startswith('https://jobs.smartrecruiters.com/?company=') and
                             len(ats_url.split('?company=')[1]) < 3)
                        ):
                            is_invalid = True
                            print(f"Removing due to pattern match: {row.get('company_name')} - {ats_url}")

                        # Check for possible placeholder URLs
                        elif (ats_url.startswith('https://careers.smartrecruiters.com/') or
                            ats_url.startswith('https://jobs.smartrecruiters.com/')) and (
                            # Empty company name after the domain
                            ats_url.endswith('/') or
                            # Has placeholders
                            'tenant' in ats_url.lower() or
                            '[' in ats_url or
                            ']' in ats_url
                        ):
                            is_invalid = True
                            print(f"Removing due to placeholder URL: {row.get('company_name')} - {ats_url}")

                    if not is_invalid:
                        writer.writerow(row)
                    else:
                        removed_count += 1

            # Replace the original file with the temporary file
            if removed_count > 0:
                os.replace(temp_file, csv_file)
                print(f"Updated {csv_file.name}, removed {removed_count} entries")
            else:
                # If no changes, delete the temporary file
                os.unlink(temp_file)
                print(f"No invalid URLs found in {csv_file.name}")

            total_removed += removed_count

        print(f"\nSummary: Processed {len(csv_files)} files, removed {total_removed} invalid SmartRecruiters URLs")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()