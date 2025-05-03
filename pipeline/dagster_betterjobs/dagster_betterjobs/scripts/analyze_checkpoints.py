import csv
import os
from pathlib import Path

def main():
    """Analyze checkpoint files for SmartRecruiters platform entries."""
    # Path to checkpoints directory
    script_dir = Path(__file__).parent
    checkpoints_dir = script_dir.parent / "checkpoints"

    # Find all CSV files
    csv_files = list(checkpoints_dir.glob("*_url_discovery_checkpoint.csv"))
    print(f"Found {len(csv_files)} checkpoint files")

    for csv_file in csv_files:
        print(f"\nAnalyzing {csv_file.name}...")

        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Find SmartRecruiters entries
            smart_entries = []
            for row in reader:
                if row.get('platform') == 'smartrecruiters':
                    smart_entries.append(row)

            print(f"  Found {len(smart_entries)} SmartRecruiters entries")

            # Print the first 10 entries with their URLs
            if smart_entries:
                print("  Sample entries:")
                for entry in smart_entries[:10]:
                    print(f"    {entry.get('company_name')} - Platform: {entry.get('platform')} - URL: {entry.get('ats_url')}")

if __name__ == "__main__":
    main()