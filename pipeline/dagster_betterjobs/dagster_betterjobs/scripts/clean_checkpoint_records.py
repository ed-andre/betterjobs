"""
Script to clean checkpoint records based on specified conditions.

Usage:
    python clean_checkpoint_records.py --files [file1.csv file2.csv ...] [--platform PLATFORM] [--ats-url-contains STRING]

Examples:
    # Remove records with platform='smartrecruiters' from url_discovery_checkpoint.csv
    python clean_checkpoint_records.py --files url_discovery_checkpoint.csv --platform smartrecruiters

    # Remove records containing 'smartrecruiters.com' in ATS URL from multiple files
    python clean_checkpoint_records.py --files url_discovery_checkpoint.csv lever_url_discovery_checkpoint.csv --ats-url-contains smartrecruiters.com

    # Process multiple files with both conditions
    python clean_checkpoint_records.py --files *.csv --platform smartrecruiters --ats-url-contains smartrecruiters.com

Notes:
    - Creates a .bak backup file before modifying
    - Logs removed records for verification
    - Can process multiple files at once
    - Supports filtering by exact platform match or partial URL match
"""

import os
import pandas as pd
from pathlib import Path
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_checkpoint_dir():
    """Get the checkpoint directory path based on current working directory."""
    cwd = Path(os.getcwd())

    # If running from scripts directory, go up two levels
    if cwd.name == "scripts":
        return cwd.parent / "checkpoints"

    # Original logic for other cases
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        return Path("dagster_betterjobs/checkpoints")
    return Path("pipeline/dagster_betterjobs/dagster_betterjobs/checkpoints")

def clean_checkpoint_records(checkpoint_files, conditions):
    """
    Clean records from checkpoint files based on specified conditions.

    Args:
        checkpoint_files (list): List of checkpoint file names to process
        conditions (dict): Dictionary of column names and values/functions to match
            Format: {
                'column_name': value | callable,
                ...
            }
            If value is callable, it will be used as a filter function on the column

    Example:
        clean_checkpoint_records(
            ['url_discovery_checkpoint.csv'],
            {'platform': 'smartrecruiters'}
        )

        # With a filter function
        clean_checkpoint_records(
            ['url_discovery_checkpoint.csv'],
            {'ats_url': lambda x: 'smartrecruiters.com' in str(x).lower()}
        )
    """
    checkpoint_dir = get_checkpoint_dir()

    for file_name in checkpoint_files:
        file_path = checkpoint_dir / file_name
        if not file_path.exists():
            logging.warning(f"Checkpoint file not found: {file_path}")
            continue

        try:
            # Read the checkpoint file
            df = pd.read_csv(file_path)
            original_count = len(df)

            # Create backup
            backup_path = file_path.with_suffix('.csv.bak')
            df.to_csv(backup_path, index=False)
            logging.info(f"Created backup at {backup_path}")

            # Apply each condition
            for column, condition in conditions.items():
                if column not in df.columns:
                    logging.warning(f"Column '{column}' not found in {file_name}")
                    continue

                if callable(condition):
                    # Apply filter function
                    mask = df[column].apply(condition)
                    removed_records = df[mask]
                    df = df[~mask]
                else:
                    # Direct value comparison
                    removed_records = df[df[column] == condition]
                    df = df[df[column] != condition]

                if not removed_records.empty:
                    logging.info(f"Removed {len(removed_records)} records from {file_name} matching condition: {column}")
                    # Log removed records for verification
                    for _, record in removed_records.iterrows():
                        logging.info(f"Removed record: {record.to_dict()}")

            # Save cleaned data
            df.to_csv(file_path, index=False)
            logging.info(f"Processed {file_name}: {original_count - len(df)} records removed, {len(df)} records remaining")

        except Exception as e:
            logging.error(f"Error processing {file_name}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Clean checkpoint records based on conditions')
    parser.add_argument('--files', nargs='+', help='Checkpoint files to process', required=True)
    parser.add_argument('--platform', help='Filter by platform')
    parser.add_argument('--ats-url-contains', help='Filter where ATS URL contains string')
    args = parser.parse_args()

    conditions = {}
    if args.platform:
        conditions['platform'] = args.platform
    if args.ats_url_contains:
        conditions['ats_url'] = lambda x: args.ats_url_contains.lower() in str(x).lower()

    if not conditions:
        logging.error("No conditions specified")
        return

    clean_checkpoint_records(args.files, conditions)

if __name__ == "__main__":
    main()