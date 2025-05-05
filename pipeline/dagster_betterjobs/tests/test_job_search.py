#!/usr/bin/env python

import argparse
import sys
import os
from datetime import datetime
import pandas as pd
from google.cloud import bigquery

# Add the parent directory to the path so we can import the module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dagster_betterjobs.assets.job_search import (
    search_jobs,
    JobSearchConfig,
    calculate_relevance_score,
    generate_results_preview
)

class MockContext:
    """Mock Dagster context for testing."""

    def __init__(self):
        self.log = self
        self.resources = self

        # Initialize BigQuery client
        self.bigquery = bigquery.Client()

    def info(self, message):
        print(f"INFO: {message}")

    def warning(self, message):
        print(f"WARNING: {message}")

    def error(self, message):
        print(f"ERROR: {message}")

    def add_output_metadata(self, metadata):
        print("\n=== Output Metadata ===")
        for key, value in metadata.items():
            if hasattr(value, "value"):
                print(f"{key}: {value.value}")
            else:
                print(f"{key}: {value}")
        print("======================\n")

    def log_event(self, event):
        print(f"EVENT: {event}")

def main():
    parser = argparse.ArgumentParser(description="Test the job search asset")

    # Basic search parameters
    parser.add_argument("--keywords", nargs="+", help="Keywords to search for")
    parser.add_argument("--job-titles", nargs="+", help="Job titles to search for")
    parser.add_argument("--excluded-keywords", nargs="+", help="Keywords to exclude")
    parser.add_argument("--locations", nargs="+", help="Locations to search for")
    parser.add_argument("--remote", choices=["true", "false", "none"], default="none",
                        help="Filter for remote jobs (true=only remote, false=exclude remote, none=don't filter)")

    # Platform selection
    parser.add_argument("--platforms", nargs="+", default=["all"],
                        help="Platforms to search (all, greenhouse, bamboohr)")

    # Date range
    parser.add_argument("--days-back", type=int, default=30, help="Number of days to look back")
    parser.add_argument("--date-from", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="End date (YYYY-MM-DD)")

    # Results control
    parser.add_argument("--max-results", type=int, default=100, help="Maximum results to return")
    parser.add_argument("--min-score", type=float, default=0.1, help="Minimum relevance score (0.0-1.0)")
    parser.add_argument("--output-file", help="Path to save results CSV")

    # Display options
    parser.add_argument("--no-descriptions", action="store_true", help="Exclude job descriptions from output")
    parser.add_argument("--include-raw", action="store_true", help="Include raw data in output")
    parser.add_argument("--full-preview", action="store_true", help="Show full job preview")

    args = parser.parse_args()

    # Convert remote argument to appropriate type
    remote = None
    if args.remote == "true":
        remote = True
    elif args.remote == "false":
        remote = False

    # Create configuration
    config = JobSearchConfig(
        keywords=args.keywords or [],
        job_titles=args.job_titles or [],
        excluded_keywords=args.excluded_keywords or [],
        locations=args.locations or [],
        remote=remote,
        platforms=args.platforms,
        days_back=args.days_back,
        date_from=args.date_from,
        date_to=args.date_to,
        max_results=args.max_results,
        min_match_score=args.min_score,
        output_format="dataframe",
        output_file=args.output_file,
        include_descriptions=not args.no_descriptions,
        include_raw_data=args.include_raw
    )

    # Create mock context
    context = MockContext()

    # Set environment variable for dataset
    if not os.getenv("GCP_DATASET_ID"):
        os.environ["GCP_DATASET_ID"] = "betterjobs"

    print(f"Searching for jobs with the following parameters:")
    print(f"  Keywords: {config.keywords}")
    print(f"  Job Titles: {config.job_titles}")
    print(f"  Locations: {config.locations}")
    print(f"  Remote: {config.remote}")
    print(f"  Platforms: {config.platforms}")
    print(f"  Date Range: {args.days_back} days" if not args.date_from else
          f"  Date Range: {args.date_from} to {args.date_to or 'now'}")
    print(f"  Max Results: {config.max_results}")
    print(f"  Min Score: {config.min_match_score}")
    print()

    try:
        # Run the search
        start_time = datetime.now()
        results = search_jobs(context, config)
        end_time = datetime.now()

        # Print results
        if isinstance(results, pd.DataFrame):
            if len(results) == 0:
                print("No results found.")
            else:
                print(f"Found {len(results)} jobs matching your criteria.")

                # Print preview or full results
                if args.full_preview:
                    pd.set_option('display.max_colwidth', None)
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', 1000)

                    if args.no_descriptions:
                        # Show more columns but no descriptions
                        columns = [col for col in results.columns if col != 'job_description' and col != 'raw_data']
                        print(results[columns])
                    else:
                        # Just show the basics
                        basic_columns = ['platform', 'job_title', 'location', 'posting_date', 'job_url']
                        if 'relevance_score' in results.columns:
                            basic_columns.append('relevance_score')
                        print(results[basic_columns])
                else:
                    # Just print minimal info
                    for i, row in results.iterrows():
                        print(f"{i+1}. [{row.get('platform')}] {row.get('job_title')} - {row.get('location')}")
                        if 'relevance_score' in row:
                            print(f"   Score: {row.get('relevance_score', 0):.2f}")
                        print(f"   URL: {row.get('job_url')}")
                        print()

                # Save to file if specified
                if args.output_file:
                    results.to_csv(args.output_file, index=False)
                    print(f"Results saved to {args.output_file}")

        # Print execution time
        execution_time = (end_time - start_time).total_seconds()
        print(f"\nExecution time: {execution_time:.2f} seconds")

    except Exception as e:
        print(f"Error executing search: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()