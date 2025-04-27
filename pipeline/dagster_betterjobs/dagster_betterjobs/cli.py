import os
import argparse
import pandas as pd
from pathlib import Path
import duckdb
from datetime import datetime

def main():
    """Main CLI entry point for BetterJobs."""

    parser = argparse.ArgumentParser(description="BetterJobs CLI tool for job search")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Search command
    search_parser = subparsers.add_parser("search", help="Search for jobs")
    search_parser.add_argument("keyword", help="Keyword to search for")
    search_parser.add_argument("--location", help="Optional location filter")
    search_parser.add_argument("--format", choices=["csv", "json", "html", "markdown"], default="csv",
                              help="Output format (default: csv)")
    search_parser.add_argument("--output", help="Output file path (default: stdout)")

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show job statistics")
    stats_parser.add_argument("--platform", help="Filter by platform")
    stats_parser.add_argument("--format", choices=["csv", "json", "html", "markdown"], default="markdown",
                             help="Output format (default: markdown)")
    stats_parser.add_argument("--output", help="Output file path (default: stdout)")

    # Companies command
    companies_parser = subparsers.add_parser("companies", help="List companies")
    companies_parser.add_argument("--platform", help="Filter by platform")
    companies_parser.add_argument("--has-url", action="store_true", help="Only show companies with URLs")
    companies_parser.add_argument("--format", choices=["csv", "json", "html", "markdown"], default="csv",
                               help="Output format (default: csv)")
    companies_parser.add_argument("--output", help="Output file path (default: stdout)")

    args = parser.parse_args()

    # Connect to the database
    db_path = Path("pipeline/dagster_betterjobs/dagster_betterjobs/db/betterjobs.db")
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return 1

    conn = duckdb.connect(str(db_path))

    # Execute the appropriate command
    if args.command == "search":
        search_jobs(conn, args)
    elif args.command == "stats":
        show_stats(conn, args)
    elif args.command == "companies":
        list_companies(conn, args)
    else:
        parser.print_help()

    return 0

def search_jobs(conn, args):
    """Search for jobs matching the keyword."""

    search_pattern = f"%{args.keyword.lower()}%"
    location_filter = "" if not args.location else f"AND LOWER(j.location) LIKE '%{args.location.lower()}%'"

    query = f"""
    SELECT
        j.job_id,
        c.company_name,
        c.company_industry,
        c.platform,
        j.job_title,
        j.location,
        j.job_url,
        j.date_posted,
        j.date_retrieved
    FROM
        jobs j
    JOIN
        companies c ON j.company_id = c.company_id
    WHERE
        (LOWER(j.job_title) LIKE ? OR LOWER(j.job_description) LIKE ?)
        {location_filter}
    ORDER BY
        j.date_retrieved DESC,
        c.company_name
    """

    results = conn.execute(query, [search_pattern, search_pattern]).fetch_df()

    # Generate output
    print(f"Found {len(results)} jobs matching '{args.keyword}'")

    if len(results) > 0:
        output_results(results, args.format, args.output)

def show_stats(conn, args):
    """Show job statistics."""

    platform_filter = "" if not args.platform else f"WHERE LOWER(platform) = '{args.platform.lower()}'"

    # Get company stats
    company_query = f"""
    SELECT
        platform,
        COUNT(*) as total_companies,
        SUM(CASE WHEN career_url IS NOT NULL AND career_url != 'Not found' THEN 1 ELSE 0 END) as companies_with_url
    FROM
        companies
    {platform_filter}
    GROUP BY
        platform
    ORDER BY
        platform
    """

    company_stats = conn.execute(company_query).fetch_df()

    # Get job stats
    job_query = f"""
    SELECT
        c.platform,
        COUNT(DISTINCT c.company_id) as companies_with_jobs,
        COUNT(*) as total_jobs
    FROM
        jobs j
    JOIN
        companies c ON j.company_id = c.company_id
    {platform_filter}
    GROUP BY
        c.platform
    ORDER BY
        c.platform
    """

    job_stats = conn.execute(job_query).fetch_df()

    # Combine the results
    if len(company_stats) > 0:
        if len(job_stats) > 0:
            stats = pd.merge(company_stats, job_stats, on="platform", how="left")
            stats.fillna(0, inplace=True)
            stats[["companies_with_jobs", "total_jobs"]] = stats[["companies_with_jobs", "total_jobs"]].astype(int)
        else:
            stats = company_stats
            stats["companies_with_jobs"] = 0
            stats["total_jobs"] = 0
    else:
        print("No statistics available")
        return

    # Add a total row
    total_row = pd.DataFrame([{
        "platform": "TOTAL",
        "total_companies": stats["total_companies"].sum(),
        "companies_with_url": stats["companies_with_url"].sum(),
        "companies_with_jobs": stats["companies_with_jobs"].sum(),
        "total_jobs": stats["total_jobs"].sum()
    }])

    stats = pd.concat([stats, total_row])

    # Generate output
    output_results(stats, args.format, args.output)

def list_companies(conn, args):
    """List companies in the database."""

    filters = []
    if args.platform:
        filters.append(f"LOWER(platform) = '{args.platform.lower()}'")
    if args.has_url:
        filters.append("career_url IS NOT NULL AND career_url != 'Not found'")

    where_clause = " WHERE " + " AND ".join(filters) if filters else ""

    query = f"""
    SELECT
        company_id,
        company_name,
        company_industry,
        platform,
        career_url
    FROM
        companies
    {where_clause}
    ORDER BY
        platform,
        company_name
    """

    results = conn.execute(query).fetch_df()

    # Generate output
    print(f"Found {len(results)} companies")

    if len(results) > 0:
        output_results(results, args.format, args.output)

def output_results(df, format_type, output_path=None):
    """Output the results in the specified format."""

    if output_path:
        # Ensure the directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

    if format_type == "csv":
        if output_path:
            df.to_csv(output_path, index=False)
        else:
            print(df.to_csv(index=False))
    elif format_type == "json":
        if output_path:
            df.to_json(output_path, orient="records", indent=2)
        else:
            print(df.to_json(orient="records", indent=2))
    elif format_type == "html":
        html = df.to_html(index=False)
        if output_path:
            with open(output_path, "w") as f:
                f.write(html)
        else:
            print(html)
    elif format_type == "markdown":
        md = df.to_markdown(index=False)
        if output_path:
            with open(output_path, "w") as f:
                f.write(md)
        else:
            print(md)

if __name__ == "__main__":
    main()