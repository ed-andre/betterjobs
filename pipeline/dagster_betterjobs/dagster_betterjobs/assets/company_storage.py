import pandas as pd
from dagster import asset, AssetExecutionContext, get_dagster_logger

logger = get_dagster_logger()

# @asset(
#     group_name="company_data",
#     compute_kind="database",
#     required_resource_keys={"duckdb_resource"},
#     deps=["company_urls", "initialize_db"]
# )
# def store_company_urls(context: AssetExecutionContext, company_urls: pd.DataFrame):
#     """
#     Stores the company URLs in the DuckDB database.

#     This asset takes the company URLs DataFrame from the company_urls asset
#     and inserts the data into the companies table in the database.
#     """
#     conn = context.resources.duckdb_resource.get_connection()

#     # Prepare data for insertion
#     for _, row in company_urls.iterrows():
#         # Check if company already exists
#         existing = conn.execute(
#             """
#             SELECT company_id FROM companies
#             WHERE company_name = ? AND platform = ?
#             """,
#             (row['company_name'], row['platform'])
#         ).fetchone()

#         if existing:
#             # Update existing company
#             conn.execute(
#                 """
#                 UPDATE companies
#                 SET company_industry = ?,
#                     career_url = ?,
#                     last_updated = CURRENT_TIMESTAMP
#                 WHERE company_name = ? AND platform = ?
#                 """,
#                 (row['company_industry'], row['career_url'], row['company_name'], row['platform'])
#             )
#         else:
#             # Insert new company
#             conn.execute(
#                 """
#                 INSERT INTO companies (company_name, company_industry, platform, career_url)
#                 VALUES (?, ?, ?, ?)
#                 """,
#                 (row['company_name'], row['company_industry'], row['platform'], row['career_url'])
#             )

#     # Get counts for summary
#     total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
#     companies_with_urls = conn.execute(
#         "SELECT COUNT(*) FROM companies WHERE career_url IS NOT NULL"
#     ).fetchone()[0]

#     context.log.info(f"Total companies in database: {total_companies}")
#     context.log.info(f"Companies with URLs: {companies_with_urls}")

#     return {
#         "total_companies": total_companies,
#         "companies_with_urls": companies_with_urls
#     }