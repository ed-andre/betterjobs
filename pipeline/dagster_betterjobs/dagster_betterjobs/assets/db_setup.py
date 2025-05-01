import os
from pathlib import Path
from dagster import asset, AssetExecutionContext, get_dagster_logger

logger = get_dagster_logger()

@asset(
    group_name="database",
    compute_kind="database",
    required_resource_keys={"duckdb_resource"},
)
def initialize_db(context: AssetExecutionContext):
    """
    Initialize the database schema by executing the SQL schema file.
    This asset creates the necessary tables for storing company and job data.
    """
    # Get current working directory and create schema path based on it
    cwd = Path(os.getcwd())
    context.log.info(f"Current working directory: {cwd}")

    # we need to add dagster_betterjobs to the path
    # if we're already in the pipeline/dagster_betterjobs directory
    if cwd.name == "dagster_betterjobs" and "pipeline" in str(cwd):
        schema_file = cwd / "dagster_betterjobs" / "db" / "schema.sql"
    else:
        # Try to find the schema file based on conventional locations
        schema_file = Path("pipeline/dagster_betterjobs/dagster_betterjobs/db/schema.sql")

    context.log.info(f"Looking for schema file at: {schema_file}")

    # Drop existing tables to ensure clean schema
    with context.resources.duckdb_resource.get_connection() as conn:
        # First check if tables exist by querying sqlite_master
        tables = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """).fetchall()

        if tables:
            context.log.info(f"Found all existing tables: {[t[0] for t in tables]}")
            context.log.info("Dropping main existing tables to refresh schema")

            try:
                # Drop tables in correct order (most dependent first)
                conn.execute("DROP TABLE IF EXISTS jobs;")
                conn.execute("DROP TABLE IF EXISTS raw_job_listings;")
                conn.execute("DROP TABLE IF EXISTS companies;")
            except Exception as e:
                # If the above fails, try dropping with CASCADE
                context.log.info(f"Error with standard drop: {str(e)}")
                context.log.info("Trying to drop tables with CASCADE option")

                try:
                    # Use CASCADE to automatically handle dependencies
                    conn.execute("DROP TABLE IF EXISTS companies CASCADE;")
                    conn.execute("DROP TABLE IF EXISTS raw_job_listings CASCADE;")
                    conn.execute("DROP TABLE IF EXISTS jobs CASCADE;")
                except Exception as e2:
                    context.log.error(f"Failed to drop tables: {str(e2)}")
                    context.log.info("Will attempt to create schema anyway")

            context.log.info("Tables dropped or reset for fresh schema")

    if not schema_file.exists():
        # Log more detailed path information for debugging
        context.log.error(f"Schema file not found at: {schema_file}")
        context.log.error(f"Current working directory: {os.getcwd()}")

        # As a fallback, create the schema directly in code
        context.log.info("Creating schema directly from code")

        # Initialize database with hard-coded schema
        with context.resources.duckdb_resource.get_connection() as conn:
            # Companies table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_id INTEGER PRIMARY KEY,
                company_name TEXT NOT NULL,
                company_industry TEXT,
                employee_count_range TEXT,
                city TEXT,
                platform TEXT NOT NULL,
                ats_url TEXT,
                career_url TEXT,
                url_verified BOOLEAN DEFAULT FALSE,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(company_name, platform)
            );
            """)

            # Jobs table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id INTEGER PRIMARY KEY,
                company_id INTEGER NOT NULL,
                job_title TEXT NOT NULL,
                job_description TEXT,
                job_url TEXT NOT NULL,
                location TEXT,
                date_posted TIMESTAMP,
                date_retrieved TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (company_id) REFERENCES companies(company_id)
            );
            """)

            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_platform ON companies(platform);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(company_name);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies(company_industry);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_city ON companies(city);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_verified ON companies(url_verified);")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs(job_title);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_date ON jobs(date_posted);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);")

        context.log.info("Database schema created directly from code")
        return "Database initialized (from code)"

    # If we found the schema file, use it
    context.log.info(f"Found schema file at: {schema_file}")
    with open(schema_file, 'r') as f:
        schema_sql = f.read()

    # Execute schema creation
    context.log.info("Initializing database schema from file")
    with context.resources.duckdb_resource.get_connection() as conn:
        statements = schema_sql.split(';')
        for statement in statements:
            if statement.strip():
                conn.execute(statement)

    context.log.info("Database schema initialized successfully")

    return "Database initialized"