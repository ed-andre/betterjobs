import psycopg2
from dagster import ConfigurableResource

class PostgresResource(ConfigurableResource):
    """Resource for connecting to a PostgreSQL database."""
    host: str
    port: int = 5432
    user: str
    password: str
    dbname: str = "postgres"
    sslmode: str = "require"  # Important for Supabase

    def get_connection(self):
        """Return a connection to the PostgreSQL database."""
        # Add connection parameters for Supabase
        connection_params = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": self.dbname,
            "sslmode": self.sslmode
        }

        # If using Supabase's connection pooler, add required options
        if "pooler.supabase" in self.host:
            connection_params["options"] = f"-c search_path=public"

        return psycopg2.connect(**connection_params)