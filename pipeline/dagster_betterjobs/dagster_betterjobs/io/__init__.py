"""IO modules for the dagster_betterjobs pipeline."""

from typing import List, Optional, Sequence

import pandas as pd
from dagster import IOManager, InputContext, OutputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_duckdb import DuckDBIOManager

class DuckDBPandasTypeHandler(DbTypeHandler[pd.DataFrame]):
    """Type handler for pandas DataFrames."""

    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: pd.DataFrame, connection
    ):
        """Store the pandas DataFrame in DuckDB."""
        connection.execute(f"create schema if not exists {table_slice.schema};")
        connection.execute(
            f"create table if not exists {table_slice.schema}.{table_slice.table} as select * from obj;"
        )
        if not connection.fetchall():
            # Table already exists, insert data
            connection.execute(
                f"insert into {table_slice.schema}.{table_slice.table} select * from obj"
            )

        context.add_output_metadata({"row_count": len(obj)})

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection
    ) -> pd.DataFrame:
        """Load the input as a pandas DataFrame."""
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pd.DataFrame()

        select_statement = connection.execute(
            f"SELECT * FROM {table_slice.schema}.{table_slice.table}"
        )
        return select_statement.fetch_df()

    @property
    def supported_types(self):
        return [pd.DataFrame]

class BetterJobsIOManager(DuckDBIOManager):
    """IO Manager for the BetterJobs project using DuckDB for storage."""

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        """Return the list of type handlers supported by this IO manager."""
        return [DuckDBPandasTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[type]:
        """Default type to use when loading data."""
        return pd.DataFrame