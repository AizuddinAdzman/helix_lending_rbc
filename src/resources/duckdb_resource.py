"""
resources/duckdb_resource.py
-----------------------------
Shared DuckDB connection as a Dagster resource.

All assets use this resource rather than opening their own connections.
This ensures:
  - Single connection per pipeline run (DuckDB is single-writer)
  - Consistent DB path from config
  - Clean teardown after each run
"""

import duckdb
import logging
from dagster import ConfigurableResource, InitResourceContext
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


class DuckDBResource(ConfigurableResource):
    """
    Dagster resource wrapping a DuckDB connection.

    Usage in an asset:
        @asset
        def my_asset(duckdb_resource: DuckDBResource):
            with duckdb_resource.get_connection() as conn:
                conn.execute("SELECT ...")
    """
    db_path: str

    @contextmanager
    def get_connection(self):
        """
        Yield a DuckDB connection, ensuring it is closed after use.
        Creates the output directory if it does not exist.
        """
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Opening DuckDB connection → {self.db_path}")
        conn = duckdb.connect(self.db_path)
        try:
            # Enable progress bar for long-running queries (local dev only)
            conn.execute("PRAGMA enable_progress_bar")
            yield conn
        except Exception as e:
            logger.error(f"DuckDB connection error: {e}")
            raise
        finally:
            conn.close()
            logger.debug(f"DuckDB connection closed → {self.db_path}")
