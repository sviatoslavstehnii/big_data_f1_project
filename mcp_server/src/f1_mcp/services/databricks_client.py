"""Databricks SDK wrapper for SQL execution and schema discovery."""

from typing import Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    StatementState,
    ExecuteStatementRequestOnWaitTimeout,
)

from f1_mcp.config import Settings, get_settings


class DatabricksClient:
    """Client for interacting with Databricks SQL warehouse."""

    def __init__(self, settings: Optional[Settings] = None):
        """Initialize the Databricks client.

        Args:
            settings: Application settings. If None, loads from environment.
        """
        self._settings = settings or get_settings()
        self._client: Optional[WorkspaceClient] = None

    @property
    def client(self) -> WorkspaceClient:
        """Lazy-load the WorkspaceClient."""
        if self._client is None:
            self._client = WorkspaceClient(
                host=self._settings.databricks_host,
                token=self._settings.databricks_token,
            )
        return self._client

    def execute_query(
        self,
        query: str,
        max_rows: Optional[int] = None,
    ) -> dict[str, Any]:
        """Execute a SQL query and return results.

        Args:
            query: SQL query to execute.
            max_rows: Maximum rows to return. Defaults to settings value.

        Returns:
            Dictionary with columns, rows, row_count, and success status.
        """
        max_rows = max_rows or self._settings.max_result_rows

        try:
            # wait_timeout must be 0 (disabled) or between 5-50 seconds
            # Use 50s max and let the SDK handle longer queries asynchronously
            wait_timeout = min(self._settings.query_timeout_seconds, 50)
            wait_timeout = max(wait_timeout, 5)  # At least 5 seconds
            
            statement = self.client.statement_execution.execute_statement(
                warehouse_id=self._settings.databricks_warehouse_id,
                statement=query,
                wait_timeout=f"{wait_timeout}s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
                row_limit=max_rows,
            )

            if statement.status.state == StatementState.SUCCEEDED:
                columns = []
                rows = []

                if statement.manifest and statement.manifest.schema:
                    columns = [col.name for col in statement.manifest.schema.columns]

                if statement.result and statement.result.data_array:
                    for row_data in statement.result.data_array:
                        rows.append(dict(zip(columns, row_data)))

                return {
                    "success": True,
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                }
            else:
                error_msg = "Query did not succeed"
                if statement.status.error:
                    error_msg = statement.status.error.message
                return {
                    "success": False,
                    "error": error_msg,
                    "state": str(statement.status.state),
                }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

    def list_tables(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> dict[str, Any]:
        """List all tables in the specified catalog and schema.

        Args:
            catalog: Catalog name. Defaults to settings value.
            schema: Schema name. Defaults to settings value.

        Returns:
            Dictionary with table names and metadata.
        """
        catalog = catalog or self._settings.databricks_catalog
        schema = schema or self._settings.databricks_schema

        query = f"""
        SELECT table_name, table_type, comment
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        ORDER BY table_name
        """
        return self.execute_query(query)

    def get_table_schema(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> dict[str, Any]:
        """Get column information for a specific table.

        Args:
            table_name: Name of the table.
            catalog: Catalog name. Defaults to settings value.
            schema: Schema name. Defaults to settings value.

        Returns:
            Dictionary with column names, types, and metadata.
        """
        catalog = catalog or self._settings.databricks_catalog
        schema = schema or self._settings.databricks_schema

        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            comment
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        return self.execute_query(query)

    def get_table_sample(
        self,
        table_name: str,
        limit: int = 5,
    ) -> dict[str, Any]:
        """Get a sample of rows from a table.

        Args:
            table_name: Fully qualified table name or just table name.
            limit: Number of rows to return.

        Returns:
            Dictionary with sample data.
        """
        full_table = self._settings.get_full_table_name(table_name)
        query = f"SELECT * FROM {full_table} LIMIT {limit}"
        return self.execute_query(query)


# Singleton instance
_client_instance: Optional[DatabricksClient] = None


def get_databricks_client() -> DatabricksClient:
    """Get or create the singleton DatabricksClient instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = DatabricksClient()
    return _client_instance

