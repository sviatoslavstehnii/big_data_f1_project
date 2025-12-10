"""Schema discovery tools for MCP server."""

from typing import Any, Optional

from fastmcp import FastMCP

from f1_mcp.services.databricks_client import get_databricks_client
from f1_mcp.utils.formatters import ResultFormatter
from f1_mcp.config import get_settings


def register_schema_tools(mcp: FastMCP) -> None:
    """Register schema discovery tools with the MCP server.

    Args:
        mcp: The FastMCP server instance.
    """

    @mcp.tool()
    def list_f1_tables(
        include_bronze: bool = False,
        include_silver: bool = True,
        include_gold: bool = True,
    ) -> dict[str, Any]:
        """List available F1 tables in the Databricks catalog.

        Returns a list of tables organized by data layer (bronze, silver, gold).

        Args:
            include_bronze: Include raw bronze layer tables.
            include_silver: Include cleaned silver layer tables.
            include_gold: Include aggregated gold layer tables.

        Returns:
            Dictionary with table names grouped by layer.
        """
        client = get_databricks_client()

        # Build layer filter
        layer_conditions = []
        if include_bronze:
            layer_conditions.append("table_name LIKE 'f1_bronze%'")
        if include_silver:
            layer_conditions.append("table_name LIKE 'f1_silver%'")
        if include_gold:
            layer_conditions.append("table_name LIKE 'f1_gold%'")

        if not layer_conditions:
            return {
                "success": False,
                "error": "At least one layer must be included",
            }

        layer_filter = " OR ".join(layer_conditions)
        settings = get_settings()

        query = f"""
        SELECT 
            table_name,
            table_type,
            comment
        FROM {settings.databricks_catalog}.information_schema.tables
        WHERE table_schema = '{settings.databricks_schema}'
          AND ({layer_filter})
        ORDER BY table_name
        """

        result = client.execute_query(query)

        if result.get("success"):
            # Organize by layer
            tables_by_layer = {
                "bronze": [],
                "silver": [],
                "gold": [],
            }

            for row in result.get("rows", []):
                table_name = row.get("table_name", "")
                if "bronze" in table_name:
                    tables_by_layer["bronze"].append(row)
                elif "silver" in table_name:
                    tables_by_layer["silver"].append(row)
                elif "gold" in table_name:
                    tables_by_layer["gold"].append(row)

            result["tables_by_layer"] = tables_by_layer

        return result

    @mcp.tool()
    def describe_table(table_name: str) -> dict[str, Any]:
        """Get the schema (column definitions) for a specific table.

        Args:
            table_name: Name of the table (e.g., 'f1_gold_driver_season_stats').

        Returns:
            Dictionary with column names, data types, and descriptions.
        """
        client = get_databricks_client()
        settings = get_settings()

        # Clean table name (remove catalog/schema prefix if present)
        clean_name = table_name.split(".")[-1]

        result = client.get_table_schema(clean_name)

        if result.get("success"):
            result["table_name"] = clean_name
            result["full_table_name"] = f"{settings.databricks_catalog}.{settings.databricks_schema}.{clean_name}"
            result["formatted"] = ResultFormatter.format_table_schema(clean_name, result)

        return result

    @mcp.tool()
    def get_table_sample(
        table_name: str,
        limit: int = 5,
    ) -> dict[str, Any]:
        """Get a sample of rows from a table.

        Useful for understanding the data structure and content.

        Args:
            table_name: Name of the table.
            limit: Number of sample rows to return (max 20).

        Returns:
            Sample rows from the table.
        """
        # Limit max sample size
        limit = min(limit, 20)

        client = get_databricks_client()
        return client.get_table_sample(table_name, limit)

    @mcp.tool()
    def get_f1_data_overview() -> dict[str, Any]:
        """Get an overview of the F1 data available.

        Returns summary statistics and descriptions of the main tables.

        Returns:
            Overview of available F1 data including row counts.
        """
        client = get_databricks_client()

        # Get counts for main gold tables
        overview_query = """
        SELECT 
            'driver_season_stats' as table_name,
            COUNT(*) as row_count,
            MIN(season) as min_season,
            MAX(season) as max_season
        FROM f1.f1_gold_driver_season_stats

        UNION ALL

        SELECT 
            'constructor_season_stats' as table_name,
            COUNT(*) as row_count,
            MIN(season) as min_season,
            MAX(season) as max_season
        FROM f1.f1_gold_constructor_season_stats

        UNION ALL

        SELECT 
            'race_driver_features' as table_name,
            COUNT(*) as row_count,
            MIN(season) as min_season,
            MAX(season) as max_season
        FROM f1.f1_gold_race_driver_features
        """

        result = client.execute_query(overview_query)

        if result.get("success"):
            result["description"] = {
                "driver_season_stats": (
                    "Aggregated driver performance by season: wins, podiums, "
                    "points, championship position, average grid/finish positions."
                ),
                "constructor_season_stats": (
                    "Aggregated team/constructor performance by season: "
                    "total points, wins, podiums, championship standings."
                ),
                "race_driver_features": (
                    "Detailed race-level features for each driver: grid position, "
                    "finish position, pit stops, lap times, qualifying data. "
                    "Best for ML model training and detailed analysis."
                ),
            }
            result["recommended_tables"] = [
                {
                    "name": "f1.f1_gold_driver_season_stats",
                    "use_case": "Driver performance analysis, championship trends",
                },
                {
                    "name": "f1.f1_gold_constructor_season_stats",
                    "use_case": "Team performance analysis, constructor comparisons",
                },
                {
                    "name": "f1.f1_gold_race_driver_features",
                    "use_case": "Race predictions, pit stop analysis, detailed insights",
                },
            ]

        return result

