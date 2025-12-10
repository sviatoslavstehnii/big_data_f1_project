"""SQL query execution tools for MCP server."""

from typing import Any, Optional

from fastmcp import FastMCP

from f1_mcp.services.databricks_client import get_databricks_client
from f1_mcp.utils.validators import get_sql_validator
from f1_mcp.utils.formatters import ResultFormatter


def register_sql_tools(mcp: FastMCP) -> None:
    @mcp.tool()
    def query_f1_data(
        query: str,
        max_rows: Optional[int] = None,
        format: str = "json",
    ) -> dict[str, Any]:
        validator = get_sql_validator()
        validation = validator.validate_query(query)

        if not validation.is_valid:
            return {
                "success": False,
                "error": validation.error_message,
            }

        client = get_databricks_client()
        result = client.execute_query(query, max_rows=max_rows or 1000)

        if format == "markdown" and result.get("success"):
            result["formatted"] = ResultFormatter.format_as_markdown_table(result)
        elif format == "text" and result.get("success"):
            result["formatted"] = ResultFormatter.format_query_result(result)

        return result

    @mcp.tool()
    def get_driver_season_stats(
        driver_name: Optional[str] = None,
        season: Optional[int] = None,
        team_name: Optional[str] = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        conditions = []
        if driver_name:
            safe_name = driver_name.replace("'", "''")
            conditions.append(f"LOWER(driverName) LIKE LOWER('%{safe_name}%')")
        if season:
            conditions.append(f"season = {int(season)}")
        if team_name:
            safe_team = team_name.replace("'", "''")
            conditions.append(f"LOWER(teamName) LIKE LOWER('%{safe_team}%')")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
        SELECT 
            season,
            driverName,
            teamName,
            races_count,
            total_points,
            wins,
            podiums,
            dnf_count,
            avg_grid_position,
            avg_finish_position,
            final_champ_position
        FROM f1.f1_gold_driver_season_stats
        {where_clause}
        ORDER BY season DESC, total_points DESC
        LIMIT {limit}
        """

        client = get_databricks_client()
        return client.execute_query(query)

    @mcp.tool()
    def get_constructor_season_stats(
        team_name: Optional[str] = None,
        season: Optional[int] = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        conditions = []
        if team_name:
            safe_team = team_name.replace("'", "''")
            conditions.append(f"LOWER(teamName) LIKE LOWER('%{safe_team}%')")
        if season:
            conditions.append(f"season = {int(season)}")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
        SELECT 
            season,
            teamName,
            teamNationality,
            entries_count,
            team_total_points,
            wins,
            podiums,
            dnf_count,
            final_cons_position
        FROM f1.f1_gold_constructor_season_stats
        {where_clause}
        ORDER BY season DESC, team_total_points DESC
        LIMIT {limit}
        """

        client = get_databricks_client()
        return client.execute_query(query)

    @mcp.tool()
    def get_race_results(
        race_name: Optional[str] = None,
        season: Optional[int] = None,
        driver_name: Optional[str] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        conditions = []
        if race_name:
            safe_race = race_name.replace("'", "''")
            conditions.append(f"LOWER(raceName) LIKE LOWER('%{safe_race}%')")
        if season:
            conditions.append(f"season = {int(season)}")
        if driver_name:
            safe_name = driver_name.replace("'", "''")
            conditions.append(f"LOWER(driverName) LIKE LOWER('%{safe_name}%')")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
        SELECT 
            season,
            round,
            raceName,
            circuitName,
            country,
            driverName,
            teamName,
            grid,
            race_finish_position,
            race_points,
            pit_stop_count,
            avg_pit_stop_ms,
            statusDescription
        FROM f1.f1_gold_race_driver_features
        {where_clause}
        ORDER BY season DESC, round DESC, race_finish_position
        LIMIT {limit}
        """

        client = get_databricks_client()
        return client.execute_query(query)

    @mcp.tool()
    def get_pit_stop_data(
        season: Optional[int] = None,
        driver_name: Optional[str] = None,
        team_name: Optional[str] = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        conditions = []
        if season:
            conditions.append(f"season = {int(season)}")
        if driver_name:
            safe_name = driver_name.replace("'", "''")
            conditions.append(f"LOWER(driverName) LIKE LOWER('%{safe_name}%')")
        if team_name:
            safe_team = team_name.replace("'", "''")
            conditions.append(f"LOWER(teamName) LIKE LOWER('%{safe_team}%')")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
        SELECT 
            season,
            raceName,
            driverName,
            teamName,
            pit_stop_count,
            avg_pit_stop_ms,
            total_pit_stop_ms,
            race_finish_position
        FROM f1.f1_gold_race_driver_features
        {where_clause}
        AND pit_stop_count > 0
        ORDER BY season DESC, avg_pit_stop_ms
        LIMIT {limit}
        """

        client = get_databricks_client()
        return client.execute_query(query)

