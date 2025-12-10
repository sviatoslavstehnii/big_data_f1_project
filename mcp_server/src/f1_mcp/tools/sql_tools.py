"""SQL query execution tools for MCP server."""

from typing import Any, Optional

from fastmcp import FastMCP

from f1_mcp.services.databricks_client import get_databricks_client
from f1_mcp.utils.validators import get_sql_validator
from f1_mcp.utils.formatters import ResultFormatter


def register_sql_tools(mcp: FastMCP) -> None:
    """Register SQL-related tools with the MCP server.

    Args:
        mcp: The FastMCP server instance.
    """

    @mcp.tool()
    def query_f1_data(
        query: str,
        max_rows: Optional[int] = None,
        format: str = "json",
    ) -> dict[str, Any]:
        """Execute a read-only SQL query against F1 data in Databricks.

        Use this tool to run SQL queries against the Formula 1 database.
        Only SELECT queries are allowed for safety.

        Args:
            query: SQL query to execute. Must start with SELECT, WITH, or SHOW.
            max_rows: Maximum number of rows to return (default: 1000).
            format: Output format - 'json', 'markdown', or 'text'.

        Returns:
            Query results with columns and rows.

        Example queries:
            - "SELECT * FROM f1.f1_gold_driver_season_stats LIMIT 10"
            - "SELECT driverName, total_points FROM f1.f1_gold_driver_season_stats WHERE season = 2023"
        """
        # Validate the query
        validator = get_sql_validator()
        validation = validator.validate_query(query)

        if not validation.is_valid:
            return {
                "success": False,
                "error": validation.error_message,
            }

        # Execute the query
        client = get_databricks_client()
        result = client.execute_query(query, max_rows=max_rows or 1000)

        # Format based on requested format
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
        """Get driver season statistics from the gold layer.

        Retrieves aggregated statistics for F1 drivers including wins,
        podiums, points, and championship standings.

        Args:
            driver_name: Optional filter by driver name (partial match).
            season: Optional filter by season year.
            team_name: Optional filter by team name (partial match).
            limit: Maximum rows to return.

        Returns:
            Driver season statistics.
        """
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
        """Get constructor/team season statistics from the gold layer.

        Retrieves aggregated statistics for F1 constructors/teams.

        Args:
            team_name: Optional filter by team name (partial match).
            season: Optional filter by season year.
            limit: Maximum rows to return.

        Returns:
            Constructor season statistics.
        """
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
        """Get race results with driver features.

        Retrieves detailed race-level data including grid positions,
        finish positions, pit stops, and lap times.

        Args:
            race_name: Optional filter by race/circuit name (partial match).
            season: Optional filter by season year.
            driver_name: Optional filter by driver name (partial match).
            limit: Maximum rows to return.

        Returns:
            Race results with features.
        """
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
        """Get pit stop data for analysis.

        Retrieves pit stop information for drivers including duration
        and stop counts.

        Args:
            season: Optional filter by season year.
            driver_name: Optional filter by driver name (partial match).
            team_name: Optional filter by team name (partial match).
            limit: Maximum rows to return.

        Returns:
            Pit stop data for analysis.
        """
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

