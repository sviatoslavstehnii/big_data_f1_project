"""ML prediction tools for MCP server."""

from typing import Any, Optional

from fastmcp import FastMCP

from f1_mcp.services.model_service import get_model_service, PredictionType
from f1_mcp.services.databricks_client import get_databricks_client


def register_ml_tools(mcp: FastMCP) -> None:
    """Register ML-related tools with the MCP server.

    Args:
        mcp: The FastMCP server instance.
    """

    @mcp.tool()
    def predict_pit_stops(
        prediction_type: str,
        circuit_id: Optional[int] = None,
        driver_id: Optional[int] = None,
        constructor_id: Optional[int] = None,
        season: int = 2023,
        race_laps: int = 60,
        pit_stop_number: int = 1,
        weather_conditions: Optional[str] = None,
        tire_compound: Optional[str] = None,
    ) -> dict[str, Any]:
        """Predict pit stop strategy for a race.

        This tool provides two types of predictions:
        - 'optimal_pit_count': Predict the optimal number of pit stops
        - 'pit_stop_duration': Predict pit stop duration in milliseconds

        Args:
            prediction_type: Either 'optimal_pit_count' or 'pit_stop_duration'.
            circuit_id: ID of the circuit (optional, for context).
            driver_id: ID of the driver (optional, for context).
            constructor_id: ID of the constructor/team (required for duration prediction).
            season: Season year for historical context.
            race_laps: Total number of laps in the race.
            pit_stop_number: Which pit stop to predict duration for (1st, 2nd, etc.).
            weather_conditions: Optional weather description ('dry', 'wet', 'mixed').
            tire_compound: Optional starting tire compound ('soft', 'medium', 'hard').

        Returns:
            Prediction results with confidence and model information.

        Note:
            This is currently a PLACEHOLDER implementation. 
            The actual ML model should be integrated here.
        """
        model_service = get_model_service()

        # Validate prediction type
        try:
            pred_type = PredictionType(prediction_type)
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid prediction type. Choose from: {[p.value for p in PredictionType]}",
            }

        if pred_type == PredictionType.OPTIMAL_PIT_COUNT:
            prediction = model_service.predict_optimal_pit_count(
                circuit_id=circuit_id or 0,
                driver_id=driver_id or 0,
                season=season,
                race_laps=race_laps,
                weather_conditions=weather_conditions,
                tire_compound=tire_compound,
            )
        else:
            if not constructor_id:
                return {
                    "success": False,
                    "error": "constructor_id is required for pit stop duration prediction.",
                }

            prediction = model_service.predict_pit_stop_duration(
                circuit_id=circuit_id or 0,
                driver_id=driver_id or 0,
                constructor_id=constructor_id,
                season=season,
                pit_stop_number=pit_stop_number,
            )

        return {
            "success": True,
            "input_parameters": {
                "prediction_type": prediction_type,
                "circuit_id": circuit_id,
                "driver_id": driver_id,
                "constructor_id": constructor_id,
                "season": season,
                "race_laps": race_laps,
                "pit_stop_number": pit_stop_number,
                "weather_conditions": weather_conditions,
                "tire_compound": tire_compound,
            },
            **prediction.to_dict(),
        }

    @mcp.tool()
    def get_model_info() -> dict[str, Any]:
        """Get information about the ML model and its capabilities.

        Returns:
            Model information including version, supported predictions, and status.
        """
        model_service = get_model_service()
        return {
            "success": True,
            **model_service.get_model_info(),
        }

    @mcp.tool()
    def get_historical_pit_stats(
        circuit_name: Optional[str] = None,
        team_name: Optional[str] = None,
        season: Optional[int] = None,
    ) -> dict[str, Any]:
        """Get historical pit stop statistics for model context.

        This tool retrieves aggregated pit stop data that can be used
        as context for predictions or analysis.

        Args:
            circuit_name: Optional filter by circuit name.
            team_name: Optional filter by team name.
            season: Optional filter by season.

        Returns:
            Historical pit stop statistics.
        """
        client = get_databricks_client()

        conditions = ["pit_stop_count > 0", "avg_pit_stop_ms > 0"]
        group_by = []

        if circuit_name:
            safe_circuit = circuit_name.replace("'", "''")
            conditions.append(f"LOWER(circuitName) LIKE LOWER('%{safe_circuit}%')")
            group_by.append("circuitName")

        if team_name:
            safe_team = team_name.replace("'", "''")
            conditions.append(f"LOWER(teamName) LIKE LOWER('%{safe_team}%')")
            group_by.append("teamName")

        if season:
            conditions.append(f"season = {int(season)}")
        else:
            group_by.append("season")

        where_clause = " AND ".join(conditions)
        group_clause = ", ".join(group_by) if group_by else "1"

        # Build appropriate query based on grouping
        if group_by:
            select_fields = ", ".join(group_by)
            query = f"""
            SELECT 
                {select_fields},
                COUNT(*) as race_count,
                AVG(pit_stop_count) as avg_stops_per_race,
                AVG(avg_pit_stop_ms) as avg_pit_duration_ms,
                MIN(avg_pit_stop_ms) as fastest_avg_pit_ms,
                MAX(avg_pit_stop_ms) as slowest_avg_pit_ms,
                STDDEV(avg_pit_stop_ms) as pit_duration_stddev
            FROM f1.f1_gold_race_driver_features
            WHERE {where_clause}
            GROUP BY {group_clause}
            ORDER BY avg_pit_duration_ms
            LIMIT 50
            """
        else:
            query = f"""
            SELECT 
                COUNT(*) as total_races,
                AVG(pit_stop_count) as avg_stops_per_race,
                AVG(avg_pit_stop_ms) as avg_pit_duration_ms,
                MIN(avg_pit_stop_ms) as fastest_avg_pit_ms,
                MAX(avg_pit_stop_ms) as slowest_avg_pit_ms,
                PERCENTILE_APPROX(avg_pit_stop_ms, 0.5) as median_pit_duration_ms,
                STDDEV(avg_pit_stop_ms) as pit_duration_stddev
            FROM f1.f1_gold_race_driver_features
            WHERE {where_clause}
            """

        result = client.execute_query(query)

        if result.get("success"):
            result["filters_applied"] = {
                "circuit": circuit_name,
                "team": team_name,
                "season": season,
            }
            result["use_case"] = (
                "Use this data to understand typical pit stop patterns "
                "and provide context for predictions."
            )

        return result

    @mcp.tool()
    def analyze_race_factors(
        season: int,
        race_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """Analyze factors affecting race performance.

        Provides analysis of how different factors correlate with
        race finish position.

        Args:
            season: Season year to analyze.
            race_name: Optional specific race to analyze.

        Returns:
            Analysis of performance factors.
        """
        client = get_databricks_client()

        race_filter = ""
        if race_name:
            safe_race = race_name.replace("'", "''")
            race_filter = f"AND LOWER(raceName) LIKE LOWER('%{safe_race}%')"

        query = f"""
        SELECT 
            grid,
            race_finish_position,
            quali_best_position,
            pit_stop_count,
            avg_pit_stop_ms,
            race_points,
            statusDescription
        FROM f1.f1_gold_race_driver_features
        WHERE season = {int(season)}
          AND race_finish_position IS NOT NULL
          AND grid IS NOT NULL
          {race_filter}
        """

        result = client.execute_query(query, max_rows=2000)

        if not result.get("success"):
            return result

        rows = result.get("rows", [])
        if not rows:
            return {
                "success": False,
                "error": f"No data found for season {season}.",
            }

        # Calculate basic statistics
        import numpy as np

        grids = [float(r.get("grid", 0)) for r in rows if r.get("grid")]
        finishes = [float(r.get("race_finish_position", 0)) for r in rows if r.get("race_finish_position")]

        # Grid to finish correlation
        if len(grids) > 1 and len(finishes) > 1:
            grid_finish_corr = np.corrcoef(grids[:len(finishes)], finishes[:len(grids)])[0, 1]
        else:
            grid_finish_corr = 0

        # Position gains/losses
        position_changes = [
            float(r.get("grid", 0)) - float(r.get("race_finish_position", 0))
            for r in rows
            if r.get("grid") and r.get("race_finish_position")
        ]

        avg_position_change = np.mean(position_changes) if position_changes else 0

        # DNF analysis
        dnf_count = sum(1 for r in rows if r.get("statusDescription") != "Finished")
        dnf_rate = dnf_count / len(rows) if rows else 0

        # Pit stop impact
        pit_times = [float(r.get("avg_pit_stop_ms", 0)) for r in rows if r.get("avg_pit_stop_ms")]
        avg_pit_time = np.mean(pit_times) if pit_times else 0

        return {
            "success": True,
            "season": season,
            "race": race_name or "all races",
            "total_entries": len(rows),
            "analysis": {
                "grid_finish_correlation": round(grid_finish_corr, 3),
                "correlation_interpretation": (
                    "Strong" if abs(grid_finish_corr) > 0.7 else
                    "Moderate" if abs(grid_finish_corr) > 0.4 else "Weak"
                ),
                "avg_position_change": round(avg_position_change, 2),
                "dnf_rate": round(dnf_rate * 100, 1),
                "avg_pit_stop_ms": round(avg_pit_time, 0),
            },
            "insights": [
                f"Grid position has a {('strong' if abs(grid_finish_corr) > 0.7 else 'moderate' if abs(grid_finish_corr) > 0.4 else 'weak')} correlation with finish position.",
                f"Drivers {'gain' if avg_position_change > 0 else 'lose'} an average of {abs(avg_position_change):.1f} positions from grid to finish.",
                f"DNF rate for the {'race' if race_name else 'season'} is {dnf_rate * 100:.1f}%.",
            ],
        }

