"""Visualization tools for MCP server."""

import base64
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastmcp import FastMCP

from f1_mcp.services.databricks_client import get_databricks_client
from f1_mcp.services.chart_service import get_chart_service


# Default directory for saved charts
CHARTS_OUTPUT_DIR = Path.home() / "f1_charts"


def register_visualization_tools(mcp: FastMCP) -> None:
    """Register visualization tools with the MCP server.

    Args:
        mcp: The FastMCP server instance.
    """

    @mcp.tool()
    def chart_driver_performance(
        driver_name: str,
        metric: str = "total_points",
        chart_type: str = "line",
    ) -> dict[str, Any]:
        """Create a chart showing driver performance over seasons.

        Args:
            driver_name: Name of the driver (partial match supported).
            metric: Metric to chart - 'total_points', 'wins', 'podiums', 
                   'avg_finish_position', 'dnf_count'.
            chart_type: 'line' or 'bar'.

        Returns:
            Chart as base64-encoded PNG with metadata.
        """
        valid_metrics = [
            "total_points", "wins", "podiums", 
            "avg_finish_position", "dnf_count", "races_count"
        ]
        if metric not in valid_metrics:
            return {
                "success": False,
                "error": f"Invalid metric. Choose from: {', '.join(valid_metrics)}",
            }

        client = get_databricks_client()
        safe_name = driver_name.replace("'", "''")

        query = f"""
        SELECT 
            season,
            driverName,
            teamName,
            {metric}
        FROM f1.f1_gold_driver_season_stats
        WHERE LOWER(driverName) LIKE LOWER('%{safe_name}%')
        ORDER BY season
        """

        result = client.execute_query(query)

        if not result.get("success"):
            return result

        rows = result.get("rows", [])
        if not rows:
            return {
                "success": False,
                "error": f"No data found for driver matching '{driver_name}'",
            }

        # Extract data for charting
        actual_driver_name = rows[0].get("driverName", driver_name)
        seasons = [r.get("season") for r in rows]
        values = [float(r.get(metric, 0) or 0) for r in rows]

        chart_service = get_chart_service()

        if chart_type == "line":
            chart_result = chart_service.create_line_chart(
                x_values=seasons,
                y_series={actual_driver_name: values},
                title=f"{actual_driver_name} - {metric.replace('_', ' ').title()} by Season",
                xlabel="Season",
                ylabel=metric.replace("_", " ").title(),
            )
        else:
            chart_result = chart_service.create_bar_chart(
                labels=[str(s) for s in seasons],
                values=values,
                title=f"{actual_driver_name} - {metric.replace('_', ' ').title()} by Season",
                xlabel="Season",
                ylabel=metric.replace("_", " ").title(),
            )

        return {
            "success": True,
            "driver": actual_driver_name,
            "metric": metric,
            "seasons_covered": [min(seasons), max(seasons)],
            **chart_result.to_dict(),
        }

    @mcp.tool()
    def chart_team_comparison(
        team_names: list[str],
        season: Optional[int] = None,
        metric: str = "team_total_points",
    ) -> dict[str, Any]:
        """Create a chart comparing multiple teams.

        Args:
            team_names: List of team names to compare.
            season: Optional specific season (if None, shows latest 5 seasons).
            metric: Metric to compare - 'team_total_points', 'wins', 'podiums'.

        Returns:
            Chart as base64-encoded PNG with metadata.
        """
        if not team_names or len(team_names) < 2:
            return {
                "success": False,
                "error": "Please provide at least 2 team names to compare.",
            }

        valid_metrics = ["team_total_points", "wins", "podiums", "dnf_count"]
        if metric not in valid_metrics:
            return {
                "success": False,
                "error": f"Invalid metric. Choose from: {', '.join(valid_metrics)}",
            }

        # Build team filter
        team_conditions = " OR ".join(
            f"LOWER(teamName) LIKE LOWER('%{t.replace(chr(39), chr(39)*2)}%')"
            for t in team_names
        )

        season_filter = ""
        if season:
            season_filter = f"AND season = {int(season)}"
        else:
            # Get latest 5 seasons
            season_filter = "AND season >= (SELECT MAX(season) - 4 FROM f1.f1_gold_constructor_season_stats)"

        client = get_databricks_client()

        query = f"""
        SELECT 
            season,
            teamName,
            {metric}
        FROM f1.f1_gold_constructor_season_stats
        WHERE ({team_conditions})
        {season_filter}
        ORDER BY season, teamName
        """

        result = client.execute_query(query)

        if not result.get("success"):
            return result

        rows = result.get("rows", [])
        if not rows:
            return {
                "success": False,
                "error": "No data found for the specified teams.",
            }

        # Organize data by team
        seasons = sorted(set(r.get("season") for r in rows))
        teams_data = {}

        for row in rows:
            team = row.get("teamName")
            if team not in teams_data:
                teams_data[team] = {}
            teams_data[team][row.get("season")] = float(row.get(metric, 0) or 0)

        # Build series for chart
        chart_series = {}
        for team in teams_data:
            chart_series[team] = [teams_data[team].get(s, 0) for s in seasons]

        chart_service = get_chart_service()

        if len(seasons) == 1:
            # Single season - use grouped bar
            chart_result = chart_service.create_bar_chart(
                labels=list(teams_data.keys()),
                values=[teams_data[t].get(seasons[0], 0) for t in teams_data],
                title=f"Team Comparison - {metric.replace('_', ' ').title()} ({seasons[0]})",
                xlabel="Team",
                ylabel=metric.replace("_", " ").title(),
            )
        else:
            # Multiple seasons - use line chart
            chart_result = chart_service.create_line_chart(
                x_values=seasons,
                y_series=chart_series,
                title=f"Team Comparison - {metric.replace('_', ' ').title()}",
                xlabel="Season",
                ylabel=metric.replace("_", " ").title(),
            )

        return {
            "success": True,
            "teams": list(teams_data.keys()),
            "metric": metric,
            "seasons": seasons,
            **chart_result.to_dict(),
        }

    @mcp.tool()
    def chart_pit_stop_analysis(
        season: Optional[int] = None,
        team_name: Optional[str] = None,
        chart_type: str = "box",
    ) -> dict[str, Any]:
        """Create a chart analyzing pit stop performance.

        Args:
            season: Optional season filter.
            team_name: Optional team filter.
            chart_type: 'box' for distribution, 'scatter' for correlation.

        Returns:
            Chart as base64-encoded PNG with metadata.
        """
        client = get_databricks_client()

        conditions = ["pit_stop_count > 0", "avg_pit_stop_ms > 0"]
        if season:
            conditions.append(f"season = {int(season)}")
        if team_name:
            safe_team = team_name.replace("'", "''")
            conditions.append(f"LOWER(teamName) LIKE LOWER('%{safe_team}%')")

        where_clause = " AND ".join(conditions)

        query = f"""
        SELECT 
            teamName,
            avg_pit_stop_ms,
            pit_stop_count,
            race_finish_position
        FROM f1.f1_gold_race_driver_features
        WHERE {where_clause}
        ORDER BY teamName
        LIMIT 1000
        """

        result = client.execute_query(query)

        if not result.get("success"):
            return result

        rows = result.get("rows", [])
        if not rows:
            return {
                "success": False,
                "error": "No pit stop data found for the specified criteria.",
            }

        chart_service = get_chart_service()

        if chart_type == "box":
            # Group by team for box plot
            team_data = {}
            for row in rows:
                team = row.get("teamName")
                if team not in team_data:
                    team_data[team] = []
                pit_ms = row.get("avg_pit_stop_ms")
                if pit_ms:
                    team_data[team].append(float(pit_ms))

            # Filter to teams with enough data points
            team_data = {k: v for k, v in team_data.items() if len(v) >= 5}

            if not team_data:
                return {
                    "success": False,
                    "error": "Not enough data for box plot analysis.",
                }

            # Sort by median pit time
            sorted_teams = sorted(
                team_data.keys(),
                key=lambda t: sum(team_data[t]) / len(team_data[t])
            )[:10]  # Top 10 teams

            team_data = {t: team_data[t] for t in sorted_teams}

            chart_result = chart_service.create_box_plot(
                data=team_data,
                title="Pit Stop Duration Distribution by Team",
                xlabel="Team",
                ylabel="Average Pit Stop Time (ms)",
            )
        else:
            # Scatter plot: pit stop time vs finish position
            x_values = [float(r.get("avg_pit_stop_ms", 0)) for r in rows if r.get("avg_pit_stop_ms")]
            y_values = [float(r.get("race_finish_position", 0)) for r in rows if r.get("avg_pit_stop_ms")]

            chart_result = chart_service.create_scatter_chart(
                x_values=x_values,
                y_values=y_values,
                title="Pit Stop Time vs Race Finish Position",
                xlabel="Average Pit Stop Time (ms)",
                ylabel="Finish Position",
            )

        season_str = str(season) if season else "all seasons"
        return {
            "success": True,
            "analysis_type": chart_type,
            "season": season_str,
            "data_points": len(rows),
            **chart_result.to_dict(),
        }

    @mcp.tool()
    def chart_correlation_heatmap(
        features: Optional[list[str]] = None,
        season: Optional[int] = None,
    ) -> dict[str, Any]:
        """Create a correlation heatmap of race performance features.

        Args:
            features: Optional list of features to include. 
                     Defaults to key performance features.
            season: Optional season filter.

        Returns:
            Heatmap as base64-encoded PNG with metadata.
        """
        default_features = [
            "grid",
            "race_finish_position",
            "race_points",
            "pit_stop_count",
            "avg_pit_stop_ms",
            "quali_best_position",
        ]

        features = features or default_features
        client = get_databricks_client()

        season_filter = f"AND season = {int(season)}" if season else ""

        feature_list = ", ".join(features)
        query = f"""
        SELECT {feature_list}
        FROM f1.f1_gold_race_driver_features
        WHERE race_finish_position IS NOT NULL
        {season_filter}
        LIMIT 5000
        """

        result = client.execute_query(query)

        if not result.get("success"):
            return result

        rows = result.get("rows", [])
        if not rows:
            return {
                "success": False,
                "error": "No data found for correlation analysis.",
            }

        # Build correlation matrix
        import numpy as np

        # Extract numeric data
        data = {f: [] for f in features}
        for row in rows:
            valid_row = True
            for f in features:
                val = row.get(f)
                if val is None:
                    valid_row = False
                    break
            if valid_row:
                for f in features:
                    data[f].append(float(row.get(f, 0)))

        if not data[features[0]]:
            return {
                "success": False,
                "error": "Not enough valid data for correlation analysis.",
            }

        # Calculate correlation matrix
        n = len(features)
        corr_matrix = [[0.0] * n for _ in range(n)]

        for i, f1 in enumerate(features):
            for j, f2 in enumerate(features):
                if len(data[f1]) > 1:
                    corr = np.corrcoef(data[f1], data[f2])[0, 1]
                    corr_matrix[i][j] = corr if not np.isnan(corr) else 0.0
                else:
                    corr_matrix[i][j] = 1.0 if i == j else 0.0

        chart_service = get_chart_service()

        # Shorten feature names for display
        short_names = [f.replace("_", " ").replace("position", "pos")[:15] for f in features]

        chart_result = chart_service.create_heatmap(
            data=corr_matrix,
            x_labels=short_names,
            y_labels=short_names,
            title="Feature Correlation Heatmap",
        )

        return {
            "success": True,
            "features": features,
            "data_points": len(data[features[0]]),
            "season": season or "all",
            **chart_result.to_dict(),
        }

    @mcp.tool()
    def chart_season_standings(
        season: int,
        top_n: int = 10,
        entity: str = "drivers",
    ) -> dict[str, Any]:
        """Create a chart showing championship standings for a season.

        Args:
            season: Season year.
            top_n: Number of top positions to show.
            entity: 'drivers' or 'constructors'.

        Returns:
            Chart as base64-encoded PNG with metadata.
        """
        client = get_databricks_client()
        top_n = min(top_n, 20)

        if entity == "drivers":
            query = f"""
            SELECT 
                driverName as name,
                total_points as points,
                wins,
                final_champ_position as position
            FROM f1.f1_gold_driver_season_stats
            WHERE season = {int(season)}
            ORDER BY total_points DESC
            LIMIT {top_n}
            """
            title = f"{season} Driver Championship Standings"
        else:
            query = f"""
            SELECT 
                teamName as name,
                team_total_points as points,
                wins,
                final_cons_position as position
            FROM f1.f1_gold_constructor_season_stats
            WHERE season = {int(season)}
            ORDER BY team_total_points DESC
            LIMIT {top_n}
            """
            title = f"{season} Constructor Championship Standings"

        result = client.execute_query(query)

        if not result.get("success"):
            return result

        rows = result.get("rows", [])
        if not rows:
            return {
                "success": False,
                "error": f"No data found for season {season}.",
            }

        # Extract data
        names = [r.get("name", "") for r in rows]
        points = [float(r.get("points", 0) or 0) for r in rows]

        chart_service = get_chart_service()

        chart_result = chart_service.create_bar_chart(
            labels=names,
            values=points,
            title=title,
            xlabel="",
            ylabel="Points",
            horizontal=True,
        )

        return {
            "success": True,
            "season": season,
            "entity": entity,
            "standings": [
                {"name": r.get("name"), "points": r.get("points"), "wins": r.get("wins")}
                for r in rows
            ],
            **chart_result.to_dict(),
        }

    @mcp.tool()
    def chart_custom(
        query: str,
        x_column: str,
        y_column: str,
        chart_type: str = "bar",
        title: Optional[str] = None,
        group_column: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a custom chart from any SQL query result.

        Args:
            query: SQL SELECT query to get the data.
            x_column: Column name to use for x-axis.
            y_column: Column name to use for y-axis values.
            chart_type: 'bar', 'line', 'scatter', or 'horizontal_bar'.
            title: Optional chart title.
            group_column: Optional column for grouping (creates multi-series chart).

        Returns:
            Chart as base64-encoded PNG with metadata.
        """
        from f1_mcp.utils.validators import get_sql_validator

        # Validate query
        validator = get_sql_validator()
        validation = validator.validate_query(query)

        if not validation.is_valid:
            return {
                "success": False,
                "error": validation.error_message,
            }

        client = get_databricks_client()
        result = client.execute_query(query, max_rows=500)

        if not result.get("success"):
            return result

        rows = result.get("rows", [])
        if not rows:
            return {
                "success": False,
                "error": "Query returned no data.",
            }

        # Validate columns exist
        columns = result.get("columns", [])
        if x_column not in columns:
            return {
                "success": False,
                "error": f"Column '{x_column}' not found. Available: {', '.join(columns)}",
            }
        if y_column not in columns:
            return {
                "success": False,
                "error": f"Column '{y_column}' not found. Available: {', '.join(columns)}",
            }

        chart_service = get_chart_service()
        auto_title = title or f"{y_column} by {x_column}"

        if group_column and group_column in columns:
            # Multi-series chart
            groups = {}
            x_values = []

            for row in rows:
                x_val = row.get(x_column)
                if x_val not in x_values:
                    x_values.append(x_val)

                group = row.get(group_column, "Default")
                if group not in groups:
                    groups[group] = {}
                groups[group][x_val] = float(row.get(y_column, 0) or 0)

            # Build series
            series = {
                g: [groups[g].get(x, 0) for x in x_values]
                for g in groups
            }

            chart_result = chart_service.create_line_chart(
                x_values=x_values,
                y_series=series,
                title=auto_title,
                xlabel=x_column,
                ylabel=y_column,
            )
        else:
            # Single series chart
            x_values = [str(r.get(x_column, "")) for r in rows]
            y_values = [float(r.get(y_column, 0) or 0) for r in rows]

            if chart_type == "line":
                chart_result = chart_service.create_line_chart(
                    x_values=x_values,
                    y_series={"Data": y_values},
                    title=auto_title,
                    xlabel=x_column,
                    ylabel=y_column,
                )
            elif chart_type == "scatter":
                chart_result = chart_service.create_scatter_chart(
                    x_values=[float(v) if v.replace(".", "").replace("-", "").isdigit() else i 
                             for i, v in enumerate(x_values)],
                    y_values=y_values,
                    title=auto_title,
                    xlabel=x_column,
                    ylabel=y_column,
                )
            elif chart_type == "horizontal_bar":
                chart_result = chart_service.create_bar_chart(
                    labels=x_values,
                    values=y_values,
                    title=auto_title,
                    xlabel=x_column,
                    ylabel=y_column,
                    horizontal=True,
                )
            else:  # bar
                chart_result = chart_service.create_bar_chart(
                    labels=x_values,
                    values=y_values,
                    title=auto_title,
                    xlabel=x_column,
                    ylabel=y_column,
                )

        return {
            "success": True,
            "query": query,
            "row_count": len(rows),
            **chart_result.to_dict(),
        }

    @mcp.tool()
    def save_chart_to_file(
        chart_base64: str,
        filename: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> dict[str, Any]:
        """Save a base64-encoded chart image to a file.

        Use this after generating a chart to save it to disk for viewing.

        Args:
            chart_base64: The base64-encoded PNG image string from a chart tool.
            filename: Optional filename (without extension). Auto-generated if not provided.
            output_dir: Optional output directory. Defaults to ~/f1_charts/

        Returns:
            Dictionary with the saved file path.
        """
        try:
            # Determine output directory
            if output_dir:
                out_path = Path(output_dir)
            else:
                out_path = CHARTS_OUTPUT_DIR

            # Create directory if it doesn't exist
            out_path.mkdir(parents=True, exist_ok=True)

            # Generate filename if not provided
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"f1_chart_{timestamp}"

            # Ensure .png extension
            if not filename.endswith(".png"):
                filename = f"{filename}.png"

            file_path = out_path / filename

            # Decode and save
            image_data = base64.b64decode(chart_base64)
            with open(file_path, "wb") as f:
                f.write(image_data)

            return {
                "success": True,
                "file_path": str(file_path),
                "message": f"Chart saved to {file_path}",
                "hint": f"Open the file with: open '{file_path}'"
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to save chart: {str(e)}",
            }

    @mcp.tool()
    def open_chart(file_path: str) -> dict[str, Any]:
        """Open a saved chart file with the system's default image viewer.

        Args:
            file_path: Path to the chart image file.

        Returns:
            Status of the open operation.
        """
        import subprocess
        import sys

        try:
            path = Path(file_path)
            if not path.exists():
                return {
                    "success": False,
                    "error": f"File not found: {file_path}",
                }

            # Open with system default viewer
            if sys.platform == "darwin":  # macOS
                subprocess.run(["open", str(path)], check=True)
            elif sys.platform == "win32":  # Windows
                subprocess.run(["start", "", str(path)], shell=True, check=True)
            else:  # Linux
                subprocess.run(["xdg-open", str(path)], check=True)

            return {
                "success": True,
                "message": f"Opened {file_path} with system viewer",
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to open chart: {str(e)}",
            }

    @mcp.tool()
    def list_saved_charts(output_dir: Optional[str] = None) -> dict[str, Any]:
        """List all saved chart files.

        Args:
            output_dir: Optional directory to list. Defaults to ~/f1_charts/

        Returns:
            List of saved chart files with metadata.
        """
        try:
            if output_dir:
                out_path = Path(output_dir)
            else:
                out_path = CHARTS_OUTPUT_DIR

            if not out_path.exists():
                return {
                    "success": True,
                    "charts": [],
                    "message": "No charts directory found. Generate and save a chart first.",
                }

            charts = []
            for f in sorted(out_path.glob("*.png"), key=os.path.getmtime, reverse=True):
                stat = f.stat()
                charts.append({
                    "filename": f.name,
                    "path": str(f),
                    "size_kb": round(stat.st_size / 1024, 1),
                    "created": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })

            return {
                "success": True,
                "charts": charts,
                "count": len(charts),
                "directory": str(out_path),
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to list charts: {str(e)}",
            }

