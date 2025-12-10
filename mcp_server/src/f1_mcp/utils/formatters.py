"""Result formatting utilities for MCP tool responses."""

from typing import Any, Optional
import json


class ResultFormatter:

    @staticmethod
    def format_query_result(
        result: dict[str, Any],
        max_display_rows: int = 50,
    ) -> str:
        if not result.get("success"):
            error = result.get("error", "Unknown error")
            return f"Query failed: {error}"

        columns = result.get("columns", [])
        rows = result.get("rows", [])
        row_count = result.get("row_count", len(rows))

        if not columns:
            return "Query returned no columns."

        if not rows:
            return "Query returned no rows."

        lines = [
            f"Query returned {row_count} row(s)",
            "",
            "Columns: " + ", ".join(columns),
            "",
        ]

        display_rows = rows[:max_display_rows]
        for i, row in enumerate(display_rows, 1):
            row_str = " | ".join(
                f"{col}: {ResultFormatter._format_value(row.get(col))}"
                for col in columns
            )
            lines.append(f"[{i}] {row_str}")

        if row_count > max_display_rows:
            lines.append(f"... and {row_count - max_display_rows} more rows")

        return "\n".join(lines)

    @staticmethod
    def format_table_list(result: dict[str, Any]) -> str:
        if not result.get("success"):
            return f"Failed to list tables: {result.get('error')}"

        rows = result.get("rows", [])
        if not rows:
            return "No tables found."

        lines = [f"Found {len(rows)} table(s):", ""]

        for row in rows:
            table_name = row.get("table_name", "Unknown")
            table_type = row.get("table_type", "")
            comment = row.get("comment", "")

            line = f"  - {table_name}"
            if table_type:
                line += f" ({table_type})"
            if comment:
                line += f": {comment}"
            lines.append(line)

        return "\n".join(lines)

    @staticmethod
    def format_table_schema(table_name: str, result: dict[str, Any]) -> str:
        if not result.get("success"):
            return f"Failed to get schema: {result.get('error')}"

        rows = result.get("rows", [])
        if not rows:
            return f"No columns found for table '{table_name}'."

        lines = [f"Schema for '{table_name}':", ""]

        for row in rows:
            col_name = row.get("column_name", "Unknown")
            data_type = row.get("data_type", "Unknown")
            nullable = row.get("is_nullable", "YES")
            comment = row.get("comment", "")

            null_str = "NULL" if nullable == "YES" else "NOT NULL"
            line = f"  - {col_name}: {data_type} ({null_str})"
            if comment:
                line += f" -- {comment}"
            lines.append(line)

        return "\n".join(lines)

    @staticmethod
    def format_as_markdown_table(
        result: dict[str, Any],
        max_rows: int = 20,
    ) -> str:
        if not result.get("success"):
            return f"**Error:** {result.get('error')}"

        columns = result.get("columns", [])
        rows = result.get("rows", [])

        if not columns or not rows:
            return "*No data to display*"

        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"

        data_rows = []
        for row in rows[:max_rows]:
            values = [
                ResultFormatter._format_value(row.get(col))
                for col in columns
            ]
            data_rows.append("| " + " | ".join(values) + " |")

        lines = [header, separator] + data_rows

        if len(rows) > max_rows:
            lines.append(f"\n*... {len(rows) - max_rows} more rows*")

        return "\n".join(lines)

    @staticmethod
    def _format_value(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, float):
            return f"{value:.2f}"
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        return str(value)

    @staticmethod
    def to_json(result: dict[str, Any], indent: int = 2) -> str:
        return json.dumps(result, indent=indent, default=str)

    @staticmethod
    def extract_numeric_columns(
        result: dict[str, Any],
    ) -> tuple[list[str], list[dict]]:
        if not result.get("success") or not result.get("rows"):
            return [], []

        rows = result["rows"]
        columns = result.get("columns", [])

        numeric_cols = []
        if rows:
            first_row = rows[0]
            for col in columns:
                val = first_row.get(col)
                if isinstance(val, (int, float)) or (
                    isinstance(val, str) and val.replace(".", "").replace("-", "").isdigit()
                ):
                    numeric_cols.append(col)

        return numeric_cols, rows

