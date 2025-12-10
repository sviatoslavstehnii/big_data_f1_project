"""Tests for F1 MCP Server tools."""

import pytest
from unittest.mock import Mock, patch

from f1_mcp.utils.validators import SQLValidator, ValidationResult
from f1_mcp.utils.formatters import ResultFormatter
from f1_mcp.services.model_service import ModelService, PredictionType


class TestSQLValidator:
    """Tests for SQL validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = SQLValidator()

    def test_valid_select_query(self):
        """Test that valid SELECT queries pass validation."""
        query = "SELECT * FROM f1.f1_gold_driver_season_stats LIMIT 10"
        result = self.validator.validate_query(query)
        assert result.is_valid is True
        assert result.error_message is None

    def test_valid_with_query(self):
        """Test that WITH (CTE) queries pass validation."""
        query = """
        WITH top_drivers AS (
            SELECT driverName, total_points
            FROM f1.f1_gold_driver_season_stats
        )
        SELECT * FROM top_drivers
        """
        result = self.validator.validate_query(query)
        assert result.is_valid is True

    def test_blocked_drop_query(self):
        """Test that DROP queries are blocked."""
        query = "DROP TABLE f1.f1_gold_driver_season_stats"
        result = self.validator.validate_query(query)
        assert result.is_valid is False
        assert "DROP" in result.error_message

    def test_blocked_delete_query(self):
        """Test that DELETE queries are blocked."""
        query = "DELETE FROM f1.f1_gold_driver_season_stats"
        result = self.validator.validate_query(query)
        assert result.is_valid is False
        assert "DELETE" in result.error_message

    def test_blocked_insert_query(self):
        """Test that INSERT queries are blocked."""
        query = "INSERT INTO f1.f1_gold_driver_season_stats VALUES (1, 2, 3)"
        result = self.validator.validate_query(query)
        assert result.is_valid is False
        assert "INSERT" in result.error_message

    def test_empty_query_rejected(self):
        """Test that empty queries are rejected."""
        result = self.validator.validate_query("")
        assert result.is_valid is False
        assert "empty" in result.error_message.lower()

    def test_non_select_start_rejected(self):
        """Test that queries not starting with SELECT are rejected."""
        query = "UPDATE f1.f1_gold_driver_season_stats SET wins = 0"
        result = self.validator.validate_query(query)
        assert result.is_valid is False

    def test_sanitize_identifier(self):
        """Test identifier sanitization."""
        # Normal identifier
        assert self.validator.sanitize_identifier("table_name") == "table_name"
        
        # With special characters
        assert self.validator.sanitize_identifier("table;DROP--") == "tableDROP"
        
        # With dots (for qualified names)
        assert self.validator.sanitize_identifier("catalog.schema.table") == "catalog.schema.table"

    def test_validate_table_name(self):
        """Test table name validation."""
        # Valid names
        assert self.validator.validate_table_name("f1_gold_driver_season_stats").is_valid
        assert self.validator.validate_table_name("f1.f1_gold_driver_season_stats").is_valid
        
        # Invalid names
        assert not self.validator.validate_table_name("").is_valid
        assert not self.validator.validate_table_name("table;DROP").is_valid


class TestResultFormatter:
    """Tests for result formatting."""

    def test_format_query_result_success(self):
        """Test formatting of successful query results."""
        result = {
            "success": True,
            "columns": ["name", "points"],
            "rows": [
                {"name": "Hamilton", "points": 100},
                {"name": "Verstappen", "points": 95},
            ],
            "row_count": 2,
        }
        
        formatted = ResultFormatter.format_query_result(result)
        assert "2 row(s)" in formatted
        assert "Hamilton" in formatted
        assert "Verstappen" in formatted

    def test_format_query_result_error(self):
        """Test formatting of failed query results."""
        result = {
            "success": False,
            "error": "Connection timeout",
        }
        
        formatted = ResultFormatter.format_query_result(result)
        assert "failed" in formatted.lower()
        assert "Connection timeout" in formatted

    def test_format_as_markdown_table(self):
        """Test markdown table formatting."""
        result = {
            "success": True,
            "columns": ["name", "wins"],
            "rows": [
                {"name": "Hamilton", "wins": 103},
                {"name": "Schumacher", "wins": 91},
            ],
        }
        
        formatted = ResultFormatter.format_as_markdown_table(result)
        assert "| name | wins |" in formatted
        assert "| --- | --- |" in formatted
        assert "Hamilton" in formatted

    def test_format_value_none(self):
        """Test NULL value formatting."""
        assert ResultFormatter._format_value(None) == "NULL"

    def test_format_value_float(self):
        """Test float formatting."""
        assert ResultFormatter._format_value(3.14159) == "3.14"


class TestModelService:
    """Tests for ML model service."""

    def setup_method(self):
        """Set up test fixtures."""
        self.service = ModelService()

    def test_predict_optimal_pit_count(self):
        """Test pit count prediction returns expected structure."""
        prediction = self.service.predict_optimal_pit_count(
            circuit_id=1,
            driver_id=1,
            season=2023,
            race_laps=60,
        )
        
        assert prediction.prediction_type == PredictionType.OPTIMAL_PIT_COUNT
        assert prediction.optimal_pit_count is not None
        assert prediction.confidence == 0.0  # Placeholder returns 0 confidence
        assert "placeholder" in prediction.message.lower()

    def test_predict_pit_stop_duration(self):
        """Test pit duration prediction returns expected structure."""
        prediction = self.service.predict_pit_stop_duration(
            circuit_id=1,
            driver_id=1,
            constructor_id=1,
            season=2023,
        )
        
        assert prediction.prediction_type == PredictionType.PIT_STOP_DURATION
        assert prediction.predicted_avg_pit_ms is not None
        assert prediction.predicted_total_pit_ms is not None

    def test_get_model_info(self):
        """Test model info returns expected keys."""
        info = self.service.get_model_info()
        
        assert "model_loaded" in info
        assert "model_version" in info
        assert "supported_predictions" in info
        assert info["status"] == "placeholder"

    def test_prediction_to_dict(self):
        """Test prediction serialization."""
        prediction = self.service.predict_optimal_pit_count(
            circuit_id=1,
            driver_id=1,
            season=2023,
            race_laps=50,
        )
        
        as_dict = prediction.to_dict()
        assert isinstance(as_dict, dict)
        assert "prediction_type" in as_dict
        assert "confidence" in as_dict
        assert "model_version" in as_dict


class TestChartService:
    """Tests for chart service (basic validation only)."""

    def test_chart_service_import(self):
        """Test that chart service can be imported."""
        from f1_mcp.services.chart_service import ChartService, get_chart_service
        
        service = get_chart_service()
        assert service is not None

    def test_chart_colors(self):
        """Test that chart colors are defined."""
        from f1_mcp.services.chart_service import ChartService
        
        service = ChartService()
        colors = service._get_colors(5)
        assert len(colors) == 5
        assert all(c.startswith("#") for c in colors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

