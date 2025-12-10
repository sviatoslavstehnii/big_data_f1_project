"""ML Model service for pit stop predictions (placeholder)."""

from typing import Any, Optional
from dataclasses import dataclass
from enum import Enum


class PredictionType(str, Enum):
    """Types of pit stop predictions available."""
    OPTIMAL_PIT_COUNT = "optimal_pit_count"
    PIT_STOP_DURATION = "pit_stop_duration"


@dataclass
class PitStopPrediction:
    """Result of a pit stop prediction."""
    prediction_type: PredictionType
    optimal_pit_count: Optional[int] = None
    predicted_total_pit_ms: Optional[float] = None
    predicted_avg_pit_ms: Optional[float] = None
    confidence: float = 0.0
    model_version: str = "placeholder-v0.1"
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "prediction_type": self.prediction_type.value,
            "optimal_pit_count": self.optimal_pit_count,
            "predicted_total_pit_ms": self.predicted_total_pit_ms,
            "predicted_avg_pit_ms": self.predicted_avg_pit_ms,
            "confidence": self.confidence,
            "model_version": self.model_version,
            "message": self.message,
        }


class ModelService:
    """Service for ML model predictions.

    This is a placeholder implementation. Replace with actual model
    inference logic when the ML model is trained and deployed.
    """

    def __init__(self):
        """Initialize the model service."""
        self._model_loaded = False
        self._model_version = "placeholder-v0.1"

    def predict_optimal_pit_count(
        self,
        circuit_id: int,
        driver_id: int,
        season: int,
        race_laps: int,
        weather_conditions: Optional[str] = None,
        tire_compound: Optional[str] = None,
    ) -> PitStopPrediction:
        """Predict optimal number of pit stops for a race.

        Args:
            circuit_id: ID of the circuit.
            driver_id: ID of the driver.
            season: Season year.
            race_laps: Total laps in the race.
            weather_conditions: Optional weather description.
            tire_compound: Optional starting tire compound.

        Returns:
            PitStopPrediction with optimal pit count.

        Note:
            This is a placeholder. Replace with actual model inference.
        """
        if race_laps < 40:
            estimated_pits = 1
        elif race_laps < 60:
            estimated_pits = 2
        else:
            estimated_pits = 3

        return PitStopPrediction(
            prediction_type=PredictionType.OPTIMAL_PIT_COUNT,
            optimal_pit_count=estimated_pits,
            confidence=0.0,  # Zero confidence for placeholder
            model_version=self._model_version,
            message=(
                "PLACEHOLDER: This is a placeholder prediction. "
                "Integration of the model is on the way"
            ),
        )

    def predict_pit_stop_duration(
        self,
        circuit_id: int,
        driver_id: int,
        constructor_id: int,
        season: int,
        pit_stop_number: int = 1,
    ) -> PitStopPrediction:
        """Predict pit stop duration for a driver/team combination.

        Args:
            circuit_id: ID of the circuit.
            driver_id: ID of the driver.
            constructor_id: ID of the constructor/team.
            season: Season year.
            pit_stop_number: Which pit stop (1st, 2nd, etc.).

        Returns:
            PitStopPrediction with duration estimates.

        Note:
            This is a placeholder. Replace with actual model inference.
        """
        estimated_avg_ms = 2500.0
        estimated_total_ms = estimated_avg_ms * pit_stop_number

        return PitStopPrediction(
            prediction_type=PredictionType.PIT_STOP_DURATION,
            predicted_avg_pit_ms=estimated_avg_ms,
            predicted_total_pit_ms=estimated_total_ms,
            confidence=0.0,
            model_version=self._model_version,
            message=(
                "PLACEHOLDER: This is a placeholder prediction. "
                "Integrate your trained ML model here for accurate predictions."
            ),
        )

    def get_model_info(self) -> dict[str, Any]:
        """Get information about the loaded model."""
        return {
            "model_loaded": self._model_loaded,
            "model_version": self._model_version,
            "supported_predictions": [p.value for p in PredictionType],
            "status": "placeholder",
            "message": (
                "This is a placeholder model service. "
                "To integrate your ML model:\n"
                "1. Load your trained model in __init__\n"
                "2. Replace placeholder logic in predict_* methods\n"
                "3. Update model_version and model_loaded accordingly"
            ),
        }


_model_instance: Optional[ModelService] = None


def get_model_service() -> ModelService:
    """Get or create the singleton ModelService instance."""
    global _model_instance
    if _model_instance is None:
        _model_instance = ModelService()
    return _model_instance

