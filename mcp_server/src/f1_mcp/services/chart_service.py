"""Chart generation service using Matplotlib."""

import base64
import io
from typing import Any, Optional
from dataclasses import dataclass
from enum import Enum

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


class ChartType(str, Enum):
    BAR = "bar"
    LINE = "line"
    SCATTER = "scatter"
    HEATMAP = "heatmap"
    BOX = "box"
    PIE = "pie"
    GROUPED_BAR = "grouped_bar"
    HORIZONTAL_BAR = "horizontal_bar"


@dataclass
class ChartResult:
    chart_base64: str
    chart_type: str
    title: str
    description: str
    data_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:    
        return {
            "chart_base64": self.chart_base64,
            "chart_type": self.chart_type,
            "title": self.title,
            "description": self.description,
            "data_summary": self.data_summary,
        }


class ChartService:

    # F1-inspired color palette
    COLORS = [
        "#E10600",  # Ferrari Red
        "#00D2BE",  # Mercedes Teal
        "#0600EF",  # Red Bull Blue
        "#FF8700",  # McLaren Orange
        "#006F62",  # Aston Martin Green
        "#2B4562",  # AlphaTauri Blue
        "#B6BABD",  # Haas Gray
        "#C92D4B",  # Alfa Romeo Red
        "#005AFF",  # Williams Blue
        "#FF80C7",  # Alpine Pink
    ]

    def __init__(self, style: str = "seaborn-v0_8-darkgrid"):
        self._style = style
        self._dpi = 100
        self._figsize = (10, 6)

    def _fig_to_base64(self, fig: plt.Figure) -> str:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=self._dpi, bbox_inches="tight")
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode("utf-8")
        buf.close()
        plt.close(fig)
        return img_base64

    def _get_colors(self, n: int) -> list[str]:
        if n <= len(self.COLORS):
            return self.COLORS[:n]
        # Cycle colors if more are needed
        return [self.COLORS[i % len(self.COLORS)] for i in range(n)]

    def create_bar_chart(
        self,
        labels: list[str],
        values: list[float],
        title: str,
        xlabel: str = "",
        ylabel: str = "",
        horizontal: bool = False,
        color: Optional[str] = None,
    ) -> ChartResult:
        fig, ax = plt.subplots(figsize=self._figsize)

        bar_color = color or self.COLORS[0]

        if horizontal:
            bars = ax.barh(labels, values, color=bar_color)
            ax.set_xlabel(ylabel)
            ax.set_ylabel(xlabel)
        else:
            bars = ax.bar(labels, values, color=bar_color)
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            plt.xticks(rotation=45, ha="right")

        ax.set_title(title, fontsize=14, fontweight="bold")
        plt.tight_layout()

        summary = {
            "total": sum(values),
            "average": np.mean(values),
            "max": max(values),
            "min": min(values),
            "count": len(values),
        }

        description = (
            f"Bar chart showing {len(labels)} categories. "
            f"Max: {max(values):.2f}, Min: {min(values):.2f}, "
            f"Average: {np.mean(values):.2f}"
        )

        return ChartResult(
            chart_base64=self._fig_to_base64(fig),
            chart_type=ChartType.HORIZONTAL_BAR if horizontal else ChartType.BAR,
            title=title,
            description=description,
            data_summary=summary,
        )

    def create_line_chart(
        self,
        x_values: list[Any],
        y_series: dict[str, list[float]],
        title: str,
        xlabel: str = "",
        ylabel: str = "",
    ) -> ChartResult:
        fig, ax = plt.subplots(figsize=self._figsize)

        colors = self._get_colors(len(y_series))

        for (name, values), color in zip(y_series.items(), colors):
            ax.plot(x_values[:len(values)], values, label=name, color=color, 
                    marker="o", linewidth=2, markersize=5)

        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.legend(loc="best")
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        summary = {
            "series_count": len(y_series),
            "x_range": [str(x_values[0]), str(x_values[-1])] if x_values else [],
            "series_stats": {
                name: {
                    "max": max(vals) if vals else 0,
                    "min": min(vals) if vals else 0,
                    "avg": np.mean(vals) if vals else 0,
                }
                for name, vals in y_series.items()
            },
        }

        description = f"Line chart with {len(y_series)} series over {len(x_values)} data points."

        return ChartResult(
            chart_base64=self._fig_to_base64(fig),
            chart_type=ChartType.LINE,
            title=title,
            description=description,
            data_summary=summary,
        )

    def create_grouped_bar_chart(
        self,
        categories: list[str],
        groups: dict[str, list[float]],
        title: str,
        xlabel: str = "",
        ylabel: str = "",
    ) -> ChartResult:
        fig, ax = plt.subplots(figsize=self._figsize)

        x = np.arange(len(categories))
        width = 0.8 / len(groups)
        colors = self._get_colors(len(groups))

        for i, ((name, values), color) in enumerate(zip(groups.items(), colors)):
            offset = (i - len(groups) / 2 + 0.5) * width
            ax.bar(x + offset, values, width, label=name, color=color)

        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(categories, rotation=45, ha="right")
        ax.legend(loc="best")
        plt.tight_layout()

        summary = {
            "categories": len(categories),
            "groups": len(groups),
            "group_names": list(groups.keys()),
        }

        description = f"Grouped bar chart comparing {len(groups)} groups across {len(categories)} categories."

        return ChartResult(
            chart_base64=self._fig_to_base64(fig),
            chart_type=ChartType.GROUPED_BAR,
            title=title,
            description=description,
            data_summary=summary,
        )

    def create_scatter_chart(
        self,
        x_values: list[float],
        y_values: list[float],
        title: str,
        xlabel: str = "",
        ylabel: str = "",
        labels: Optional[list[str]] = None,
        size: Optional[list[float]] = None,
    ) -> ChartResult:
        fig, ax = plt.subplots(figsize=self._figsize)

        sizes = size if size else [50] * len(x_values)
        scatter = ax.scatter(x_values, y_values, s=sizes, c=self.COLORS[0], 
                            alpha=0.7, edgecolors="white")

        if labels:
            for i, label in enumerate(labels[:10]):
                ax.annotate(label, (x_values[i], y_values[i]), 
                           fontsize=8, ha="center", va="bottom")

        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        if len(x_values) > 1:
            correlation = np.corrcoef(x_values, y_values)[0, 1]
        else:
            correlation = 0

        summary = {
            "point_count": len(x_values),
            "correlation": correlation,
            "x_range": [min(x_values), max(x_values)],
            "y_range": [min(y_values), max(y_values)],
        }

        description = (
            f"Scatter plot with {len(x_values)} points. "
            f"Correlation: {correlation:.3f}"
        )

        return ChartResult(
            chart_base64=self._fig_to_base64(fig),
            chart_type=ChartType.SCATTER,
            title=title,
            description=description,
            data_summary=summary,
        )

    def create_heatmap(
        self,
        data: list[list[float]],
        x_labels: list[str],
        y_labels: list[str],
        title: str,
        cmap: str = "RdYlGn_r",
    ) -> ChartResult:
        fig, ax = plt.subplots(figsize=(max(10, len(x_labels) * 0.8), 
                                        max(6, len(y_labels) * 0.5)))

        data_array = np.array(data)
        im = ax.imshow(data_array, cmap=cmap, aspect="auto")

        cbar = ax.figure.colorbar(im, ax=ax)

        ax.set_xticks(np.arange(len(x_labels)))
        ax.set_yticks(np.arange(len(y_labels)))
        ax.set_xticklabels(x_labels, rotation=45, ha="right")
        ax.set_yticklabels(y_labels)

        # Add text annotations
        for i in range(len(y_labels)):
            for j in range(len(x_labels)):
                if i < len(data) and j < len(data[i]):
                    text = ax.text(j, i, f"{data[i][j]:.2f}",
                                  ha="center", va="center", 
                                  fontsize=8, color="black")

        ax.set_title(title, fontsize=14, fontweight="bold")
        plt.tight_layout()

        summary = {
            "rows": len(y_labels),
            "columns": len(x_labels),
            "max_value": float(data_array.max()),
            "min_value": float(data_array.min()),
            "mean_value": float(data_array.mean()),
        }

        description = f"Heatmap with {len(y_labels)} rows and {len(x_labels)} columns."

        return ChartResult(
            chart_base64=self._fig_to_base64(fig),
            chart_type=ChartType.HEATMAP,
            title=title,
            description=description,
            data_summary=summary,
        )

    def create_box_plot(
        self,
        data: dict[str, list[float]],
        title: str,
        xlabel: str = "",
        ylabel: str = "",
    ) -> ChartResult:
        fig, ax = plt.subplots(figsize=self._figsize)

        labels = list(data.keys())
        values = list(data.values())

        bp = ax.boxplot(values, labels=labels, patch_artist=True)

        colors = self._get_colors(len(labels))
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=14, fontweight="bold")
        plt.xticks(rotation=45, ha="right")
        ax.grid(True, alpha=0.3, axis="y")
        plt.tight_layout()

        summary = {
            "categories": len(labels),
            "statistics": {
                name: {
                    "median": float(np.median(vals)),
                    "mean": float(np.mean(vals)),
                    "std": float(np.std(vals)),
                    "min": float(min(vals)),
                    "max": float(max(vals)),
                    "count": len(vals),
                }
                for name, vals in data.items()
            },
        }

        description = f"Box plot showing distribution across {len(labels)} categories."

        return ChartResult(
            chart_base64=self._fig_to_base64(fig),
            chart_type=ChartType.BOX,
            title=title,
            description=description,
            data_summary=summary,
        )


_chart_service: Optional[ChartService] = None


def get_chart_service() -> ChartService:
    global _chart_service
    if _chart_service is None:
        _chart_service = ChartService()
    return _chart_service

