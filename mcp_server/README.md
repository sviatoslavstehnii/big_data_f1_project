# F1 Databricks MCP Server

A FastMCP server that acts as a gateway between AI agents (like Claude) and your Databricks workspace containing Formula 1 data. This server enables natural language interaction with F1 data through SQL queries, visualizations, and ML predictions.

## Architecture

```
┌─────────────┐     MCP Protocol      ┌─────────────────┐     Databricks SDK     ┌─────────────────┐
│   AI Agent  │  ←───────────────────→ │  FastMCP Server │ ←───────────────────→  │   Databricks    │
│  (Claude)   │      stdio/SSE        │   (Gateway)     │     REST API           │   Workspace     │
└─────────────┘                       └─────────────────┘                        └─────────────────┘
```

## Features

### SQL Query Tools
- **query_f1_data**: Execute read-only SQL queries against F1 tables
- **get_driver_season_stats**: Pre-built query for driver statistics
- **get_constructor_season_stats**: Pre-built query for team statistics
- **get_race_results**: Detailed race-level data
- **get_pit_stop_data**: Pit stop analysis data

### Schema Discovery Tools
- **list_f1_tables**: List available tables by data layer (bronze/silver/gold)
- **describe_table**: Get column definitions for a table
- **get_table_sample**: Preview sample data from a table
- **get_f1_data_overview**: Summary of available data

### Visualization Tools
- **chart_driver_performance**: Line/bar chart of driver stats over seasons
- **chart_team_comparison**: Compare multiple teams
- **chart_pit_stop_analysis**: Box plot or scatter of pit stop times
- **chart_correlation_heatmap**: Heatmap of feature correlations
- **chart_season_standings**: Championship standings chart
- **chart_custom**: Create chart from any SQL query

### ML Prediction Tools (Placeholder)
- **predict_pit_stops**: Predict optimal pit count or duration
- **get_model_info**: Information about the ML model
- **get_historical_pit_stats**: Historical context for predictions
- **analyze_race_factors**: Factor analysis for race performance

## Quick Start

### Prerequisites

- Python 3.10+
- Databricks workspace with F1 data tables
- Personal Access Token for Databricks

### Installation

1. Clone/copy the `mcp_server` directory to your project

2. Create a virtual environment and install dependencies:

```bash
cd mcp_server

# Using uv (recommended)
uv venv
source .venv/bin/activate
uv pip install -e .

# Or using pip
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

3. Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

4. Edit `.env` with your Databricks credentials:

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi_your_token_here
DATABRICKS_WAREHOUSE_ID=your_warehouse_id
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=f1
```

### Getting Databricks Credentials

1. **DATABRICKS_HOST**: Your Databricks workspace URL
   - Found in browser when logged into Databricks

2. **DATABRICKS_TOKEN**: Personal Access Token
   - Go to: User Settings → Developer → Access tokens
   - Generate new token with appropriate expiry

3. **DATABRICKS_WAREHOUSE_ID**: SQL Warehouse ID
   - Go to: SQL Warehouses → Select warehouse → Connection details
   - Copy the "HTTP Path" value and extract the warehouse ID

## Claude Desktop Integration

Add this configuration to your Claude Desktop config file:

**macOS**: `.mcp.json`
**Windows**: `.mcp.json`

```json
{
  "mcpServers": {
    "f1-databricks": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/absolute/path/to/mcp_server",
        "f1-mcp"
      ],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi_your_token_here",
        "DATABRICKS_WAREHOUSE_ID": "your_warehouse_id",
        "DATABRICKS_CATALOG": "workspace",
        "DATABRICKS_SCHEMA": "f1"
      }
    }
  }
}
```

**Alternative using Python directly:**

```json
{
  "mcpServers": {
    "f1-databricks": {
      "command": "/absolute/path/to/mcp_server/.venv/bin/python",
      "args": ["-m", "f1_mcp.server"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi_your_token_here",
        "DATABRICKS_WAREHOUSE_ID": "your_warehouse_id"
      }
    }
  }
}
```

After updating the config, restart Claude Desktop.

## Running the Server

### stdio transport (default - for Claude Desktop)

```bash
cd mcp_server
uv run f1-mcp
```

### SSE transport (for web-based agents)

```bash
cd mcp_server
uv run f1-mcp --sse
```

## Example Queries

Once connected, you can ask Claude questions like:

**Data Exploration:**
- "What tables are available in the F1 database?"
- "Show me the schema of the driver season stats table"
- "Get me the top 10 drivers by wins in 2023"

**Analysis:**
- "What factors most strongly influence race finish position?"
- "Which drivers consistently outperform their grid position?"
- "Compare Red Bull and Ferrari's performance over the last 5 seasons"

**Visualizations:**
- "Create a chart showing Hamilton's wins per season"
- "Show me a correlation heatmap of race performance factors"
- "Create a box plot of pit stop times by team for 2023"

**Predictions:**
- "Predict the optimal number of pit stops for a 60-lap race"
- "What's the expected pit stop duration for Red Bull?"

## Available Tables

### Gold Layer (Recommended)
- `f1.f1_gold_driver_season_stats` - Driver season aggregations
- `f1.f1_gold_constructor_season_stats` - Team season aggregations
- `f1.f1_gold_race_driver_features` - Race-level ML features

### Silver Layer (Detailed)
- `f1.f1_silver_race_results` - Individual race results
- `f1.f1_silver_pit_stops` - Pit stop records
- `f1.f1_silver_lap_times` - Lap-by-lap timing
- `f1.f1_silver_qualifying` - Qualifying results
- `f1.f1_silver_driver_standings` - Championship standings

## Extending the ML Model

The `predict_pit_stops` tool is a placeholder. To integrate your trained model:

1. Edit `src/f1_mcp/services/model_service.py`

2. Load your model in `__init__`:
```python
def __init__(self):
    self._model = load_your_model()  # Add your model loading
    self._model_loaded = True
    self._model_version = "your-model-v1.0"
```

3. Replace placeholder logic in prediction methods:
```python
def predict_optimal_pit_count(self, ...):
    features = prepare_features(...)  # Your feature engineering
    prediction = self._model.predict(features)  # Your model inference
    return PitStopPrediction(
        prediction_type=PredictionType.OPTIMAL_PIT_COUNT,
        optimal_pit_count=prediction,
        confidence=0.85,  # Your confidence calculation
        model_version=self._model_version,
        message="Prediction from trained model",
    )
```

## Project Structure

```
mcp_server/
├── pyproject.toml              # Dependencies & project config
├── .env.example                # Environment template
├── README.md                   # This file
├── src/
│   └── f1_mcp/
│       ├── __init__.py
│       ├── server.py           # FastMCP server entry point
│       ├── config.py           # Settings via pydantic-settings
│       ├── tools/
│       │   ├── sql_tools.py    # SQL query execution tools
│       │   ├── schema_tools.py # Schema/table discovery tools
│       │   ├── visualization_tools.py  # Charting tools
│       │   └── ml_tools.py     # ML prediction placeholders
│       ├── services/
│       │   ├── databricks_client.py  # Databricks SDK wrapper
│       │   ├── chart_service.py      # Matplotlib chart generation
│       │   └── model_service.py      # ML model inference
│       └── utils/
│           ├── validators.py   # SQL validation
│           └── formatters.py   # Result formatting
└── tests/
    └── test_tools.py
```

## Security

- Only SELECT, WITH, SHOW, and DESCRIBE queries are allowed
- Destructive operations (DROP, DELETE, etc.) are blocked
- SQL injection patterns are detected and blocked
- Environment variables store sensitive credentials

## Troubleshooting

**"No module named 'f1_mcp'"**
- Ensure you've installed the package: `pip install -e .`

**"Connection refused" or timeout errors**
- Verify your DATABRICKS_HOST is correct
- Check your token hasn't expired
- Ensure SQL warehouse is running

**"Permission denied" on tables**
- Verify your token has access to the catalog/schema
- Check table permissions in Databricks

**Charts not displaying**
- Charts are returned as base64-encoded PNG strings
- The agent should be able to render these in its response

## License

This project is part of a Big Data course final project.

