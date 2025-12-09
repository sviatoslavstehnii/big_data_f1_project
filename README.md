# Formula 1 Data Processing Pipeline (Bronze–Silver–Gold)

## 1. Overview

This project implements an end-to-end **data processing pipeline** for the
**Formula 1 World Championship (1950–20XX)** dataset using **PySpark on Databricks**.

The pipeline follows a **Lakehouse / Medallion** architecture:

- **Bronze** → raw, schema-enforced data from CSVs  
- **Silver** → cleaned, joined, analytics-ready tables  
- **Gold** → feature-engineered tables for analysis and ML

It covers the **Deliverable 1: Data Processing Pipeline** requirements:

- **Data Ingestion**
  - Batch ingestion from CSVs
  - Optional simulated streaming ingestion for lap times
  - Error handling & data validation with quarantine tables
- **Data Storage**
  - Databricks **DBFS + Delta Lake** (no S3 required)
  - Clear Bronze / Silver / Gold layers
  - Partitioning strategy by `season` (and `raceId` where needed)
- **Data Processing & Transformation**
  - Cleaning and preprocessing
  - Handling missing values and data quality issues
  - Feature engineering and enrichment in PySpark
- **Pipeline Orchestration**
  - Databricks **Workflows (Jobs)** to automate notebook execution
  - Optional scheduling & monitoring via the Jobs UI

---

## 2. Architecture Diagram (Data Flow)

```mermaid
flowchart LR

  subgraph Sources
    A1[Raw CSV: races.csv]
    A2[Raw CSV: drivers.csv]
    A3[Raw CSV: constructors.csv]
    A4[Raw CSV: results.csv]
    A5[Raw CSV: sprint_results.csv]
    A6[Raw CSV: lap_times.csv]
    A7[Raw CSV: pit_stops.csv]
    A8[Raw CSV: qualifying.csv]
    A9[Raw CSV: driver_standings.csv]
    A10[Raw CSV: constructor_standings.csv]
    A11[Raw CSV: circuits.csv]
    A12[Raw CSV: seasons.csv]
    A13[Raw CSV: status.csv]
  end

  subgraph Bronze["Bronze Layer (Raw Delta)"]
    B1[f1_bronze_races]
    B2[f1_bronze_drivers]
    B3[f1_bronze_constructors]
    B4[f1_bronze_results]
    B5[f1_bronze_sprint_results]
    B6[f1_bronze_lap_times]
    B7[f1_bronze_pit_stops]
    B8[f1_bronze_qualifying]
    B9[f1_bronze_driver_standings]
    B10[f1_bronze_constructor_standings]
    B11[f1_bronze_circuits]
    B12[f1_bronze_seasons]
    B13[f1_bronze_status]
    Berr1[f1_invalid_results]
    Berr2[f1_invalid_sprint_results]
    Berr3[f1_invalid_lap_times]
  end

  subgraph Silver["Silver Layer (Cleaned & Joined)"]
    S1[f1_silver_race_results]
    S2[f1_silver_sprint_results]
    S3[f1_silver_lap_times]
    S4[f1_silver_pit_stops]
    S5[f1_silver_qualifying]
    S6[f1_silver_driver_standings]
    S7[f1_silver_constructor_standings]
    S8[f1_silver_seasons]
  end

  subgraph Gold["Gold Layer (Feature Engineered)"]
    G1[f1_gold_driver_season_stats]
    G2[f1_gold_constructor_season_stats]
    G3[f1_gold_race_driver_features]
  end

  subgraph Orchestration["Orchestration (Databricks Jobs)"]
    O1[01_f1_ingest_bronze]
    O2[02_f1_transform_silver]
    O3[03_f1_build_gold]
    O4[(Optional) 04_f1_streaming_lap_times]
    O5[f1_pipeline_driver]
  end

  %% Sources to Bronze
  A1 --> O1
  A2 --> O1
  A3 --> O1
  A4 --> O1
  A5 --> O1
  A6 --> O1
  A7 --> O1
  A8 --> O1
  A9 --> O1
  A10 --> O1
  A11 --> O1
  A12 --> O1
  A13 --> O1

  O1 --> B1
  O1 --> B2
  O1 --> B3
  O1 --> B4
  O1 --> B5
  O1 --> B6
  O1 --> B7
  O1 --> B8
  O1 --> B9
  O1 --> B10
  O1 --> B11
  O1 --> B12
  O1 --> B13
  O1 --> Berr1
  O1 --> Berr2
  O1 --> Berr3

  %% Bronze to Silver
  B1 --> O2
  B2 --> O2
  B3 --> O2
  B4 --> O2
  B5 --> O2
  B6 --> O2
  B7 --> O2
  B8 --> O2
  B9 --> O2
  B10 --> O2
  B11 --> O2
  B12 --> O2
  B13 --> O2

  O2 --> S1
  O2 --> S2
  O2 --> S3
  O2 --> S4
  O2 --> S5
  O2 --> S6
  O2 --> S7
  O2 --> S8

  %% Silver to Gold
  S1 --> O3
  S2 --> O3
  S3 --> O3
  S4 --> O3
  S5 --> O3
  S6 --> O3
  S7 --> O3

  O3 --> G1
  O3 --> G2
  O3 --> G3

  %% Streaming (optional)
  A6 -. streaming lap csv .-> O4 -.-> B6

  %% Orchestration driver
  O1 --> O2 --> O3
  O4 -. optional .- O3

  G1 -->|"EDA, reports"| Users
  G2 -->|"Team analytics"| Users
  G3 -->|"ML models, dashboards"| Users
