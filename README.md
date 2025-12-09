# Formula 1 Big Data Projetc

## 1. Overview

This project implements an end-to-end **data processing pipeline** for the
**Formula 1 World Championship (1950–2025)** dataset using **PySpark on Databricks**.

The pipeline follows a **Lakehouse / Medallion** architecture:

- **Bronze** → raw, schema-enforced data from CSVs  
- **Silver** → cleaned, joined, analytics-ready tables  
- **Gold** → feature-engineered tables for analysis and ML


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
    B1[f1_bronze_data]
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
    O4[f1_pipeline_driver]
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


  %% Bronze to Silver
  B1 --> O2

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


  %% Orchestration driver
  O1 --> O2 --> O3
  O4 -. optional .- O3

  G1 -->|"EDA, reports"| Users
  G2 -->|"Team analytics"| Users
  G3 -->|"ML models, dashboards"| Users
