# **Schwab Market Data Pipeline**

## ****Overview****

This project is an end-to-end data engineering pipeline that ingests equity and options market data from the Schwab API, 
processes the data through a medallion architecture (Bronze, Silver, Gold), and stores analytical datasets in PostgreSQL
for downstream reporting and visualization.

The project demonstrates modern data engineering concepts including:
* API data ingestion
* Data lake architecture
* Parquet storage
* ETL/ELT processing
* PostgreSQL analytics
* Materialized views
* Data quality and deduplication
* Containerized deployment

## Data Architecture

**Batch Streaming**

![!plot](images\trading_platform batch streaming data architecture.png)

## Technology Stack


| Category         | Technology          |
|------------------|---------------------|
| Language         | Python              |
| Data Storage     | Parquet             |
| Database         | PostgreSQL          |
| Data Processing  | Pandas              |
| Containerization | Docker              |
| Scheduling       | Airflow (planned)   |
| Visualization    | Streamlit (planned) |
| Market Data      | Schwab API          |


## Data Layers
### Bronze Layer
#### Purpose

Preserve raw API responses
Historical storage
Source-of-truth ingestion layer

#### Characteristics

Raw data
Append-only
Minimal transformation

#### Partitioning Example:

bronze/asset_type=equity/date=YYYY-MM-DD/

### Silver Layer
#### Purpose

Structured and validated market data
Parsed JSON responses
Standardized schema

#### Transformations

Data typing
Column normalization
Deduplication
Data quality validation

#### Example Fields:

**Equities**
* symbol
* timestamp
* open
* high
* low
* close
* volume

**Options**
* underlying
* expiration
* strike
* option_type
* volume
* open_interest


### Gold Layer

#### Purpose

Analytics-ready datasets
Dashboard consumption
Business-focused metrics

#### Current Gold Views:

**equity_hourly_metrics**



    hourly_return
    rolling_volatility
    relative_volume
    equity_rankings

**equity_rankings**

    top gainers
    top losers
    volatility rankings
    volume rankings
    options_chain_summary

**options_chain_summary**

    put_call_ratio
    total_call_volume
    total_put_volume
    total_open_interest

**options_expiration_summary**

    contracts
    total_volume
    total_open_interest
