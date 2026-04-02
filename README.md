# 🚇 BTS/MRT Ridership Analytics Pipeline

End-to-end data engineering pipeline analyzing Bangkok's mass transit ridership data. Built to demonstrate modern DE practices: ETL from multiple sources, data warehouse design, transformation with dbt, orchestration with Airflow, and visualization with Metabase.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                          │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐ │
│  │ BEM Website  │  │ BTS PDF      │  │ Google Maps API     │ │
│  │ (MRT data)   │  │ (BTS data)   │  │ (Travel times)      │ │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬──────────┘ │
└─────────┼─────────────────┼─────────────────────┼────────────┘
          │                 │                     │
          ▼                 ▼                     ▼
┌──────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION (Airflow)                   │
│  Monthly DAG: extract → load → transform → test              │
│  Daily DAG:   capture travel times (4x/day)                  │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE (PostgreSQL)                 │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐ │
│  │ raw.*    │ →  │ staging.*    │ →  │ marts.*            │ │
│  │ as-is    │    │ cleaned      │    │ analytics-ready    │ │
│  └──────────┘    └──────────────┘    └────────────────────┘ │
│                    (dbt models)         (dbt models)         │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                    DASHBOARD (Metabase)                       │
│  • Ridership trends & YoY growth                             │
│  • Line comparison & market share                            │
│  • Peak hour analysis & travel times                         │
│  • COVID recovery tracking                                   │
└──────────────────────────────────────────────────────────────┘
```

## Data Model (Star Schema)

```
                    ┌──────────────┐
                    │  dim_date    │
                    │──────────────│
                    │ date_id (PK) │
                    │ full_date    │
                    │ day_of_week  │
                    │ is_weekend   │
                    │ is_holiday   │
                    │ month, year  │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌──────┴───────┐   ┌──────────────┐
│ dim_station  │   │fact_ridership│   │  dim_line     │
│──────────────│   │──────────────│   │──────────────│
│ station_key  │◄──│ station_key  │──►│ line_key     │
│ station_code │   │ date_id      │   │ line_name    │
│ name_en/th   │   │ line_key     │   │ operator     │
│ line_name    │   │ daily_pax    │   │ color        │
│ lat, lng     │   │ revenue_thb  │   │ fare_min/max │
│ is_interchange│  │ data_source  │   │ total_km     │
└──────────────┘   └──────────────┘   └──────────────┘
```

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.10+
- Git

### Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/bts-mrt-pipeline.git
cd bts-mrt-pipeline

# First-time setup
make setup

# Edit .env with your API keys (optional for basic pipeline)
nano .env

# Start all services
make up
```

### Access

| Service     | URL                     | Credentials        |
|-------------|-------------------------|---------------------|
| Airflow     | http://localhost:8080    | admin / admin       |
| Metabase    | http://localhost:3000    | (setup on first visit) |
| PostgreSQL  | localhost:5434           | warehouse / warehouse123 |

### Run Pipeline Manually

```bash
# Extract data
make extract-bem        # MRT ridership from BEM
make extract-bts        # BTS ridership from PDFs
make extract-travel     # Travel times from Google Maps

# Transform with dbt
make dbt-run            # Run all models
make dbt-test           # Run data quality tests
make dbt-docs           # Generate documentation site
```

## Project Structure

```
bts-mrt-pipeline/
├── dags/                          # Airflow DAGs
│   └── ridership_pipeline.py      # Monthly + daily DAGs
├── dbt/                           # dbt project
│   ├── models/
│   │   ├── staging/               # Clean raw data
│   │   │   ├── stg_bem_ridership.sql
│   │   │   ├── stg_bts_ridership.sql
│   │   │   ├── stg_stations.sql
│   │   │   ├── sources.yml
│   │   │   └── schema.yml
│   │   ├── intermediate/          # Combine sources
│   │   │   └── int_ridership_combined.sql
│   │   └── marts/                 # Analytics-ready
│   │       ├── mart_line_comparison.sql
│   │       └── mart_peak_analysis.sql
│   ├── tests/                     # Custom data tests
│   │   └── assert_ridership_within_range.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/                       # Python ETL scripts
│   ├── extract_bem_ridership.py   # BEM web scraper
│   ├── extract_bts_pdf.py         # BTS PDF extractor
│   └── extract_travel_times.py    # Google Maps API
├── docker/
│   └── init-warehouse.sql         # Schema + seed data
├── .github/workflows/
│   └── ci.yml                     # CI/CD pipeline
├── data/
│   └── pdfs/                      # Place BTS PDFs here
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Data Sources

| Source | Type | Frequency | URL |
|--------|------|-----------|-----|
| BEM Investor Relations | Web (HTML table) | Monthly | [investor.bemplc.co.th](https://investor.bemplc.co.th/en/ridership-report/ridership) |
| BTS Annual Report | PDF | Annually | [bts.co.th](https://www.bts.co.th/eng/library/system-annual.html) |
| Google Maps API | REST API | Daily (4x) | [Distance Matrix API](https://developers.google.com/maps/documentation/distance-matrix) |
| Station data | Wikipedia/MRTA | One-time | Seeded in init-warehouse.sql |

## Key Insights (Example)

- **COVID Recovery**: MRT Blue Line ridership recovered to ~453K daily passengers by Aug 2024, surpassing pre-COVID levels
- **20-Baht Fare Cap Policy**: Significant ridership increase on Purple and Red lines after Oct 2023 fare cap
- **New Line Impact**: Yellow Line (opened Jul 2023) and Pink Line (opened Feb 2024) added ~50K+ daily passengers to the network
- **Peak Patterns**: Morning rush (7-9 AM) adds 15-25 minutes to travel times between interchange stations

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Extraction | Python, requests, BeautifulSoup, pdfplumber |
| Orchestration | Apache Airflow |
| Storage | PostgreSQL |
| Transformation | dbt-core |
| Visualization | Metabase |
| Containerization | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| Alerting | LINE Notify (optional) |

## Skills Demonstrated

- **Data Modeling**: Star schema design with dimensions and facts
- **ETL**: Multi-source extraction (API, web scraping, PDF parsing)
- **SQL**: Window functions, CTEs, YoY calculations
- **dbt**: Staging/intermediate/mart layers, tests, documentation
- **Orchestration**: Airflow DAGs with dependencies and error handling
- **DevOps**: Docker Compose, CI/CD, environment management
- **Data Quality**: Schema tests, custom tests, alerting

## License

MIT - Feel free to use this as a portfolio project.
