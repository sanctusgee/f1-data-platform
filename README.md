# F1 Data Platform

A production-ready Formula 1 analytics pipeline built with Python, dbt, and PostgreSQL. This platform ingests F1 race data from the Jolpica Ergast API, transforms it through a layered dbt architecture, and provides analytics-ready models for race strategy insights.

## Features

- **Robust Data Ingestion**: Auto-retry logic, rate limiting, and error handling for reliable API data collection
- **Professional Architecture**: Modular Python package with clean separation of concerns
- **dbt Transformations**: Staging to Intermediate to Marts layered approach for analytical modeling
- **Production-Ready**: Comprehensive logging, configuration management, and dependency handling
- **Race Analytics**: Driver performance, stint analysis, pit stop strategy, and lap time insights

## Architecture

```
f1-data-platform/
├── f1_pipeline/                    # Core ingestion package
│   ├── config/                     # Configuration management
│   ├── ingestion/                  # API client, data transformation, orchestration
│   └── utils/                      # Database utilities
├── scripts/                        # Execution scripts
├── models/                         # dbt transformation layer
│   ├── staging/                    # Raw data cleaning
│   ├── intermediate/               # Business logic calculations
│   └── marts/                      # Summary datasets for analytics
│   └── analytics/                  # Strategic insights and performance metrics
├── macros/                         # dbt SQL utilities
└── tests/                          # Data quality tests
```

## Quick Start

### Prerequisites

- PostgreSQL (local, Docker, or cloud instance)
- Python 3.10+
- Conda or pip for dependency management
- dbt for data transformations

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/sanctusgee/f1-data-platform
   cd f1-data-platform
   ```

2. **Create and activate environment**
   ```bash
   conda create -n f1-data-platform python=3.11
   conda activate f1-data-platform
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   dbt deps
   
   # OR for development
   pip install -e .
   dbt deps
   ```

4. **Create PostgreSQL database**
   ```sql
   CREATE DATABASE f1_analytics;
   ```

5. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

### Configuration

Create a `.env` file in the project root:

```env
API_BASE_URL=http://api.jolpi.ca/ergast
DB_HOST=localhost
DB_PORT=5432
DB_NAME=f1_analytics
DB_USER=your_username
DB_PASSWORD=your_password
```

### Usage

1. **Run data ingestion**
   ```bash
   python scripts/ingest_f1_data.py
   ```

2. **Run dbt transformations**
   ```bash
   dbt run
   ```

3. **View analytics data**
   ```sql
   SELECT * FROM mart_driver_race_summary LIMIT 10;
   SELECT * FROM mart_stint_degradation_summary LIMIT 10;
   ```

## Data Models

### Staging Layer
- `stg_lap_times`: Cleaned lap timing data with standardized formats
- `stg_pit_stops`: Processed pit stop events with duration calculations

### Intermediate Layer
- `int_stint_segmentation`: Tire stint identification and analysis
- `int_driver_lap_deltas`: Lap time comparisons and delta calculations
- `int_stint_degradation`: Tire performance degradation analysis

### Analytics Layer (Marts)
- `mart_driver_race_summary`: Driver performance metrics by race
- `mart_stint_summary`: Comprehensive stint analysis
- `mart_qualifying_consistency`: Qualifying performance patterns
- `mart_session_progression`: Session-by-session performance tracking

## Development

### Project Structure

The platform follows professional Python packaging standards with clear separation of concerns:

- **f1_pipeline/**: Core package containing all ingestion logic
- **scripts/**: Execution scripts that orchestrate workflows  
- **models/**: dbt transformation layer with staging to intermediate to marts flow
- **macros/**: Reusable SQL functions for dbt models

### Key Components

- **API Client**: Handles HTTP requests with exponential backoff retry logic
- **Data Transformer**: Converts nested JSON to structured DataFrames
- **Orchestrator**: Coordinates multi-season data collection workflows
- **Database Utilities**: Manages PostgreSQL connections and table operations

### Adding New Data Sources

1. Create new API methods in `f1_pipeline/ingestion/api_client.py`
2. Add transformation logic in `f1_pipeline/ingestion/data_transformer.py`
3. Update orchestrator to include new data types
4. Create corresponding dbt staging models

## Performance

- **Ingestion Rate**: ~4 requests/second (API limit compliant)
- **Data Volume**: 700+ lap records, 600+ pit stops per season
- **Transformation Speed**: Complete dbt run in under 2 seconds
- **Error Handling**: Exponential backoff with 5 retry attempts

## Testing

This project uses dbt's built-in testing framework to ensure data quality and model integrity.

### Running Tests

```bash
# Install dependencies (required for first-time setup)
dbt deps

# Run all tests
dbt test

# Run tests for specific models
dbt test --select stg_lap_times
dbt test --select mart_qualifying_consistency

# Run only staging tests
dbt test --select staging.*

# Run tests and see detailed output
dbt test --verbose
```

### Test Types

- **Schema tests**: Data validation (not null, unique values, accepted ranges)
- **Custom tests**: Business logic validation (e.g., stint segmentation logic)
- **dbt_utils tests**: Advanced validations using the dbt_utils package


### Troubleshooting

If tests fail:
1. Check the compiled SQL in `target/compiled/` for the specific test
2. Run the failing query directly in your database to investigate
3. Adjust test parameters in `schema.yml` files if needed

Tests should pass after running `dbt run` to ensure all models are built with the latest data.

## Documentation

- **Part 1: API Selection** - Choosing the right F1 data source
- **Part 2: Ingestion Pipeline** - Building production-ready data ingestion
- **Part 3: dbt Transformations** - Creating analytical models

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-analysis`)
3. Make your changes
4. Add tests for new functionality
5. Run `dbt test` to ensure data quality
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Jolpica Ergast API](http://ergast.com/mrd/) for providing comprehensive F1 data
- [dbt](https://www.getdbt.com/) for the transformation framework
- [FastF1](https://github.com/theOehrly/Fast-F1) community for F1 analytics inspiration



---

## Legal Disclaimer

This project is not affiliated with, endorsed by, or connected to Formula 1, the FIA, or any F1 teams or drivers. Formula 1, F1, and related trademarks are owned by Formula One Licensing B.V. This is an independent, non-commercial project for educational and analytical purposes only.

The data used in this project is sourced from publicly available APIs and is used in compliance with their terms of service. This project does not redistribute official F1 data or content.