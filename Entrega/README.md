# Data Engineering Projects - Master in Artificial Intelligence

This repository contains two data engineering projects implementing dimensional models and ETL pipelines.

## Project Structure

```
Entrega/
├── Task 1/                  # Sales & Flights Analysis Projects
│   ├── config.yml          # Sales data configuration
│   ├── configFlights.yml   # Flights data configuration
│   ├── main.py            # Main ETL pipeline
│   ├── config.py          # Configuration loader
│   ├── dataProcessing.py  # Data transformation logic
│   ├── dbUtils.py         # Database utilities
│   └── restos.py          # Utility functions
├── Task 2/                  # F1 Data Warehouse Project
│   ├── config_f1.yml      # F1 data configuration
│   ├── main.py            # Main ETL pipeline
│   ├── config.py          # Configuration loader
│   ├── dataProcessing.py  # Data transformation logic
│   ├── dbUtils.py         # Database utilities
│   ├── requirements.txt   # Python dependencies
│   └── README.md          # Detailed documentation
├── dataFlightsLab.sql      # Flights database schema
└── dataF1.sql              # F1 database schema
```

## Task 1: Sales & Flights Analysis

A dimensional model for analyzing sales data and flight operations. Features include:
- Sales performance tracking by customer, product, and date
- Flight operations analysis with airline, location, and date dimensions
- Configurable ETL pipeline supporting multiple data sources

### Running Task 1

```bash
cd "Entrega/Task 1"
python main.py config.yml          # For sales analysis
python main.py configFlights.yml   # For flights analysis
```

## Task 2: Formula 1 Data Warehouse

A comprehensive F1 data warehouse implementing three star schemas:

### Star Schema 1: Qualification Performance
Analyzes driver and constructor performance during qualification sessions with dimensions:
- Driver (personal info, nationality)
- Constructor (team info)
- Circuit (track location, characteristics)
- Race (event details)

### Star Schema 2: Pit Stops Analysis
Tracks pit stop strategies and durations with:
- Shared dimensional tables (Driver, Race)
- Detailed pit stop timing and lap information

### Star Schema 3: Race Results
Comprehensive race outcome analysis including:
- Final positions and points
- Fastest lap times
- Race completion status
- All shared dimensions plus Status dimension

### Running Task 2

```bash
cd "Entrega/Task 2"
python main.py config_f1.yml
```

See `Task 2/README.md` for detailed setup instructions.

## Common Features

Both projects share:
- **Modular architecture**: Separate modules for configuration, data processing, and database operations
- **YAML-based configuration**: Flexible mapping between source data and target schema
- **Chunk processing**: Efficient handling of large CSV files
- **Data cleaning**: Configurable rules for handling duplicates and NULL values
- **Dimension management**: Automatic hash generation for surrogate keys
- **Foreign key resolution**: Automatic FK lookup and mapping for fact tables

## Requirements

- Python 3.x
- MySQL 5.7+ or compatible DBMS
- Python packages:
  - pandas
  - sqlalchemy
  - pymysql
  - PyYAML

Install all dependencies:
```bash
pip install pandas sqlalchemy pymysql pyyaml
```

## Configuration

Each project uses a YAML configuration file defining:
- Data sources and merge operations
- Database connection parameters
- Table mappings (source → target)
- Data cleaning rules
- Dimension vs fact table designation

## Architecture

### ETL Pipeline Flow

1. **Extract**: Read CSV files (with optional merging)
2. **Transform**:
   - Map source columns to target schema
   - Apply transformations (date parsing, hash generation)
   - Handle NULL values per configuration
   - Deduplicate records
3. **Load**:
   - Process dimensions first
   - Resolve foreign keys
   - Load fact tables
   - Batch insertion for performance

### Key Components

- `main.py`: Orchestrates the ETL process
- `config.py`: Parses YAML configuration and provides access to settings
- `dataProcessing.py`: Implements data transformation logic
- `dbUtils.py`: Handles database connections and operations

## Notes

- Create target databases before running pipelines
- Configure database credentials in YAML files
- Place source CSV files in appropriate directories
- Check logs for processing progress and errors
- Subsequent runs skip table creation if schema exists
