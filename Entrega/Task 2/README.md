# Task 2: F1 Data Warehouse ETL Pipeline

This project implements a dimensional model and ETL pipeline for analyzing Formula 1 data, focusing on:
1. **Qualification Performance** - Driver and constructor performance during qualification sessions
2. **Pit Stops Analysis** - Pit stop strategies and durations during races
3. **Race Results** - Final race positions, points, and fastest laps

## Structure

The project consists of three star schemas:
- **Schema 1**: Qualification results with dimensions for drivers, constructors, circuits, and races
- **Schema 2**: Pit stops analysis sharing the same dimensional tables
- **Schema 3**: Race results with additional status dimension

## Requirements

- Python 3.x
- MySQL or compatible DBMS
- Required Python libraries:
  - pandas
  - sqlalchemy
  - pymysql
  - PyYAML

Install dependencies:
```bash
pip install pandas sqlalchemy pymysql pyyaml
```

## Data Sources

Place the F1 Kaggle dataset CSV files in the `../../DATASETS/F1/` directory relative to this folder:
- races.csv
- drivers.csv
- constructors.csv
- circuits.csv
- qualifying.csv
- pit_stops.csv
- results.csv
- status.csv

Dataset available at: https://www.kaggle.com/code/kevinkwan/formula-1-pit-stops-analysis/input

## Configuration

Edit `config_f1.yml` to configure:
- Database connection parameters (user, password, host, port)
- Data source file paths (if different from default)

## Running the Pipeline

1. Ensure MySQL is running and the database `f1` is created:
```sql
CREATE DATABASE IF NOT EXISTS f1;
```

2. Run the ETL pipeline:
```bash
python main.py config_f1.yml
```

The pipeline will:
1. Create the database schema (tables, constraints)
2. Process and merge source CSV files
3. Extract and transform dimension data
4. Load dimension tables
5. Extract and transform fact data
6. Load fact tables

## Database Schema

The SQL schema is defined in `../dataF1.sql` and includes:

**Dimension Tables:**
- `dim_driver` - Driver information
- `dim_constructor` - Constructor/team information
- `dim_circuit` - Circuit/track information
- `dim_race` - Race events
- `dim_status` - Race result status codes

**Fact Tables:**
- `fact_qualifying` - Qualification session results
- `fact_pitstops` - Pit stop events
- `fact_results` - Race final results

## Notes

- The pipeline processes data in chunks to handle large datasets efficiently
- Duplicate records are handled according to configuration rules
- NULL values are managed per table configuration
- The database schema is automatically created on first run
- Subsequent runs will skip schema creation if tables already exist
