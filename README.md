# TEC Energy Data Ingestion Service

A complete data pipeline that downloads, validates, and stores natural gas shipment data from Energy Transfer's TW pipeline system into a PostgreSQL database.

## Overview

This service downloads operationally available capacity data from Energy Transfer's public API for the last 3 days, validates the data structure, and uploads it to PostgreSQL. The system processes data from 6 different cycles per day (timely, evening, intraday_1-3, final).

## Features

- **Complete Data Pipeline**: Downloads, validates, and uploads data in a single workflow
- **Multi-cycle Processing**: Handles 6 cycle types per day (cycles 0, 1, 3, 4, 5, 7)
- **Data Validation**: Validates CSV structure, data types, and content before database insertion
- **PostgreSQL Integration**: Automatic table creation and data uploading
- **Docker Support**: Complete containerized setup with PostgreSQL database
- **Flexible Scheduling**: Run once or continuously at specified intervals
- **Error Handling**: Comprehensive error handling with detailed logging

## Project Structure

```
tec-data-ingestion-service/
├── src/
│   ├── __init__.py
│   ├── downloader.py      # CSV downloader for Energy Transfer data
│   ├── uploader.py        # PostgreSQL database uploader
│   ├── scheduler.py       # Task scheduler for automated runs
│   ├── validator.py       # Data validation logic
│   └── main.py            # Main pipeline orchestrator
├── sql/
│   └── init.sql          # Database initialization script
├── data/                 # Directory for downloaded CSV files
├── docker-compose.yml    # Docker services configuration
├── Dockerfile           # Container configuration
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Quick Start

### Docker (Recommended)

**Prerequisites**: Docker and Docker Compose installed on your system

1. **Start the services**:
   ```bash
   docker-compose up --build
   ```
   This will start PostgreSQL database, build the application container, and process existing CSV files in the `data/` directory.

2. **Run the complete pipeline**:
   ```bash
   docker-compose exec app python3 src/main.py
   ```

3. **Run with different options**:
   ```bash
   # Skip download and process existing files
   docker-compose run --rm app python3 src/main.py --skip-download
   
   # Run continuously every 2 hours
   docker-compose run --rm app python3 src/main.py --continuous --interval 2
   
   # Test database connection
   docker-compose run --rm app python3 src/main.py --test-db
   ```

4. **Access the database**:
   - Host: `localhost`
   - Port: `5432`
   - Database: `tec_data`
   - User: `tec_user` 
   - Password: `tec_password`

5. **Stop the services**:
   ```bash
   # Stop containers
   docker-compose down
   
   # Stop and remove database data
   docker-compose down -v
   ```

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up PostgreSQL** and configure environment variables:
   ```bash
   export DB_HOST=localhost
   export DB_NAME=tec_data
   export DB_USER=postgres
   export DB_PASSWORD=your_password
   ```

3. **Run the pipeline**:
   ```bash
   python3 src/main.py
   ```

## Usage

### Complete Pipeline

Run the full data ingestion workflow (download → validate → upload):

```bash
# Run once (default: last 3 days)
python3 src/main.py

# Run continuously every 6 hours
python3 src/main.py --continuous

# Custom interval (every 2 hours)
python3 src/main.py --continuous --interval 2

# Skip download, process existing files
python3 src/main.py --skip-download

# Download and validate only (skip upload)
python3 src/main.py --skip-upload

# Test database connection
python3 src/main.py --test-db
```

### Individual Components

```bash
# Download only
python3 src/downloader.py

# Validate existing CSV files
python3 src/validator.py

# Upload to database
python3 src/uploader.py
```

## Data Source

The CSV files are downloaded from Energy Transfer's TW pipeline system:
- **Base URL**: `https://twtransfer.energytransfer.com/ipost/capacity/operationally-available`
- **Data Type**: Operationally available capacity for natural gas shipments
- **Update Frequency**: 6 cycles per day (timely=0, evening=1, intraday_1=3, intraday_2=4, final=5, intraday_3=7)
- **Coverage**: TW (Transwestern) pipeline system

## File Naming Convention

Downloaded files are saved with the following naming pattern:
```
tec_data_YYYYMMDD_cycle_N.csv
```

Examples:
- `tec_data_20250607_cycle_0.csv` (timely cycle)
- `tec_data_20250607_cycle_1.csv` (evening cycle)
- `tec_data_20250607_cycle_5.csv` (final cycle)

## Database Configuration

The service uses PostgreSQL to store pipeline data. Database connection can be configured via environment variables:

- `DB_HOST` (default: `localhost`)
- `DB_NAME` (default: `tec_data`)
- `DB_USER` (default: `postgres`)
- `DB_PASSWORD` (default: `password`)
- `DB_PORT` (default: `5432`)

The database table is automatically created with the schema defined in `sql/init.sql`.

## CSV Data Format

Each CSV contains the following columns:
- `Loc`: Location ID
- `Loc Zn`: Location Zone  
- `Loc Name`: Location Name
- `Loc Purp Desc`: Location Purpose Description
- `Loc/QTI`: Location/QTI indicator
- `Flow Ind`: Flow Indicator (D/R for Delivery/Receipt)
- `DC`: Designed Capacity
- `OPC`: Operational Capacity
- `TSQ`: Total Scheduled Quantity
- `OAC`: Operationally Available Capacity
- `IT`: Interruptible Transportation
- `Auth Overrun Ind`: Authorized Overrun Indicator
- `Nom Cap Exceed Ind`: Nomination Capacity Exceed Indicator
- `All Qty Avail`: All Quantity Available
- `Qty Reason`: Quantity Reason

## Requirements

- Python 3.7+
- PostgreSQL database
- Docker and Docker Compose (for containerized setup)
- Required Python packages (see `requirements.txt`)

## License

This project is developed as part of a candidate assessment for TEC Energy.
