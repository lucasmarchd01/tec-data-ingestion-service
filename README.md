# TEC Energy Data Ingestion Service - CSV Downloader

This is the first phase of the TEC Energy Data Ingestion Service project. This component handles downloading CSV files containing natural gas shipment data from Energy Transfer's TW pipeline system.

## Overview

The CSV downloader retrieves operationally available capacity data from Energy Transfer's public API. The system downloads data from the last 3 days, handling multiple cycles per day (typically 1-4 cycles).

## Features

- **Automated CSV Downloads**: Downloads natural gas shipment data from Energy Transfer
- **Multi-day Coverage**: Retrieves data from the last 3 days by default
- **Cycle Handling**: Attempts to download multiple cycles per day (1-4)
- **Error Handling**: Robust error handling with detailed logging
- **Flexible Scheduling**: Can run once or continuously at specified intervals
- **Data Organization**: Saves files with descriptive names including date and cycle

## Project Structure

```
tec-data-ingestion-service/
├── src/
│   ├── __init__.py
│   ├── downloader.py      # Main downloader class
│   ├── uploader.py        # Script to upload CSV data to PostgreSQL
│   ├── scheduler.py       # Simple scheduler for automated runs
│   ├── validator.py       # Data validation logic
│   └── main.py            # Unified entry point for the application
├── data/                 # Directory for downloaded CSV files (created automatically)
├── scripts/              # Utility scripts (e.g., for database setup)
├── requirements.txt       # Python dependencies
├── README.md             # This file
└── sample_data.csv       # Sample data file
```

## Installation

1. **Clone the repository** (when ready to publish):
   ```bash
   git clone <repository-url>
   cd tec-data-ingestion-service
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # or
   venv\Scripts\activate     # On Windows
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Run the Integrated Workflow (Download, Validate, Upload)

To run the complete data ingestion pipeline (download, validate, and upload):

```bash
python3 src/main.py
```

This command will execute the sequential workflow defined in `src/main.py`.

### Run Individual Components

#### Download CSVs

To download CSV files for the last 3 days:

```bash
python3 src/downloader.py
```

Or using the scheduler for more complex scenarios (e.g., continuous runs):

```bash
python3 src/scheduler.py
```

#### Validate Downloaded CSVs

To validate the CSV files in the `data/` directory:
```bash
python3 src/validator.py
```
*(Note: Specific command-line arguments for the validator might be added as the `validator.py` script is developed.)*


#### Upload Validated CSVs to Database

After downloading and validating CSV files, you can upload them to the PostgreSQL database using:

```bash
# Ensure your PostgreSQL server is running and configured (see Database Setup)
python3 src/uploader.py
```
Make sure to set the following environment variables for database connection, or update them in `src/uploader.py`:
- `DB_HOST` (defaults to `localhost`)
- `DB_NAME` (defaults to `tec_data`)
- `DB_USER` (defaults to `postgres`)
- `DB_PASSWORD` (defaults to `password`)
- `DB_PORT` (defaults to `5432`)


### Run Continuously (Scheduled Downloads via Scheduler)

To run the downloader continuously using the dedicated scheduler:

```bash
# Run every 6 hours (default)
python3 src/scheduler.py --continuous

# Run every 2 hours
python3 src/scheduler.py --continuous 2

# Run every 12 hours  
python3 src/scheduler.py --continuous 12
```
*(Note: The `main.py` script might also incorporate continuous run capabilities in the future.)*

### Stop Continuous Mode

Press `Ctrl+C` to stop the continuous scheduler.

## Data Source

The CSV files are downloaded from:
- **Base URL**: `https://twtransfer.energytransfer.com/ipost/capacity/operationally-available`
- **Data Type**: Operationally available capacity for natural gas shipments
- **Update Frequency**: Multiple times per day (cycles)
- **Coverage**: TW (Transwestern) pipeline system

### CSV Data Format

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

## Database Setup and Uploading

The `src/uploader.py` script handles uploading the downloaded CSV data into a PostgreSQL database.

**Features:**
- **Automated Table Creation**: Creates the `tec_data` table if it doesn't exist, matching the CSV structure.
- **Pandas & SQLAlchemy**: Uses pandas for efficient CSV parsing and data manipulation, and SQLAlchemy for robust database interaction.
- **Column Name Cleaning**: Automatically adjusts CSV column names to be database-friendly (e.g., "Loc Zn" becomes "loc_zn").
- **Append Data**: Appends data to the table, allowing for multiple runs without duplicating schema.
- **Environment Variable Configuration**: Database connection parameters can be set via environment variables.

**Prerequisites:**
- A running PostgreSQL server.
- `psycopg2-binary`, `pandas`, and `SQLAlchemy` Python packages (included in `requirements.txt`).

**Table Schema (`tec_data`):**
- `id`: SERIAL PRIMARY KEY
- `loc`: VARCHAR(255)
- `loc_zn`: VARCHAR(255)
- `loc_name`: VARCHAR(255)
- `loc_purp_desc`: VARCHAR(255)
- `loc_qti`: VARCHAR(255)
- `flow_ind`: VARCHAR(10)
- `dc`: INTEGER
- `opc`: INTEGER
- `tsq`: INTEGER
- `oac`: INTEGER
- `it`: VARCHAR(10)
- `auth_overrun_ind`: VARCHAR(10)
- `nom_cap_exceed_ind`: VARCHAR(10)
- `all_qty_avail`: VARCHAR(10)
- `qty_reason`: VARCHAR(255)

## File Naming Convention

Downloaded files are saved with the following naming pattern:
```
tec_data_YYYYMMDD_cycle_N.csv
```

Examples:
- `tec_data_20240118_cycle_1.csv`
- `tec_data_20240118_cycle_2.csv`
- `tec_data_20240117_cycle_3.csv`

## Logging

The application provides detailed logging including:
- Download progress and success/failure status
- File save locations
- Error messages and troubleshooting information
- Scheduling information when running continuously

Logs are printed to the console with timestamps and severity levels.

## Configuration

Key configuration options in the `src/downloader.py` (formerly `CSVDownloader` class):

- `data_dir`: Directory to store downloaded files (default: "data")
- `days_back`: Number of days to download (default: 3)
- `BASE_URL`: Energy Transfer API endpoint
- `timeout`: HTTP request timeout (default: 30 seconds)

## Error Handling

The downloader handles various error scenarios:
- **Network Issues**: Retries and detailed error logging
- **Invalid Responses**: Validates CSV format before saving
- **Missing Data**: Logs when no data is available for specific dates/cycles
- **File System Issues**: Creates directories and handles permissions

## Development Notes

This is Phase 1 of the project focusing solely on CSV downloading. Future phases will include:
- Database schema design (PostgreSQL)
- Data validation and parsing
- Database insertion
- More sophisticated scheduling
- Data quality monitoring

## Requirements

- Python 3.7+
- `requests` library for HTTP downloads
- `psycopg2-binary` for PostgreSQL connection
- `pandas` for data handling
- `SQLAlchemy` for database interaction
- Internet connection to access Energy Transfer API

## Troubleshooting

### Common Issues

1. **No files downloaded**:
   - Check internet connection
   - Verify the Energy Transfer website is accessible
   - Check if dates/cycles have available data

2. **Permission errors**:
   - Ensure write permissions in the project directory
   - Check if the `data/` directory can be created

3. **Import errors**:
   - Ensure all dependencies are installed: `pip install -r requirements.txt`
   - Verify Python version compatibility

### Debug Mode

To enable more verbose logging, modify the logging level in the Python files (e.g., `src/downloader.py`, `src/main.py`):
```python
logging.basicConfig(level=logging.DEBUG)
```

## License

This project is developed as part of a candidate assessment for TEC Energy.

## Next Steps

Future enhancements will include:
- PostgreSQL database integration
- Data validation and quality checks
- RESTful API for data access
- Containerization with Docker
- Production deployment considerations
