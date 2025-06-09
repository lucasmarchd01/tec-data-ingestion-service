# Quick Start with Docker

This project includes a simple Docker setup to get you up and running quickly.

## Prerequisites
- Docker and Docker Compose installed on your system

## Getting Started

1. **Clone and navigate to the project:**
   ```bash
   git clone <repository-url>
   cd tec-data-ingestion-service
   ```

2. **Start the application:**
   ```bash
   docker compose up --build
   ```

   This will:
   - Start a PostgreSQL database
   - Build and run the TEC data ingestion application
   - Process existing CSV files in the `data/` directory

3. **Test database connection:**
   ```bash
   docker compose run --rm app python src/main.py --test-db
   ```

4. **Run with different options:**
   ```bash
   # Skip download and process existing files
   docker compose run --rm app python src/main.py --skip-download
   
   # Run continuously every 2 hours
   docker compose run --rm app python src/main.py --continuous --interval 2
   ```

## What's Included

- **PostgreSQL 14**: Database for storing pipeline data
- **Python App**: Complete data ingestion pipeline
- **Volume Mounts**: Your local `data/` directory is mounted to persist CSV files

## Database Access

The PostgreSQL database is accessible at:
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `tec_data`
- **Username**: `tec_user`
- **Password**: `tec_password`

## Stopping

Press `Ctrl+C` to stop, then run:
```bash
docker compose down
```

To also remove the database data:
```bash
docker compose down -v
```
