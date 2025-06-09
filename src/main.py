#!/usr/bin/env python3
"""
TEC Energy Data Ingestion Service - Main Entry Point

This module provides a unified entry point for the complete data ingestion workflow:
1. Download CSV files from Energy Transfer TW pipeline system
2. Validate downloaded data
3. Upload validated data to PostgreSQL database

This orchestrates the downloader, validator, and uploader modules into a seamless pipeline.
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Optional
import time

# Import our modules
from downloader import CSVDownloader
from uploader import (
    get_db_engine,
    create_table_if_not_exists,
    insert_data_from_csv_pandas,
)
from validator import validate_dataframe

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """Main pipeline class that orchestrates the complete data ingestion workflow."""

    def __init__(
        self,
        data_dir: str = "data",
        skip_download: bool = False,
        skip_upload: bool = False,
    ):
        """
        Initialize the data ingestion pipeline.

        Args:
            data_dir: Directory for CSV files (default: "data")
            skip_download: Skip download phase and work with existing files
            skip_upload: Skip upload phase (download and validate only)
        """
        self.data_dir = data_dir
        self.skip_download = skip_download
        self.skip_upload = skip_upload
        self.downloader = CSVDownloader(data_dir=data_dir)
        self.downloaded_files: List[str] = []
        self.processed_files: List[str] = []
        self.failed_files: List[str] = []

    def ensure_data_directory(self):
        """Create data directory if it doesn't exist."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logger.info(f"Created data directory: {self.data_dir}")

    def get_existing_csv_files(self) -> List[str]:
        """Get list of existing CSV files in the data directory."""
        if not os.path.exists(self.data_dir):
            return []

        csv_files = []
        for filename in os.listdir(self.data_dir):
            if filename.endswith(".csv"):
                filepath = os.path.join(self.data_dir, filename)
                csv_files.append(filepath)

        logger.info(f"Found {len(csv_files)} existing CSV files")
        return csv_files

    def download_phase(self) -> bool:
        """
        Execute the download phase.

        Returns:
            True if download was successful or skipped, False if failed
        """
        if self.skip_download:
            logger.info("Skipping download phase - using existing files")
            self.downloaded_files = self.get_existing_csv_files()
            if not self.downloaded_files:
                logger.warning("No existing CSV files found and download is skipped")
                return False
            return True

        logger.info("=== DOWNLOAD PHASE ===")
        logger.info("Starting CSV download from Energy Transfer TW pipeline system")

        try:
            self.downloaded_files = self.downloader.download_last_three_days()

            if self.downloaded_files:
                logger.info(
                    f"Download phase completed successfully. Downloaded {len(self.downloaded_files)} files:"
                )
                for file in self.downloaded_files:
                    logger.info(f"  - {file}")
                return True
            else:
                logger.warning("Download phase completed but no files were downloaded")
                # Check if there are existing files we can work with
                existing_files = self.get_existing_csv_files()
                if existing_files:
                    logger.info(
                        f"Found {len(existing_files)} existing files to process"
                    )
                    self.downloaded_files = existing_files
                    return True
                return False

        except Exception as e:
            logger.error(f"Download phase failed: {e}")
            return False

    def validation_phase(self) -> bool:
        """
        Execute the validation phase.

        Returns:
            True if validation was successful, False if all files failed
        """
        if not self.downloaded_files:
            logger.error("No files available for validation")
            return False

        logger.info("=== VALIDATION PHASE ===")
        logger.info(f"Starting validation of {len(self.downloaded_files)} CSV files")

        valid_files = []

        for csv_file in self.downloaded_files:
            try:
                logger.info(f"Validating {csv_file}...")

                # Basic file existence check
                if not os.path.exists(csv_file):
                    logger.error(f"File not found: {csv_file}")
                    self.failed_files.append(csv_file)
                    continue

                # The actual validation is done within the uploader during processing
                # But we can do a basic check here to catch obvious issues early
                import pandas as pd

                try:
                    df = pd.read_csv(csv_file)
                    if df.empty:
                        logger.warning(f"File {csv_file} is empty, skipping")
                        self.failed_files.append(csv_file)
                        continue

                    # Quick column check
                    expected_cols = [
                        "Loc",
                        "Loc Zn",
                        "Loc Name",
                        "DC",
                        "OPC",
                        "TSQ",
                        "OAC",
                    ]
                    missing_cols = set(expected_cols) - set(df.columns)
                    if missing_cols:
                        logger.error(
                            f"File {csv_file} missing critical columns: {missing_cols}"
                        )
                        self.failed_files.append(csv_file)
                        continue

                    valid_files.append(csv_file)
                    logger.info(f"File {csv_file} passed basic validation")

                except Exception as e:
                    logger.error(f"Error reading/validating {csv_file}: {e}")
                    self.failed_files.append(csv_file)
                    continue

            except Exception as e:
                logger.error(f"Validation error for {csv_file}: {e}")
                self.failed_files.append(csv_file)

        if valid_files:
            logger.info(
                f"Validation phase completed. {len(valid_files)} files passed basic validation"
            )
            self.downloaded_files = (
                valid_files  # Update list to only include valid files
            )
            return True
        else:
            logger.error("Validation phase failed - no valid files found")
            return False

    def upload_phase(self) -> bool:
        """
        Execute the upload phase.

        Returns:
            True if upload was successful, False if failed
        """
        if self.skip_upload:
            logger.info("Skipping upload phase")
            return True

        if not self.downloaded_files:
            logger.error("No validated files available for upload")
            return False

        logger.info("=== UPLOAD PHASE ===")
        logger.info(
            f"Starting database upload of {len(self.downloaded_files)} validated files"
        )

        try:
            # Initialize database connection
            logger.info("Connecting to PostgreSQL database...")
            engine = get_db_engine()

            # Create table if it doesn't exist
            logger.info("Ensuring database table exists...")
            create_table_if_not_exists(engine)

            # Process each validated file
            successful_uploads = 0
            for csv_file in self.downloaded_files:
                try:
                    logger.info(f"Uploading {csv_file} to database...")
                    insert_data_from_csv_pandas(engine, csv_file)
                    self.processed_files.append(csv_file)
                    successful_uploads += 1
                    logger.info(f"Successfully uploaded {csv_file}")

                except Exception as e:
                    logger.error(f"Failed to upload {csv_file}: {e}")
                    self.failed_files.append(csv_file)

            # Clean up database connection
            engine.dispose()

            if successful_uploads > 0:
                logger.info(
                    f"Upload phase completed. Successfully uploaded {successful_uploads} files"
                )
                return True
            else:
                logger.error(
                    "Upload phase failed - no files were successfully uploaded"
                )
                return False

        except Exception as e:
            logger.error(f"Upload phase failed: {e}")
            return False

    def run_pipeline(self) -> bool:
        """
        Execute the complete data ingestion pipeline.

        Returns:
            True if pipeline completed successfully, False if any critical phase failed
        """
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("TEC ENERGY DATA INGESTION PIPELINE STARTED")
        logger.info(f"Start time: {start_time}")
        logger.info("=" * 60)

        success = True

        # Phase 1: Download
        if not self.download_phase():
            logger.error("Pipeline failed in download phase")
            success = False

        # Phase 2: Validation (only if download succeeded or we have existing files)
        if success and not self.validation_phase():
            logger.error("Pipeline failed in validation phase")
            success = False

        # Phase 3: Upload (only if validation succeeded and not skipped)
        if success and not self.upload_phase():
            logger.error("Pipeline failed in upload phase")
            success = False

        # Summary
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info(f"End time: {end_time}")
        logger.info(f"Total duration: {duration}")
        logger.info(f"Files processed successfully: {len(self.processed_files)}")
        logger.info(f"Files failed: {len(self.failed_files)}")

        if self.processed_files:
            logger.info("Successfully processed files:")
            for file in self.processed_files:
                logger.info(f"  ✓ {file}")

        if self.failed_files:
            logger.warning("Failed files:")
            for file in self.failed_files:
                logger.warning(f"  ✗ {file}")

        status = "SUCCESS" if success else "FAILED"
        logger.info(f"Pipeline status: {status}")
        logger.info("=" * 60)

        return success

    def run_continuous(self, interval_hours: int = 6):
        """
        Run the pipeline continuously at specified intervals.

        Args:
            interval_hours: Hours between pipeline runs (default: 6)
        """
        logger.info(
            f"Starting continuous pipeline execution (interval: {interval_hours} hours)"
        )

        run_count = 0
        while True:
            run_count += 1
            logger.info(f"Starting pipeline run #{run_count}")

            try:
                success = self.run_pipeline()
                if success:
                    logger.info(f"Pipeline run #{run_count} completed successfully")
                else:
                    logger.warning(f"Pipeline run #{run_count} completed with errors")
            except Exception as e:
                logger.error(f"Pipeline run #{run_count} failed with exception: {e}")

            # Wait for next run
            logger.info(f"Waiting {interval_hours} hours until next pipeline run...")
            time.sleep(interval_hours * 3600)


def check_database_connection() -> bool:
    """
    Check if database connection is working.

    Returns:
        True if connection is successful, False otherwise
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        engine.dispose()
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        logger.error(
            "Please check your database configuration and ensure PostgreSQL is running"
        )
        return False


def print_environment_info():
    """Print information about the current environment and configuration."""
    logger.info("Environment Configuration:")
    logger.info(f"  DB_HOST: {os.environ.get('DB_HOST', 'localhost (default)')}")
    logger.info(f"  DB_NAME: {os.environ.get('DB_NAME', 'tec_data (default)')}")
    logger.info(f"  DB_USER: {os.environ.get('DB_USER', 'postgres (default)')}")
    logger.info(f"  DB_PORT: {os.environ.get('DB_PORT', '5432 (default)')}")
    logger.info(f"  Current working directory: {os.getcwd()}")


def main():
    """Main function with command-line interface."""
    parser = argparse.ArgumentParser(
        description="TEC Energy Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          # Run complete pipeline once
  python main.py --continuous             # Run continuously every 6 hours
  python main.py --continuous --interval 2  # Run continuously every 2 hours
  python main.py --skip-download          # Process existing files only
  python main.py --skip-upload            # Download and validate only
  python main.py --test-db                # Test database connection only
  python main.py --data-dir custom_data   # Use custom data directory
        """,
    )

    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run the pipeline continuously at specified intervals",
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=6,
        help="Hours between continuous runs (default: 6)",
    )

    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download phase and process existing files",
    )

    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip upload phase (download and validate only)",
    )

    parser.add_argument(
        "--data-dir", default="data", help="Directory for CSV files (default: data)"
    )

    parser.add_argument(
        "--test-db", action="store_true", help="Test database connection and exit"
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    # Print environment info
    print_environment_info()

    # Test database connection if requested
    if args.test_db:
        logger.info("Testing database connection...")
        if check_database_connection():
            logger.info("Database connection test passed")
            sys.exit(0)
        else:
            logger.error("Database connection test failed")
            sys.exit(1)

    # Test database connection unless upload is skipped
    if not args.skip_upload:
        if not check_database_connection():
            logger.error(
                "Database connection failed. Use --skip-upload to run without database."
            )
            sys.exit(1)

    # Initialize pipeline
    pipeline = DataIngestionPipeline(
        data_dir=args.data_dir,
        skip_download=args.skip_download,
        skip_upload=args.skip_upload,
    )

    try:
        if args.continuous:
            # Run continuously
            if args.interval < 1:
                logger.error("Interval must be at least 1 hour")
                sys.exit(1)

            logger.info(f"Starting continuous mode with {args.interval} hour intervals")
            pipeline.run_continuous(interval_hours=args.interval)
        else:
            # Run once
            success = pipeline.run_pipeline()
            sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
