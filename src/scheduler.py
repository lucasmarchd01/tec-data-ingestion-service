#!/usr/bin/env python3
"""
TEC Energy Data Ingestion Service - Scheduler

Simple scheduler to run the CSV downloader at regular intervals.
"""

import time
import logging
from datetime import datetime
from downloader import CSVDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Scheduler:
    """Simple scheduler for running CSV downloads at regular intervals."""

    def __init__(self, interval_hours: int = 6):
        """
        Initialize the scheduler.

        Args:
            interval_hours: Hours between download runs (default: 6)
        """
        self.interval_hours = interval_hours
        self.interval_seconds = interval_hours * 3600
        self.downloader = CSVDownloader()

    def run_download(self):
        """Execute a single download run."""
        logger.info(f"Starting scheduled download at {datetime.now()}")
        try:
            downloaded_files = self.downloader.download_last_three_days()
            logger.info(
                f"Scheduled download completed. Downloaded {len(downloaded_files)} files"
            )
        except Exception as e:
            logger.error(f"Error during scheduled download: {e}")

    def run_once(self):
        """Run the downloader once and exit."""
        logger.info("Running CSV downloader once")
        self.run_download()
        logger.info("Single run completed")

    def run_continuous(self):
        """Run the scheduler continuously with the specified interval."""
        logger.info(
            f"Starting continuous scheduler (interval: {self.interval_hours} hours)"
        )

        # Run immediately on start
        self.run_download()

        # Then run at intervals
        while True:
            logger.info(f"Waiting {self.interval_hours} hours until next download...")
            time.sleep(self.interval_seconds)
            self.run_download()


def main():
    """Main function with command-line interface."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        # Run continuously
        interval = 6  # Default 6 hours
        if len(sys.argv) > 2:
            try:
                interval = int(sys.argv[2])
            except ValueError:
                logger.error("Invalid interval. Using default 6 hours.")

        scheduler = Scheduler(interval_hours=interval)
        try:
            scheduler.run_continuous()
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
    else:
        # Run once
        scheduler = Scheduler()
        scheduler.run_once()


if __name__ == "__main__":
    main()
