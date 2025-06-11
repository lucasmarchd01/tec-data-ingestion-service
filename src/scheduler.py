#!/usr/bin/env python3
"""
TEC Energy Data Ingestion Service - Scheduler

Generic scheduler to run any pipeline or task at regular intervals.
"""

import time
import logging
from datetime import datetime
from typing import Callable, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Scheduler:
    """Generic scheduler for running tasks at regular intervals."""

    def __init__(self, interval_hours: int = 6, task_name: str = "Task"):
        """
        Initialize the scheduler.

        Args:
            interval_hours: Hours between task runs (default: 6)
            task_name: Name of the task for logging purposes
        """
        self.interval_hours = interval_hours
        self.interval_seconds = interval_hours * 3600
        self.task_name = task_name
        self.task_function: Optional[Callable] = None
        self.run_count = 0

    def set_task(self, task_function: Callable) -> None:
        """
        Set the task function to be executed by the scheduler.

        Args:
            task_function: Callable that represents the task to be scheduled
        """
        self.task_function = task_function

    def run_task(self) -> bool:
        """
        Execute a single task run.

        Returns:
            True if task completed successfully, False otherwise
        """
        if not self.task_function:
            logger.error(
                "No task function set. Use set_task() to configure the scheduler."
            )
            return False

        self.run_count += 1
        logger.info(
            f"Starting scheduled {self.task_name} run #{self.run_count} at {datetime.now()}"
        )

        try:
            result = self.task_function()
            if result:
                logger.info(
                    f"Scheduled {self.task_name} run #{self.run_count} completed successfully"
                )
            else:
                logger.warning(
                    f"Scheduled {self.task_name} run #{self.run_count} completed with errors"
                )
            return result
        except Exception as e:
            logger.error(
                f"Error during scheduled {self.task_name} run #{self.run_count}: {e}"
            )
            return False

    def run_once(self) -> bool:
        """
        Run the task once and exit.

        Returns:
            True if task completed successfully, False otherwise
        """
        logger.info(f"Running {self.task_name} once")
        result = self.run_task()
        logger.info("Single run completed")
        return result

    def run_continuous(self) -> None:
        """Run the scheduler continuously with the specified interval."""
        if not self.task_function:
            logger.error(
                "No task function set. Use set_task() to configure the scheduler."
            )
            return

        logger.info(
            f"Starting continuous scheduler for {self.task_name} (interval: {self.interval_hours} hours)"
        )

        # Run immediately on start
        self.run_task()

        # Then run at intervals
        try:
            while True:
                logger.info(
                    f"Waiting {self.interval_hours} hours until next {self.task_name} run..."
                )
                time.sleep(self.interval_seconds)
                self.run_task()
        except KeyboardInterrupt:
            logger.info(f"Scheduler stopped by user after {self.run_count} runs")
            raise


def main():
    """Main function with command-line interface for backward compatibility."""
    import sys
    from downloader import CSVDownloader

    def download_task():
        """Legacy download task for backward compatibility."""
        downloader = CSVDownloader()
        try:
            downloaded_files = downloader.download_last_three_days()
            logger.info(f"Downloaded {len(downloaded_files)} files")
            return len(downloaded_files) > 0
        except Exception as e:
            logger.error(f"Download task failed: {e}")
            return False

    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        # Run continuously
        interval = 6  # Default 6 hours
        if len(sys.argv) > 2:
            try:
                interval = int(sys.argv[2])
            except ValueError:
                logger.error("Invalid interval. Using default 6 hours.")

        scheduler = Scheduler(interval_hours=interval, task_name="CSV Download")
        scheduler.set_task(download_task)
        try:
            scheduler.run_continuous()
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
    else:
        # Run once
        scheduler = Scheduler(task_name="CSV Download")
        scheduler.set_task(download_task)
        scheduler.run_once()


if __name__ == "__main__":
    main()
