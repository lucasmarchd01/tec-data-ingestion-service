#!/usr/bin/env python3
"""
TEC Energy Data Ingestion Service - CSV Downloader

This module handles downloading natural gas shipment data CSVs from Energy Transfer.
The program downloads data from the last 3 days, handling multiple cycles per day.
"""

import requests
import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CSVDownloader:
    """Handles downloading CSV data from Energy Transfer TW pipeline system."""
    
    BASE_URL = "https://twtransfer.energytransfer.com/ipost/capacity/operationally-available"
    
    # Cycle mapping with proper names and numbers
    CYCLES = {
        'timely': 0,
        'evening': 1, 
        'intraday_1': 3,
        'intraday_2': 4,
        'final': 5,
        'intraday_3': 7
    }
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the CSV downloader.
        
        Args:
            data_dir: Directory to store downloaded CSV files
        """
        self.data_dir = data_dir
        self.ensure_data_directory()
    
    def ensure_data_directory(self):
        """Create data directory if it doesn't exist."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logger.info(f"Created data directory: {self.data_dir}")
    
    def get_date_range(self, days_back: int = 3) -> List[datetime]:
        """
        Get list of dates for the last N days.
        
        Args:
            days_back: Number of days to go back (default: 3)
            
        Returns:
            List of datetime objects for the date range
        """
        dates = []
        for i in range(days_back):
            date = datetime.now() - timedelta(days=i)
            dates.append(date)
        return dates
    
    def build_csv_url(self, gas_day: datetime, cycle: int) -> str:
        """
        Build the CSV download URL for a specific gas day and cycle.
        
        Args:
            gas_day: The gas day date
            cycle: The cycle number
            
        Returns:
            Complete URL for CSV download
        """
        # Format date as MM%2FDD%2FYYYY (URL encoded MM/DD/YYYY)
        formatted_date = gas_day.strftime("%m%%2F%d%%2F%Y")
        
        params = {
            "f": "csv",
            "extension": "csv", 
            "asset": "TW",
            "gasDay": formatted_date,
            "cycle": str(cycle),
            "searchType": "NOM",
            "searchString": "",
            "locType": "ALL",
            "locZone": "ALL"
        }
        
        # Build URL with parameters
        url = f"{self.BASE_URL}?"
        url += "&".join([f"{k}={v}" for k, v in params.items()])
        return url
    
    def download_csv(self, url: str) -> Optional[str]:
        """
        Download CSV data from the given URL.
        
        Args:
            url: URL to download CSV from
            
        Returns:
            CSV content as string, or None if download failed
        """
        try:
            logger.info(f"Downloading CSV from: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Check if we got actual CSV data (not an error page)
            if response.text.startswith('"Loc"'):
                logger.info("Successfully downloaded CSV data")
                return response.text
            else:
                logger.warning("Downloaded content doesn't appear to be CSV data")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download CSV: {e}")
            return None
    
    def save_csv(self, content: str, gas_day: datetime, cycle: int) -> str:
        """
        Save CSV content to file.
        
        Args:
            content: CSV content to save
            gas_day: The gas day date
            cycle: The cycle number
            
        Returns:
            Path to saved file
        """
        filename = f"tec_data_{gas_day.strftime('%Y%m%d')}_cycle_{cycle}.csv"
        filepath = os.path.join(self.data_dir, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Saved CSV data to: {filepath}")
        return filepath
    
    def download_for_date_and_cycle(self, gas_day: datetime, cycle: int) -> Optional[str]:
        """
        Download CSV for a specific date and cycle.
        
        Args:
            gas_day: The gas day date
            cycle: The cycle number
            
        Returns:
            Path to saved file, or None if download failed
        """
        url = self.build_csv_url(gas_day, cycle)
        content = self.download_csv(url)
        
        if content:
            return self.save_csv(content, gas_day, cycle)
        return None
    
    def download_last_three_days(self) -> List[str]:
        """
        Download CSV files for the last 3 days, trying multiple cycles per day.
        
        Returns:
            List of paths to successfully downloaded files
        """
        downloaded_files = []
        dates = self.get_date_range(3)
        
        logger.info(f"Starting download for last 3 days: {[d.strftime('%Y-%m-%d') for d in dates]}")
        
        for date in dates:
            logger.info(f"Processing date: {date.strftime('%Y-%m-%d')}")
            
            # Try all available cycles based on the cycle mapping
            cycles_to_try = list(self.CYCLES.values())  # [0, 1, 3, 4, 5, 7]
            
            for cycle in cycles_to_try:
                try:
                    filepath = self.download_for_date_and_cycle(date, cycle)
                    if filepath:
                        downloaded_files.append(filepath)
                        # Get cycle name for logging
                        cycle_name = next((name for name, num in self.CYCLES.items() if num == cycle), f"cycle_{cycle}")
                        logger.info(f"Downloaded {cycle_name} (cycle {cycle}) for {date.strftime('%Y-%m-%d')}")
                    else:
                        cycle_name = next((name for name, num in self.CYCLES.items() if num == cycle), f"cycle_{cycle}")
                        logger.info(f"No data available for {date.strftime('%Y-%m-%d')} {cycle_name} (cycle {cycle})")
                except Exception as e:
                    cycle_name = next((name for name, num in self.CYCLES.items() if num == cycle), f"cycle_{cycle}")
                    logger.error(f"Error downloading {date.strftime('%Y-%m-%d')} {cycle_name} (cycle {cycle}): {e}")
        
        logger.info(f"Download complete. Successfully downloaded {len(downloaded_files)} files")
        return downloaded_files


def main():
    """Main function to run the CSV downloader."""
    logger.info("Starting TEC Energy CSV Downloader")
    
    downloader = CSVDownloader()
    downloaded_files = downloader.download_last_three_days()
    
    if downloaded_files:
        logger.info("Downloaded files:")
        for file in downloaded_files:
            logger.info(f"  - {file}")
    else:
        logger.warning("No files were downloaded")
    
    logger.info("CSV Downloader finished")


if __name__ == "__main__":
    main()
