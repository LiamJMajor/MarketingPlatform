import os
from datetime import datetime
from sqlalchemy import create_engine
from data_collectors.meta import MetaCollector
#from data_collectors.google import GoogleAdsCollector
import logging

# Log current datetime at startup
logger = logging.getLogger(__name__)
logger.info(f"Script started at: {datetime.now()}")


# Setup logging with more detailed configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Logs to console
        logging.FileHandler('marketing_collector.log')  # Also logs to file
    ]
)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_PATH = os.getenv('DATABASE_PATH', 'marketing_data.db')
engine = create_engine(f'sqlite:///{DATABASE_PATH}')

def main():
    try:
        logger.info("Starting data collection process")
        
        # Initialize collectors
        logger.info("Initializing Meta collector")
        meta_collector = MetaCollector()
        
        logger.info("Initializing Google collector")
        #google_collector = GoogleAdsCollector()
        
        # Collect data
        logger.info("Starting Meta data collection")
        meta_data = meta_collector.collect_data()
        logger.info(f"Meta data collection complete. Shape: {meta_data.shape if meta_data is not None else 'No data'}")
        
        #logger.info("Starting Google Ads data collection")
        #google_data = google_collector.collect_data()
        #logger.info(f"Google data collection complete. Shape: {google_data.shape if google_data is not None else 'No data'}")
        
        logger.info("Data collection completed successfully")
        
    except Exception as e:
        logger.error(f"Error during data collection: {str(e)}")
        raise

if __name__ == "__main__":
    main()