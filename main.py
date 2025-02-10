import os
from datetime import datetime
from sqlalchemy import create_engine
from data_collectors.meta import MetaCollector
from data_collectors.google import GoogleAdsCollector
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_PATH = os.getenv('DATABASE_PATH', 'marketing_data.db')
engine = create_engine(f'sqlite:///{DATABASE_PATH}')

def main():
    try:
        # Initialize collectors with engine
        meta_collector = MetaCollector(engine)
        google_collector = GoogleAdsCollector(engine)
        
        # Collect data
        meta_collector.collect_data()
        google_collector.collect_data()
        
        logger.info("Data collection completed successfully")
        
    except Exception as e:
        logger.error(f"Error during data collection: {str(e)}")
        raise

if __name__ == "__main__":
    main()