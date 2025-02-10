print("Hello World again")

import os
from datetime import datetime
from sqlalchemy import create_engine
from data_collectors.meta import MetaCollector
#from data_collectors.google import GoogleAdsCollector
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_PATH = os.getenv('DATABASE_PATH', 'marketing_data.db')
engine = create_engine(f'sqlite:///{DATABASE_PATH}')

def main():
    try:
        # Initialize collectors
        meta_collector = MetaCollector()
        #google_collector = GoogleAdsCollector()
        
        # Collect data
        meta_data = meta_collector.collect_data()
        #google_data = google_collector.collect_data()
        
        # Save to database
        current_date = datetime.now().strftime('%Y_%m_%d')
        meta_data.to_sql(f'meta_data_{current_date}', engine, if_exists='replace')
        #google_data.to_sql(f'google_data_{current_date}', engine, if_exists='replace')
        
        logger.info("Data collection completed successfully")
        
    except Exception as e:
        logger.error(f"Error during data collection: {str(e)}")
        raise

if __name__ == "__main__":
    main() 