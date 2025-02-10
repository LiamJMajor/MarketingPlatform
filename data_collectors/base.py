from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import logging

class BaseCollector:
    def __init__(self, engine):
        self.engine = engine
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def get_last_update_date(self, table_name):
        """Get the most recent date in the database for this collector"""
        try:
            query = f"SELECT MAX(date) as last_date FROM {table_name}"
            with self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            self.logger.warning(f"No existing data found: {str(e)}")
            return None
            
    def get_date_range(self, last_update):
        """Calculate the date range needed for update"""
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = last_update + timedelta(days=1) if last_update else end_date - timedelta(days=90)
        return start_date, end_date 