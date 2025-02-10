from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import pandas as pd
from datetime import datetime
from .base import BaseCollector

class MetaCollector(BaseCollector):
    def __init__(self, engine):
        super().__init__(engine)
        
        # Initialize the Facebook API
        app_id = os.getenv('META_APP_ID')
        app_secret = os.getenv('META_APP_SECRET')
        access_token = os.getenv('META_ACCESS_TOKEN')
        FacebookAdsApi.init(app_id, app_secret, access_token)
        
        self.ad_account_id = os.getenv('META_AD_ACCOUNT_ID')
        self.table_name = 'meta_insights'
    
    def collect_data(self):
        try:
            # Get last update date
            last_update = self.get_last_update_date(self.table_name)
            start_date, end_date = self.get_date_range(last_update)
            
            self.logger.info(f"Collecting Meta data from {start_date} to {end_date}")
            
            account = AdAccount(self.ad_account_id)
            insights = account.get_insights(
                params={
                    'time_range': {
                        'since': start_date.strftime('%Y-%m-%d'),
                        'until': end_date.strftime('%Y-%m-%d')
                    },
                    'fields': [
                        'campaign_name',
                        'spend',
                        'impressions',
                        'clicks',
                        'reach',
                        'date_start'
                    ]
                }
            )
            
            if not insights:
                self.logger.info("No new data to collect")
                return
            
            df = pd.DataFrame(insights)
            df['date'] = pd.to_datetime(df['date_start']).dt.date
            
            # Update database
            df.to_sql(
                self.table_name, 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            
            self.logger.info(f"Successfully updated {len(df)} rows of Meta data")
            return df
            
        except Exception as e:
            self.logger.error(f"Error collecting Meta data: {str(e)}")
            raise