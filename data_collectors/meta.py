from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import pandas as pd
import os
import logging

class MetaCollector:
    def __init__(self):
        # Add logger
        self.logger = logging.getLogger(__name__)
        
        # Initialize the Facebook API
        self.logger.info("Initializing Meta API connection")  # Added log
        app_id = os.getenv('META_APP_ID')
        app_secret = os.getenv('META_APP_SECRET')
        access_token = os.getenv('META_ACCESS_TOKEN')
        FacebookAdsApi.init(app_id, app_secret, access_token)
        
        self.ad_account_id = os.getenv('META_AD_ACCOUNT_ID')
    
    def collect_data(self):
        try:
            self.logger.info(f"Starting Meta data collection for account: {self.ad_account_id}")  # Added log
            account = AdAccount(self.ad_account_id)
            
            self.logger.info("Requesting insights from Meta API")  # Added log
            insights = account.get_insights(
                params={
                    'date_preset': 'last_30d',
                    'fields': [
                        'campaign_name',
                        'spend',
                        'impressions',
                        'clicks',
                        'reach'
                    ]
                }
            )
            
            self.logger.info(f"Received {len(insights)} records from Meta API")  # Added log
            return pd.DataFrame(insights)
            
        except Exception as e:
            self.logger.error(f"Error in Meta data collection: {str(e)}")  # Added log
            raise