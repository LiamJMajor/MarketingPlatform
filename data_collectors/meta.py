from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import pandas as pd
import os

class MetaCollector:
    def __init__(self):
        # Initialize the Facebook API
        app_id = os.getenv('META_APP_ID')
        app_secret = os.getenv('META_APP_SECRET')
        access_token = os.getenv('META_ACCESS_TOKEN')
        FacebookAdsApi.init(app_id, app_secret, access_token)
        
        self.ad_account_id = os.getenv('META_AD_ACCOUNT_ID')
    
    def collect_data(self):
        account = AdAccount(self.ad_account_id)
        
        # Get insights data
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
        

        # Print first few rows of insights data for debugging
        print("Sample Meta Ads insights data:")
        print(pd.DataFrame(insights).head())

    
        return pd.DataFrame(insights) 