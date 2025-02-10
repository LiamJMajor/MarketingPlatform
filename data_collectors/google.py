from google.ads.googleads.client import GoogleAdsClient
import pandas as pd
import os

class GoogleAdsCollector:
    def __init__(self):
        # Initialize the Google Ads API client
        self.client = GoogleAdsClient.load_from_env()
        self.customer_id = os.getenv('GOOGLE_CUSTOMER_ID')
    
    def collect_data(self):
        ga_service = self.client.get_service("GoogleAdsService")
        
        query = """
            SELECT
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros
            FROM campaign
            WHERE segments.date DURING LAST_30_DAYS
        """
        
        response = ga_service.search_stream(
            customer_id=self.customer_id,
            query=query
        )
        
        rows = []
        for batch in response:
            for row in batch.results:
                rows.append({
                    'campaign_name': row.campaign.name,
                    'impressions': row.metrics.impressions,
                    'clicks': row.metrics.clicks,
                    'cost': row.metrics.cost_micros / 1000000
                })
        
        return pd.DataFrame(rows) 