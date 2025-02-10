from google.ads.googleads.client import GoogleAdsClient
import pandas as pd
from datetime import datetime
from .base import BaseCollector

class GoogleAdsCollector(BaseCollector):
    def __init__(self, engine):
        super().__init__(engine)
        
        self.client = GoogleAdsClient.load_from_env()
        self.customer_id = os.getenv('GOOGLE_CUSTOMER_ID')
        self.table_name = 'google_ads_insights'
    
    def collect_data(self):
        try:
            # Get last update date
            last_update = self.get_last_update_date(self.table_name)
            start_date, end_date = self.get_date_range(last_update)
            
            self.logger.info(f"Collecting Google Ads data from {start_date} to {end_date}")
            
            ga_service = self.client.get_service("GoogleAdsService")
            
            query = f"""
                SELECT
                    campaign.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    segments.date
                FROM campaign
                WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
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
                        'cost': row.metrics.cost_micros / 1000000,
                        'date': datetime.strptime(row.segments.date, '%Y-%m-%d').date()
                    })
            
            if not rows:
                self.logger.info("No new data to collect")
                return
                
            df = pd.DataFrame(rows)
            
            # Update database
            df.to_sql(
                self.table_name, 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            
            self.logger.info(f"Successfully updated {len(df)} rows of Google Ads data")
            return df
            
        except Exception as e:
            self.logger.error(f"Error collecting Google Ads data: {str(e)}")
            raise