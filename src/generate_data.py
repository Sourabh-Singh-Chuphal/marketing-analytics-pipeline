import pandas as pd
import numpy as np
from faker import Faker
import random
import os
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

def generate_data(num_leads=500, num_campaigns=10, num_events=2000):
    """
    Generates mock marketing data: CRM leads, marketing campaigns, and website events.
    """
    
    # Create directories if they don't exist
    os.makedirs('data/raw', exist_ok=True)

    print("Generating CRM Leads...")
    # 1. CRM Leads
    sources = ['Google Ads', 'Facebook Ads', 'LinkedIn Ads', 'Organic Search', 'Direct', 'Email Marketing', 'Referral']
    statuses = ['New', 'Contacted', 'Qualified', 'Nurturing', 'Closed-Won', 'Closed-Lost']
    
    leads = []
    for i in range(num_leads):
        lead_id = f"L{1000 + i}"
        created_at = fake.date_time_between(start_date='-90d', end_date='now')
        leads.append({
            'lead_id': lead_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'source': random.choice(sources),
            'status': random.choice(statuses),
            'created_at': created_at
        })
    
    df_leads = pd.DataFrame(leads)
    df_leads.to_csv('data/raw/crm_leads.csv', index=False)
    print(f"Saved {num_leads} leads to data/raw/crm_leads.csv")

    print("Generating Marketing Campaigns...")
    # 2. Marketing Campaigns
    campaign_channels = {
        'Google Ads': ['Search_Brand', 'Search_Generic', 'Display_Retargeting'],
        'Facebook Ads': ['FB_Lookalike', 'FB_Interests', 'IG_Stories'],
        'LinkedIn Ads': ['LI_DecisionMakers', 'LI_SkillsTargeting'],
        'Email Marketing': ['Newsletter_Spring', 'Promo_FlashSale']
    }
    
    campaigns = []
    start_date = datetime.now() - timedelta(days=90)
    
    for i in range(num_campaigns):
        channel = random.choice(list(campaign_channels.keys()))
        name = random.choice(campaign_channels[channel])
        campaign_id = f"CMP{100 + i}"
        
        # Generate daily records for campaigns
        for d in range(90):
            current_date = start_date + timedelta(days=d)
            budget = round(random.uniform(50, 500), 2)
            spend = round(budget * random.uniform(0.8, 1.1), 2)
            impressions = random.randint(1000, 10000)
            clicks = int(impressions * random.uniform(0.01, 0.05))
            
            campaigns.append({
                'campaign_id': campaign_id,
                'campaign_name': name,
                'channel': channel,
                'date': current_date.strftime('%Y-%m-%d'),
                'budget': budget,
                'spend': spend,
                'impressions': impressions,
                'clicks': clicks
            })
            
    df_campaigns = pd.DataFrame(campaigns)
    df_campaigns.to_csv('data/raw/marketing_campaigns.csv', index=False)
    print(f"Saved {len(df_campaigns)} campaign daily records to data/raw/marketing_campaigns.csv")

    print("Generating Website Events...")
    # 3. Website Events
    event_types = ['page_view', 'session_start', 'form_submit', 'button_click', 'download_whitepaper']
    pages = ['/home', '/pricing', '/features', '/blog/post-1', '/blog/post-2', '/contact', '/demo-request']
    
    events = []
    lead_ids = df_leads['lead_id'].tolist()
    
    for i in range(num_events):
        # Some events are from known leads, some are anonymous (NaN)
        lead_id = random.choice(lead_ids) if random.random() > 0.4 else np.nan
        
        event_time = fake.date_time_between(start_date='-90d', end_date='now')
        
        events.append({
            'event_id': f"EV{50000 + i}",
            'lead_id': lead_id,
            'event_type': random.choice(event_types),
            'page_path': random.choice(pages),
            'timestamp': event_time
        })
        
    df_events = pd.DataFrame(events)
    df_events.to_csv('data/raw/website_events.csv', index=False)
    print(f"Saved {num_events} events to data/raw/website_events.csv")

if __name__ == "__main__":
    generate_data()
    print("\nData generation complete!")
