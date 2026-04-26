import os
from pyspark.sql import SparkSession

def get_spark():
    # FIX: These flags are REQUIRED for Java 17/21/25 compatibility
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--conf "spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED" '
        '--conf "spark.executor.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED" '
        'pyspark-shell'
    )
    
    return SparkSession.builder \
        .appName("MarketingAnalytics") \
        .master("local[*]") \
        .getOrCreate()

def extract_all(spark):
    # FIX: Updated paths to match generated data
    customers = spark.read.csv("data/raw/crm_leads.csv", header=True, inferSchema=True)
    events    = spark.read.csv("data/raw/website_events.csv", header=True, inferSchema=True)
    campaigns = spark.read.csv("data/raw/marketing_campaigns.csv", header=True, inferSchema=True)
    
    print(f"Leads:     {customers.count()} rows")
    print(f"Events:    {events.count()} rows")
    print(f"Campaigns: {campaigns.count()} rows")
    
    return customers, events, campaigns

if __name__ == "__main__":
    spark = get_spark()
    extract_all(spark)
    spark.stop()
