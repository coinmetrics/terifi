import os
import asyncio
from datetime import datetime, timedelta
from coinmetrics.api_client import CoinMetricsClient
import market_utils

# Initialize client
def init_client():
    api_key = os.environ.get("CM_API_KEY")
    if not api_key:
        raise ValueError("CM_API_KEY environment variable not set")
    return CoinMetricsClient(api_key)

async def fetch_greeks_for_expiry(client, markets, expiry_date, days_before_expiry=22, granularity="1d"):
    """
    Fetch Greeks data for markets with the same expiry date.
    
    Args:
        client: CoinMetrics client
        markets: List of market identifiers with the same expiry date
        expiry_date: Expiry date (datetime.date)
        days_before_expiry: Number of days before expiry to collect data
        granularity: Data granularity (e.g., "1d", "1h")
    """
    # Calculate data period based on expiry date
    start_time, end_time = market_utils.calculate_data_period_for_expiry(
        expiry_date, days_before_expiry=days_before_expiry
    )
    
    print(f"Fetching Greeks for {len(markets)} markets expiring on {expiry_date}, "
          f"from {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")

    try:
        # Fetch data with parallelization and export directly to CSV files
        client.get_market_greeks(
            markets=markets,
            start_time=start_time,
            end_time=end_time,
            page_size=10000,
            granularity=granularity,
        ).parallel().export_to_csv_files()
        
        print(f"Successfully saved Greeks data for expiry date {expiry_date}")
        return True
    
    except Exception as e:
        print(f"Error fetching Greeks data for expiry date {expiry_date}: {e}")
        return False

# Save Greeks data for multiple expiration dates
async def save_greeks_data(start_date, end_date, days_before_expiry=22, granularity="1d"):
    """
    Save Greeks data for all markets expiring between start_date and end_date.
    For each expiry date, collect data from (expiry_date - days_before_expiry) to expiry_date.
    
    Args:
        start_date: Start date for expiry range (datetime)
        end_date: End date for expiry range (datetime)
        days_before_expiry: Number of days before expiry to collect data
        granularity: Data granularity (e.g., "1d", "1h")
    """
    print(f"Looking for options expiring between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
    
    client = init_client()
    
    # Get catalog data with expiration dates
    catalog = market_utils.get_markets_with_expiry(
        client.catalog_market_greeks_v2,
        exchange='deribit',
        base='btc'
    )
    
    # Group markets by expiry date
    grouped_markets = market_utils.fetch_markets_by_expiry_date(
        catalog, 
        start_date, 
        end_date
    )
    
    if not grouped_markets:
        print("No markets found within the specified time window.")
        return None
    
    # Create tasks to fetch data for each expiry date
    tasks = []
    for expiry_date, markets in grouped_markets.items():
        tasks.append(fetch_greeks_for_expiry(
            client,
            markets,
            expiry_date,
            days_before_expiry=days_before_expiry,
            granularity=granularity
        ))
    
    # Run tasks in parallel (with a concurrency limit)
    MAX_CONCURRENT_TASKS = 5  # Adjust based on API rate limits and system capabilities
    
    # Split tasks into batches to avoid overwhelming the API
    results = []
    for i in range(0, len(tasks), MAX_CONCURRENT_TASKS):
        batch = tasks[i:i+MAX_CONCURRENT_TASKS]
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)
        
        # Optional: add a small delay between batches
        if i + MAX_CONCURRENT_TASKS < len(tasks):
            print(f"Completed batch {i//MAX_CONCURRENT_TASKS + 1}, waiting before starting next batch...")
            await asyncio.sleep(2)
    
    successful = results.count(True)
    print(f"Completed fetching Greeks data for {successful} out of {len(grouped_markets)} expiry dates")

# Main function to run the data collection
async def main():
    # Example: Collect data for a specific date range
    end_date = datetime(2025, 1, 1)
    start_date = end_date - timedelta(days=30)
    
    # Days before expiry to collect data
    days_before_expiry = 22  # Based on analysis, using 22 days captures 90% of the trading activity
    
    print(f"Collecting Greeks data for options expiring between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
    print(f"For each expiry date, will collect data from (expiry_date - {days_before_expiry} days) to expiry_date")
    
    # Default granularity
    granularity = "1d"
    
    await save_greeks_data(
        start_date, 
        end_date, 
        days_before_expiry=days_before_expiry,
        granularity=granularity
    )

if __name__ == "__main__":
    asyncio.run(main())