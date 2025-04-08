import os
import pandas as pd
import re
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz
from coinmetrics.api_client import CoinMetricsClient

def init_client():
    api_key = os.environ.get("CM_API_KEY")
    if not api_key:
        raise ValueError("CM_API_KEY environment variable not set")
    return CoinMetricsClient(api_key)

def extract_expiration_date(market_name):
    """Extract expiration date from market name.
    Examples: 
    - deribit-BTC-10APR22-34000-C-option
    - deribit-BTC-24MAY24-70000-P-option
    """
    try:
        # Extract the date portion (like 10APR22)
        match = re.search(r'-([0-9]+[A-Z]{3}[0-9]{2})-', market_name)
        if not match:
            return None
        
        date_str = match.group(1)
        
        # Convert month abbreviation to number
        month_map = {
            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
        }
        
        day = date_str[:2]
        month_abbr = date_str[2:5]
        year = '20' + date_str[5:7]  # Assuming all years are 20xx
        
        if month_abbr in month_map:
            month = month_map[month_abbr]
            return f"{year}-{month}-{day}T08:00:00+00:00"  # Adding time component with UTC timezone
        return None
    except Exception:
        return None

def extract_strike_and_type(market_name):
    """Extract strike price and option type from market name.
    Example: deribit-BTC-10APR22-34000-C-option
    """
    try:
        parts = market_name.split('-')
        if len(parts) >= 6:
            try:
                strike = float(parts[4])
                option_type = parts[5]
                return strike, option_type
            except ValueError:
                # If strike can't be converted to float, return None
                return None, parts[5] if len(parts) > 5 else None
        return None, None
    except Exception:
        return None, None

def analyze_greek_markets():
    client = init_client()
    print("Fetching catalog data for Greeks...")
    catalog = client.catalog_market_greeks_v2(exchange='deribit', base='btc').to_dataframe()
    
    if catalog.empty:
        print("No catalog data found.")
        return
    
    print(f"Found {len(catalog)} markets in the catalog.")
    
    # Display column names
    print("\nCatalog columns:")
    print(catalog.columns.tolist())
    
    # Sample of the catalog data
    print("\nSample of catalog data:")
    print(catalog.head())
    
    # Extract expiration dates from market names (with timezone info)
    catalog['expiration_str'] = catalog['market'].apply(extract_expiration_date)
    catalog = catalog[catalog['expiration_str'].notna()]  # Remove rows where expiration couldn't be extracted
    
    print(f"\nSuccessfully extracted expiration dates for {len(catalog)} markets")
    
    # Convert expiration strings to datetime (already timezone aware)
    catalog['expiration_date'] = pd.to_datetime(catalog['expiration_str'])
    
    # Extract strike and option type
    catalog[['strike', 'option_type']] = pd.DataFrame(catalog['market'].apply(lambda x: extract_strike_and_type(x)).tolist())
    
    # Calculate trading period length in days
    catalog['trading_days'] = (catalog['max_time'] - catalog['min_time']).dt.total_seconds() / (24*60*60)
    
    # Calculate days before expiration that trading begins
    catalog['days_before_expiration'] = (catalog['expiration_date'] - catalog['min_time']).dt.total_seconds() / (24*60*60)
    
    # Calculate days until expiration from now
    now = datetime.now(pytz.UTC)
    catalog['days_to_expiration'] = (catalog['expiration_date'] - pd.Timestamp(now)).dt.total_seconds() / (24*60*60)
    
    # Filter to see options that are still active (not yet expired)
    active_options = catalog[catalog['days_to_expiration'] > 0]
    print(f"\nNumber of active options (not yet expired): {len(active_options)}")
    
    # Analysis of trading periods
    print("\nDistribution of trading period length (days):")
    print(catalog['trading_days'].describe([0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).round(2))
    
    print("\nDistribution of days before expiration that trading begins:")
    print(catalog['days_before_expiration'].describe([0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).round(2))
    
    # Group by year-month of expiration to see patterns over time
    catalog['exp_year_month'] = catalog['expiration_date'].dt.to_period('M')
    trading_period_by_month = catalog.groupby('exp_year_month')['trading_days'].mean().reset_index()
    trading_period_by_month['exp_year_month'] = trading_period_by_month['exp_year_month'].astype(str)
    
    # Get the last 20 months to see recent trends
    recent_months = trading_period_by_month.tail(20)
    
    print("\nAverage trading period length by expiration month (last 20 months):")
    print(recent_months)
    
    # Create output directory for plots
    os.makedirs('analysis', exist_ok=True)
    
    # Plot distribution of trading periods
    plt.figure(figsize=(10, 6))
    catalog['trading_days'].hist(bins=50)
    plt.title('Distribution of Trading Period Length')
    plt.xlabel('Trading Period (days)')
    plt.ylabel('Number of Options')
    plt.grid(True)
    plt.savefig('analysis/trading_period_distribution.png')
    
    # Plot distribution of days before expiration that trading begins
    plt.figure(figsize=(10, 6))
    catalog['days_before_expiration'].hist(bins=50)
    plt.title('Days Before Expiration That Trading Begins')
    plt.xlabel('Days')
    plt.ylabel('Number of Options')
    plt.grid(True)
    plt.savefig('analysis/days_before_expiration_distribution.png')
    
    # Print option type distribution if available
    if 'option_type' in catalog.columns:
        option_type_counts = catalog['option_type'].value_counts()
        print("\nOption Type Distribution:")
        print(option_type_counts)
    
    # Analyze strike prices if available
    valid_strike_df = catalog[catalog['strike'].notna()]
    if not valid_strike_df.empty:
        print("\nStrike Price Statistics:")
        print(valid_strike_df['strike'].describe().round(2))
        
        # Only create strike-related visualizations if we have enough valid data
        if len(valid_strike_df) > 100:  # Arbitrary threshold
            # Group by strike price ranges
            min_strike = valid_strike_df['strike'].min()
            max_strike = valid_strike_df['strike'].max()
            
            # Create bins based on data range
            bins = pd.cut(valid_strike_df['strike'], bins=10)
            valid_strike_df['strike_range'] = bins
            
            # Group by strike range
            trading_by_strike = valid_strike_df.groupby('strike_range')['trading_days'].mean().reset_index()
            
            plt.figure(figsize=(12, 6))
            plt.bar(trading_by_strike['strike_range'].astype(str), trading_by_strike['trading_days'])
            plt.title('Average Trading Period Length by Strike Price Range')
            plt.xlabel('Strike Price Range')
            plt.ylabel('Average Trading Period (days)')
            plt.xticks(rotation=45)
            plt.grid(True)
            plt.tight_layout()
            plt.savefig('analysis/trading_period_by_strike.png')
    
    # Save the analysis to CSV for further exploration
    catalog.to_csv('analysis/greeks_market_analysis.csv', index=False)
    
    # Print recommendations based on analysis
    percentiles = catalog['days_before_expiration'].describe([0.5, 0.75, 0.9, 0.95]).round(2)
    
    print("\n=== RECOMMENDATIONS FOR DATA COLLECTION ===")
    print(f"Based on the analysis of {len(catalog)} options markets:")
    print(f"1. Median time before expiration that trading begins: {percentiles['50%']} days")
    print(f"2. 75th percentile: {percentiles['75%']} days")
    print(f"3. 90th percentile: {percentiles['90%']} days")
    print(f"4. 95th percentile: {percentiles['95%']} days")
    
    recommendation = percentiles['90%']
    print(f"\nRECOMMENDATION: To capture ~90% of trading activity, collect data starting {recommendation:.0f} days before expiration date.")
    
    # Also report on typical trading period length
    trading_percentiles = catalog['trading_days'].describe([0.5, 0.75, 0.9]).round(2)
    print(f"\nTypical trading period length (median): {trading_percentiles['50%']} days")
    print(f"75th percentile trading period: {trading_percentiles['75%']} days")
    print(f"90th percentile trading period: {trading_percentiles['90%']} days")
    
    return catalog

if __name__ == "__main__":
    analyze_greek_markets()