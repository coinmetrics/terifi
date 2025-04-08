import os
import re
import pandas as pd
from datetime import datetime, timedelta
from coinmetrics.api_client import CoinMetricsClient

# Initialize client
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
            # Return date in format YYYY-MM-DD
            return f"{year}-{month}-{day}"
        return None
    except Exception:
        return None

def get_markets_with_expiry(catalog_func, exchange='deribit', base='btc'):
    """
    Get markets from catalog with parsed expiration dates.
    
    Args:
        catalog_func: Function to get catalog (e.g., client.catalog_market_greeks_v2)
        exchange: Exchange name (default: 'deribit')
        base: Base asset (default: 'btc')
        
    Returns:
        DataFrame with market data including expiration_date
    """
    client = init_client()
    
    # Get catalog data
    if base:
        catalog_data = catalog_func(exchange=exchange, base=base)
    else:
        catalog_data = catalog_func(exchange=exchange)
    
    # Convert to DataFrame if needed
    if not isinstance(catalog_data, pd.DataFrame):
        if hasattr(catalog_data, 'to_dataframe'):
            catalog = catalog_data.to_dataframe()
        else:
            # If it's a dict with 'markets' key (some API endpoints return this format)
            markets = catalog_data.get('markets', [])
            catalog = pd.DataFrame({'market': markets})
    else:
        catalog = catalog_data
    
    # Extract expiration dates from market names
    catalog['expiration_date'] = catalog['market'].apply(extract_expiration_date)
    catalog = catalog[catalog['expiration_date'].notna()]  # Remove rows where expiration couldn't be extracted
    
    # Convert to datetime for comparison
    catalog['expiration_date'] = pd.to_datetime(catalog['expiration_date'])
    
    # Calculate trading period length in days if min_time and max_time columns exist
    if 'min_time' in catalog.columns and 'max_time' in catalog.columns:
        catalog['trading_days'] = (catalog['max_time'] - catalog['min_time']).dt.total_seconds() / (24*60*60)
    
    return catalog

def fetch_markets_by_expiry_date(catalog, start_date, end_date):
    """
    Group markets by expiry date and return only those expiring between start_date and end_date.
    
    Args:
        catalog: DataFrame with market data including expiration_date
        start_date: Start date for expiry range
        end_date: End date for expiry range
        
    Returns:
        Dictionary with expiry dates as keys and lists of market identifiers as values
    """
    # Filter markets by expiry date range
    filtered_catalog = catalog[
        (catalog['expiration_date'] >= start_date) & 
        (catalog['expiration_date'] <= end_date)
    ].copy()
    
    # Group markets by expiry date
    grouped_markets = {}
    for expiry_date, group in filtered_catalog.groupby(filtered_catalog['expiration_date'].dt.date):
        grouped_markets[expiry_date] = group['market'].tolist()
    
    print(f"Found {len(filtered_catalog)} markets across {len(grouped_markets)} expiry dates")
    return grouped_markets

def calculate_data_period_for_expiry(expiry_date, days_before_expiry=22):
    """
    Calculate start and end times for data collection based on expiry date.
    
    Args:
        expiry_date: Expiry date (datetime.date)
        days_before_expiry: Number of days before expiry to start collecting data
    
    Returns:
        Tuple of (start_time, end_time) as datetime objects
    """
    # Convert to datetime for calculations
    expiry_datetime = datetime.combine(expiry_date, datetime.min.time())
    
    # Start collecting data days_before_expiry days before expiry
    start_time = expiry_datetime - timedelta(days=days_before_expiry)
    
    # End collecting data at expiry date
    end_time = expiry_datetime
    
    return start_time, end_time