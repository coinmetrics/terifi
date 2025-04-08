import os
import asyncio
from datetime import datetime, timedelta
import argparse

# Import individual data collection modules
import greeks
import contract_prices
import implied_volatility
import open_interest

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Collect Deribit options data.')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--days-before-expiry', type=int, default=22, 
                       help='Days before expiry to collect data (default: 22)')
    parser.add_argument('--greeks-only', action='store_true', 
                       help='Only collect Greeks data')
    parser.add_argument('--iv-only', action='store_true', 
                       help='Only collect implied volatility data')
    parser.add_argument('--prices-only', action='store_true', 
                       help='Only collect contract price data')
    parser.add_argument('--oi-only', action='store_true', 
                       help='Only collect open interest data')
    args = parser.parse_args()
    
    # Define date range for data collection
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        end_date = datetime.utcnow()
        
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    else:
        start_date = end_date - timedelta(days=30)  # Default to 30 days before end date
    
    days_before_expiry = args.days_before_expiry
    
    print("\n===== Starting Deribit Options Data Collection =====\n")
    print(f"Looking for options expiring between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
    print(f"For each expiry date, collecting data starting {days_before_expiry} days before expiry")
    
    # Determine which data types to collect
    if args.greeks_only or args.iv_only or args.prices_only or args.oi_only:
        # Only collect selected data types
        tasks = []
        if args.greeks_only:
            print("Collecting Greeks data only")
            tasks.append(greeks.save_greeks_data(start_date, end_date, days_before_expiry=days_before_expiry))
        if args.iv_only:
            print("Collecting implied volatility data only")
            tasks.append(implied_volatility.save_implied_volatility_data(start_date, end_date, days_before_expiry=days_before_expiry))
        if args.prices_only:
            print("Collecting contract price data only")
            tasks.append(contract_prices.save_contract_price_data(start_date, end_date, days_before_expiry=days_before_expiry))
        if args.oi_only:
            print("Collecting open interest data only")
            tasks.append(open_interest.save_open_interest_data(start_date, end_date, days_before_expiry=days_before_expiry))
    else:
        # Collect all data types
        tasks = [
            greeks.save_greeks_data(start_date, end_date, days_before_expiry=days_before_expiry),
            contract_prices.save_contract_price_data(start_date, end_date, days_before_expiry=days_before_expiry),
            implied_volatility.save_implied_volatility_data(start_date, end_date, days_before_expiry=days_before_expiry),
            open_interest.save_open_interest_data(start_date, end_date, days_before_expiry=days_before_expiry)
        ]
    
    await asyncio.gather(*tasks)
    
    print("\n===== Data Collection Complete =====\n")
    print("Data has been saved to CSV files in the current directory.")
    
    print("\nTo analyze the collected data, run: python analyze_catalog.py")

if __name__ == "__main__":
    # Check if API key is set
    if not os.environ.get("CM_API_KEY"):
        print("ERROR: CM_API_KEY environment variable not set!")
        print("Please set the environment variable with your CoinMetrics API key:")
        print("export CM_API_KEY='your_api_key_here'")
    else:
        asyncio.run(main())
