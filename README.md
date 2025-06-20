# Terifi - Getting Started with Options Data

Terifi is a tool for downloading and analyzing historical options data from Deribit, focusing on Bitcoin options. It collects Greeks, implied volatility, contract prices, and open interest data using the CoinMetrics API.

## Features

- Downloads options data organized by expiration dates
- Collects data from a configurable period before expiration (default: 22 days)
- Supports multiple data types:
  - Options Greeks (delta, gamma, vega, theta, rho)
  - Implied volatility
  - Contract prices
  - Open interest
- Parallelized data collection for improved performance
- Includes analysis and visualization tools

## Requirements

- Python 3.10+
- CoinMetrics API Key
- UV package manager (recommended)

## Installation

1. Clone the repository
```bash
git clone https://github.com/coinmetrics/terifi.git
cd terifi
```

2. Set up a virtual environment using UV
```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install required packages
```bash
uv pip install coinmetrics-api-client pandas matplotlib numpy pytz
```

4. Set your CoinMetrics API key as an environment variable
```bash
export CM_API_KEY="your_api_key_here"  # On Windows: set CM_API_KEY=your_api_key_here
```

## Usage

### Basic Usage

To collect all data types for options expiring in the next 30 days (default):
```bash
uv run main.py
```

### Command Line Options

- `--start-date YYYY-MM-DD`: Specify start date for option expiry search
- `--end-date YYYY-MM-DD`: Specify end date for option expiry search
- `--days-before-expiry N`: Collect data starting N days before each expiry (default: 22)
- `--granularity VALUE`: Set data granularity (default: 1d, options: 1d, 1h, etc.)
- `--greeks-only`: Only collect Greeks data
- `--iv-only`: Only collect implied volatility data
- `--prices-only`: Only collect contract price data
- `--oi-only`: Only collect open interest data

### Examples

Collect only Greeks data for options expiring between June 1 and July 1, 2025:
```bash
uv run main.py --greeks-only --start-date 2025-06-01 --end-date 2025-07-01
```

Collect all data types for options expiring in the next 60 days, starting 30 days before expiry:
```bash
uv run main.py --end-date $(date -d "+60 days" +%Y-%m-%d) --days-before-expiry 30
```

Collect hourly granularity data for options expiring in the next 7 days:
```bash
uv run main.py --end-date $(date -d "+7 days" +%Y-%m-%d) --granularity 1h
```

### Analyzing Data

To analyze the catalog of available options on Deribit (useful to determine optimal data collection parameters):
```bash
uv run analyze_catalog.py
```

To visualize the Greeks data for specific options:
```bash
uv run greeks_viz.py  # Visualizes example BTC options
```

For a comprehensive summary of collected data:
```bash
uv run greeks_summary.py  # Analyzes and validates collected data
```

## Data Structure

Data is saved in CSV format in the following directories:
- `market-greeks/`: Options Greeks (delta, gamma, vega, theta, rho)
- `market-impliedvolatility/`: Implied volatility data
- `market-contractprices/`: Contract price data
- `market-openinterest/`: Open interest data

Files are named using the format `deribit-BTC-DDMMMYY-STRIKE-TYPE-option.csv` (e.g., `deribit-BTC-13DEC24-100000-C-option.csv`).

## Project Structure

- `main.py`: Entry point for data collection
- `analyze_catalog.py`: Tool to analyze available options data
- `greeks.py`: Code for collecting Greeks data
- `implied_volatility.py`: Code for collecting implied volatility data
- `contract_prices.py`: Code for collecting contract prices data
- `open_interest.py`: Code for collecting open interest data
- `market_utils.py`: Shared utility functions
- `greeks_viz.py`: Visualization tool for Greeks data
- `greeks_summary.py`: Analysis and validation of collected data

## Recommendations

Based on analysis of historical data patterns:
- Collecting data starting 22 days before expiration captures ~90% of significant trading activity
- The most dramatic changes in Greeks occur in the final 1-2 weeks before expiration
- Both call and put options should be analyzed together for complete price dynamics understanding

## License

[MIT License](LICENSE)

## Acknowledgements

This project uses the [CoinMetrics API](https://docs.coinmetrics.io/api/v4) for data collection.