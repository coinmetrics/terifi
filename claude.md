# Obtain Historical Data on Deribit Options 

historical option prices , implied vols, and Greeks on btc deribit options chains

1. Install the correct libraries using `uv`
2. Check the python client specification for correct functions 
3. use the catalog to obtain availability
4. download the data using parallelization as much as possible 


init client:

```python
from coinmetrics.api_client import CoinMetricsClient
import os

api_key = os.environ.get("CM_API_KEY")
client = CoinMetricsClient(api_key)

```

relevant functions:

catalog_market_greeks_v2(exchange='deribit')

catalog_market_contract_prices_v2(exchange='deribit')

catalog_market_implied_volatility_v2(exchange='deribit')

catalog_market_open_interest_v2(exchange='deribit')

After using the catalog, we can use the markets shown in the catalog to obtain the actual data using the corresponding get function. For example, catalog market Greeks v2 becomes get market Greeks. 

Let's create four separate files: - One for Greeks - One for contract prices - One for implied volatility - One for open interest







