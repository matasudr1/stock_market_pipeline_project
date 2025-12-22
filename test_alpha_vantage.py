import requests
import json
import os

api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
if not api_key:
    raise ValueError('ALPHA_VANTAGE_API_KEY environment variable is required')
ticker = 'AAPL'

url = 'https://www.alphavantage.co/query'
params = {
    'function': 'TIME_SERIES_DAILY_ADJUSTED',
    'symbol': ticker,
    'outputsize': 'compact',
    'apikey': api_key,
    'datatype': 'json'
}

print(f"Testing Alpha Vantage for {ticker}")
print(f"URL: {url}")
print(f"Params: {params}\n")

response = requests.get(url, params=params, timeout=30)
print(f"Status: {response.status_code}")
print(f"Response length: {len(response.text)} characters\n")

data = response.json()
print("Keys in response:")
for key in data.keys():
    print(f"  - {key}")

print("\nFull response (first 1000 chars):")
print(json.dumps(data, indent=2)[:1000])
