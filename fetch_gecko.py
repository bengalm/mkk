import urllib.request, json

# Fetch SOL market data from CoinGecko
url = 'https://api.coingecko.com/api/v3/simple/price?ids=solana&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true'
try:
    req = urllib.request.Request(url, headers={'User-Agent': 'mkk-bot/1.0'})
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read().decode())
    print(json.dumps(data, indent=2))
except Exception as e:
    print(f"CoinGecko failed: {e}")
