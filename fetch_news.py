import json, urllib.request, datetime
try:
    url = "https://min-api.cryptocompare.com/data/v2/news/?categories=SOL&extraParams=mkk-bot"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode())
    news = data.get('Data', [])[:5]
    for n in news:
        ts = n.get('published_on', 0)
        dt = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d') if ts else ''
        print(f"{dt} | {n.get('title','')[:120]}")
except Exception as e:
    print(f"News fetch failed: {e}")
