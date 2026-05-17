import websocket, hmac, hashlib, base64, time, json

conn = websocket.create_connection("wss://ws.okx.com:8443/ws/v5/private", timeout=10)
print("Connected!")

# Try Unix epoch timestamp (seconds) adjusted for clock offset
ts = str(int(time.time()) - 27)
print(f"Using Unix timestamp: {ts}")

message = ts + "GET" + "/users/self/verify"
sign = base64.b64encode(hmac.new("B80C6E5FE2535D137DC1BC67FDC67647".encode(), message.encode(), hashlib.sha256).digest()).decode()

login = {"op": "login", "args": [{"apiKey": "ab7fda31-97d8-4fdb-9aaa-00930bc6d954", "passphrase": "mzqmzqmzq1!M", "timestamp": ts, "sign": sign}]}
conn.send(json.dumps(login))
resp = conn.recv()
print(f"Response: {resp}")
conn.close()
