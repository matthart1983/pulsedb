# PulseDB Live Demo

## Market Data Feed

Streams live cryptocurrency prices from CoinGecko into PulseDB with simulated inter-poll ticks.

### Start

```bash
# Terminal 1: Start PulseDB + PulseUI
./dev.sh

# Terminal 2: Start market data feed
node demo/market-feed.mjs
```

Then open http://localhost:3000 and click the **⚡ Demo** button in the top bar to load pre-configured live panels.

### Demo PulseLang Queries

Try these in panels with the **⊙ Live** toggle enabled:

| Query | Type | Description |
|---|---|---|
| `last crypto.price @ \`symbol = \`BTC` | Scalar | Latest Bitcoin price |
| `last crypto.price @ \`symbol = \`ETH` | Scalar | Latest Ethereum price |
| `crypto @ \`symbol = \`BTC` | Chart | BTC price history |
| `crypto @ \`symbol = \`SOL` | Chart | SOL price history |
| `market` | Table | Full market data (all coins) |
| `market @ \`symbol = \`ETH` | Table | ETH market detail |
| `avg crypto.price @ \`symbol = \`BTC` | Scalar | Average BTC price |
| `max crypto.price @ \`symbol = \`ETH` | Scalar | Max ETH price observed |
| `count crypto.price` | Scalar | Total price ticks ingested |

### Demo Python Queries (Viper)

Launch the Python REPL connected to the same database:

```bash
pulsedb python --data-dir ./pulsedb_data
```

Then try:

```python
# List all measurements
print(db_measurements())

# Query data using PulseLang expressions
prices = db_query("crypto.price @ `symbol = `BTC")
print(prices)

# Aggregations
avg_price = db_query("avg crypto.price @ `symbol = `ETH")
print(avg_price)

# Insert data from Python
db_insert("alerts", {"level": 1, "message": "test"}, {"source": "python"})

# Use Python logic with live data
vals = db_query("crypto.price @ `symbol = `BTC")
for v in vals:
    if v > 50000.0:
        print("BTC above 50k: " + str(v))

# Get field names for a measurement
print(db_fields("crypto"))
```

Run a `.py` script:

```bash
pulsedb python -f my_analysis.py --data-dir ./pulsedb_data
```
