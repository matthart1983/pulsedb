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

Switch any panel to Python mode with the **PY/PL** toggle, or use the pre-configured Python panels from the Demo button.

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

## Recording Demos

### CLI Demo (demo.gif)

Records the PulseLang REPL and Python REPL using [VHS](https://github.com/charmbracelet/vhs):

```bash
vhs demo.tape
```

### UI Demo (ui-demo.gif)

Records the web dashboard using Playwright. Shows PulseLang live panels, Python panels, chart interaction, query editing, and PY/PL language toggling.

**Prerequisites:**
1. PulseDB server + UI running: `./dev.sh`
2. Market data feed: `node demo/market-feed.mjs`
3. Wait a few seconds for data to accumulate

**Record:**
```bash
node demo/record-demo.mjs
```

**What gets recorded:**
1. Click Demo → loads 8 panels (6 PulseLang + 2 Python)
2. BTC chart crosshair interaction
3. Run Python DB Overview panel (shows measurements + fields)
4. Run Python Price Alerts panel (BTC high/low/spread analysis)
5. Edit PulseLang panel → EMA pipeline query
6. Toggle ETH panel from PulseLang to Python, write + run query
7. Edit Python panel → custom measurement analysis
8. Final hold showing all live data
