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
