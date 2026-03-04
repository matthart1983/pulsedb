#!/usr/bin/env node
// demo/market-feed.mjs — Live crypto market data feed for PulseDB demo
// Uses CoinGecko free API + simulated price ticks between polls.
// Usage: node demo/market-feed.mjs [--url http://localhost:8087]

const PULSEDB_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:8087'

const ASSETS = 'bitcoin,ethereum,solana,dogecoin,cardano,polkadot,avalanche-2,chainlink'

const SYMBOL_MAP = {
  bitcoin: 'BTC',
  ethereum: 'ETH',
  solana: 'SOL',
  dogecoin: 'DOGE',
  cardano: 'ADA',
  polkadot: 'DOT',
  'avalanche-2': 'AVAX',
  chainlink: 'LINK',
}

// Latest known prices for simulated ticks
const latestPrices = {}

// --- Write buffer ---
let buffer = []
let totalWrites = 0

async function flush() {
  if (buffer.length === 0) return
  const lines = buffer.splice(0, buffer.length)
  const body = lines.join('\n')
  try {
    const res = await fetch(`${PULSEDB_URL}/write`, { method: 'POST', body })
    if (!res.ok) {
      const text = await res.text()
      console.error(`  ✗ Write error: ${res.status} ${text}`)
    } else {
      totalWrites += lines.length
    }
  } catch (e) {
    console.error(`  ✗ Write failed: ${e.message}`)
  }
}

// --- CoinGecko REST polling ---
async function fetchPrices() {
  try {
    const url = `https://api.coingecko.com/api/v3/simple/price?ids=${ASSETS}&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true`
    const res = await fetch(url)
    if (!res.ok) {
      console.error(`  ✗ CoinGecko ${res.status}`)
      return
    }
    const data = await res.json()
    const ts = Date.now() * 1_000_000 // ms → ns

    let count = 0
    for (const [id, info] of Object.entries(data)) {
      const sym = SYMBOL_MAP[id]
      if (!sym) continue

      const price = info.usd
      latestPrices[id] = price

      // Write price tick
      buffer.push(`crypto,symbol=${sym},name=${id} price=${price} ${ts}`)

      // Write enriched market snapshot
      const fields = [
        `price=${price}`,
        `market_cap=${info.usd_market_cap || 0}`,
        `volume_24h=${info.usd_24h_vol || 0}`,
        `change_pct_24h=${info.usd_24h_change || 0}`,
      ].join(',')
      buffer.push(`market,symbol=${sym},name=${id} ${fields} ${ts}`)
      count++
    }
    console.log(`  📊 ${count} assets | ${totalWrites} total points written`)
  } catch (e) {
    console.error(`  ✗ Fetch error: ${e.message}`)
  }
}

// --- Simulated ticks between API polls ---
// Adds realistic price jitter (±0.05%) every 500ms
function simulateTicks() {
  const ts = Date.now() * 1_000_000
  for (const [id, basePrice] of Object.entries(latestPrices)) {
    const sym = SYMBOL_MAP[id]
    if (!sym) continue
    const jitter = 1 + (Math.random() - 0.5) * 0.001 // ±0.05%
    const price = +(basePrice * jitter).toPrecision(8)
    latestPrices[id] = price
    buffer.push(`crypto,symbol=${sym},name=${id} price=${price} ${ts}`)
  }
}

// --- Main ---
console.log(`🚀 PulseDB Market Feed`)
console.log(`   Target: ${PULSEDB_URL}`)
console.log(`   Assets: ${Object.values(SYMBOL_MAP).join(', ')}`)
console.log(`   Source: CoinGecko (free) + simulated ticks`)
console.log()

// Initial fetch
await fetchPrices()
await flush()

// Poll CoinGecko every 10s (free tier rate limit)
setInterval(fetchPrices, 10_000)

// Simulated ticks every 500ms
setInterval(simulateTicks, 500)

// Flush writes every 500ms
setInterval(flush, 500)

// Status log every 15s
setInterval(() => {
  const symbols = Object.entries(latestPrices)
    .filter(([id]) => SYMBOL_MAP[id])
    .map(([id, p]) => `${SYMBOL_MAP[id]}=$${Number(p).toFixed(2)}`)
    .join('  ')
  console.log(`  💹 ${symbols}`)
}, 15_000)

process.on('SIGINT', () => {
  console.log(`\n👋 Shutting down (${totalWrites} points written)`)
  flush().then(() => process.exit(0))
})
