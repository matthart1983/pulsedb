#!/usr/bin/env node
// demo/record-demo.mjs — Record a PulseUI live demo GIF
// Prerequisites: PulseDB server + market feed running (./dev.sh + node demo/market-feed.mjs)
// Usage: node demo/record-demo.mjs

import { chromium } from 'playwright'
import { execSync } from 'child_process'
import { existsSync } from 'fs'

const URL = 'http://localhost:3000'
const VIDEO_DIR = '/tmp/pulsedb-demo-video'
const OUTPUT_GIF = 'ui-demo.gif'
const WIDTH = 1280
const HEIGHT = 720

async function main() {
  console.log('🎬 Recording PulseUI demo...')

  execSync(`rm -rf "${VIDEO_DIR}" && mkdir -p "${VIDEO_DIR}"`)

  const browser = await chromium.launch({ headless: false })
  const context = await browser.newContext({
    viewport: { width: WIDTH, height: HEIGHT },
    recordVideo: { dir: VIDEO_DIR, size: { width: WIDTH, height: HEIGHT } },
    deviceScaleFactor: 2,
  })

  const page = await context.newPage()
  await page.goto(URL, { waitUntil: 'networkidle' })
  console.log('  ✓ Page loaded')

  // Brief pause on empty state
  await sleep(800)

  // --- Scene 1: Click Demo to load 4 live panels ---
  console.log('  ▸ Loading demo dashboard')
  await page.click('button:has-text("Demo")')
  await sleep(4000) // watch data populate

  // --- Scene 2: Mouse sweep across BTC chart crosshair ---
  console.log('  ▸ Chart interaction')
  const chartPanel = page.locator('div:has(> div.drag-handle span:has-text("BTC Chart"))')
  if (await chartPanel.first().isVisible()) {
    const box = await chartPanel.first().boundingBox()
    if (box) {
      for (let i = 0; i < 10; i++) {
        const x = box.x + box.width * 0.15 + (box.width * 0.7 * i) / 9
        const y = box.y + box.height * 0.45
        await page.mouse.move(x, y)
        await sleep(120)
      }
    }
  }
  await sleep(1500)

  // --- Scene 3: Edit Market Overview → multi-agg select query ---
  console.log('  ▸ Editing query → multi-agg select')
  const marketPanel = page.locator('div:has(> div.drag-handle span:has-text("Market Overview"))')
  if (await marketPanel.first().isVisible()) {
    const editor = marketPanel.first().locator('.cm-content')
    if (await editor.isVisible()) {
      await editor.click()
      await sleep(200)
      await page.keyboard.press('Meta+a')
      await sleep(150)
      await typeSlowly(page, 'select avg price, max price, min price, dev price from market where symbol = `BTC', 30)
      await sleep(500)
      await page.keyboard.press('Meta+Enter')
      await sleep(3000)
    }
  }

  // --- Scene 4: Edit ETH panel → pipeline volatility query ---
  console.log('  ▸ Editing query → pipeline volatility')
  const ethPanel = page.locator('div:has(> div.drag-handle span:has-text("ETH Price"))')
  if (await ethPanel.first().isVisible()) {
    const editor = ethPanel.first().locator('.cm-content')
    if (await editor.isVisible()) {
      await editor.click()
      await sleep(200)
      await page.keyboard.press('Meta+a')
      await sleep(150)
      await typeSlowly(page, 'crypto.price @ `symbol = `BTC |> deltas |> {x * x} |> avg |> sqrt', 30)
      await sleep(500)
      await page.keyboard.press('Meta+Enter')
      await sleep(3000)
    }
  }

  // --- Final hold: live updates visible ---
  console.log('  ▸ Final hold')
  // Move mouse out of panels
  await page.mouse.move(WIDTH / 2, 20)
  await sleep(3000)

  // Done
  const videoPath = await page.video()?.path()
  await context.close()
  await browser.close()

  if (!videoPath || !existsSync(videoPath)) {
    console.error('✗ No video recorded')
    process.exit(1)
  }
  console.log(`  ✓ Video: ${videoPath}`)

  // Convert to GitHub-friendly GIF (<10MB)
  console.log('  ▸ Converting to GIF...')
  const palette = '/tmp/pulsedb-demo-palette.png'

  execSync(
    `ffmpeg -y -i "${videoPath}" -vf "fps=10,scale=960:-1:flags=lanczos,palettegen=max_colors=96:stats_mode=diff" "${palette}"`,
    { stdio: 'pipe' }
  )
  execSync(
    `ffmpeg -y -i "${videoPath}" -i "${palette}" -lavfi "fps=10,scale=960:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=5" "${OUTPUT_GIF}"`,
    { stdio: 'pipe' }
  )

  const size = execSync(`ls -lh "${OUTPUT_GIF}" | awk '{print $5}'`).toString().trim()
  console.log(`\n✅ ${OUTPUT_GIF} (${size})`)
}

async function typeSlowly(page, text, delayMs) {
  for (const char of text) {
    await page.keyboard.type(char, { delay: 0 })
    await sleep(delayMs)
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

main().catch((e) => {
  console.error('Error:', e)
  process.exit(1)
})
