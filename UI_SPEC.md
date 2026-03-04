# PulseDB UI — Real-Time Visualization Dashboard

## 1. Overview

A lightweight React-based dashboard for PulseDB that connects to the database HTTP API, executes PulseLang queries, and renders real-time visualizations with sub-second refresh. Inspired by the information density and responsiveness of professional trading terminals (Bloomberg Terminal, Tradingview, Grafana).

### Design Philosophy

- **Terminal-grade density** — Dark theme, compact layout, no wasted pixels. Every element serves a purpose.
- **Query-driven** — The PulseLang editor is the primary interface. Type a query, see it visualized instantly.
- **Real-time first** — Live-updating charts with configurable polling intervals. Data streams in, charts redraw.
- **Zero configuration** — Point at a PulseDB instance, start querying. No dashboard setup wizards.
- **Keyboard-centric** — Power users never touch the mouse. Full keyboard navigation and shortcuts.

### Non-Goals (v1)

- Dashboard persistence / saved layouts (future: share via URL-encoded state)
- User authentication (PulseDB is single-node; auth is out of scope)
- Alert configuration or notification systems
- Data write/ingestion from the UI

---

## 2. Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| Framework | React 19 + TypeScript | Industry standard, large ecosystem |
| Build | Vite 6 | Fast HMR, minimal config |
| Charts | [Lightweight Charts v5](https://github.com/nicepagents/lightweight-charts) | TradingView's open-source charting library — WebGL-accelerated, 60fps, built for financial time-series |
| Supplementary charts | [Recharts](https://recharts.org) | Bar, scatter, heatmap, pie — covers non-time-series visualizations |
| Layout | CSS Grid + [react-grid-layout](https://github.com/react-grid-layout/react-grid-layout) | Drag-and-resize tile grid |
| Code editor | [CodeMirror 6](https://codemirror.net/6/) | PulseLang syntax highlighting, autocomplete, inline errors |
| State | Zustand | Minimal boilerplate, good for real-time update patterns |
| Styling | Tailwind CSS 4 | Utility-first, dark theme out of the box |
| HTTP | Native `fetch` + `EventSource` | No heavy HTTP libraries; SSE for streaming |
| Data format | JSON | Matches existing PulseDB `/lang` and `/query` endpoints |
| Testing | Vitest + React Testing Library | Fast, Vite-native |

### Why Lightweight Charts?

TradingView's Lightweight Charts is purpose-built for the exact use case: rendering large time-series datasets at 60fps with real-time updates. It handles:
- Candlestick, line, area, bar, histogram, and baseline chart types
- Crosshair with snapping and multi-pane sync
- Time scale with auto-formatting (ns → seconds → minutes → hours → days)
- Incremental data append (no full redraw on new data)
- WebGL rendering for 100K+ data points without jank

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        PulseDB UI (React)                        │
│                                                                  │
│  ┌──────────────┐  ┌──────────────────────┐  ┌───────────────┐  │
│  │  Query Editor │  │   Visualization Grid  │  │  Status Bar   │  │
│  │  (CodeMirror) │  │   (react-grid-layout) │  │  (connection, │  │
│  │              │  │                        │  │   latency,    │  │
│  │  PulseLang   │  │  ┌──────┐ ┌──────┐   │  │   points)     │  │
│  │  autocomplete│  │  │Chart │ │Chart │   │  │               │  │
│  │  syntax HL   │  │  │Tile  │ │Tile  │   │  └───────────────┘  │
│  │  error spans │  │  │      │ │      │   │                     │
│  │              │  │  └──────┘ └──────┘   │  ┌───────────────┐  │
│  │  [Run] [Live]│  │  ┌──────┐ ┌──────┐   │  │  Data Table   │  │
│  │              │  │  │Chart │ │Chart │   │  │  (raw values) │  │
│  └──────────────┘  │  │Tile  │ │Tile  │   │  └───────────────┘  │
│                    │  └──────┘ └──────┘   │                     │
│                    └──────────────────────┘                     │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │                    Query Result Inspector                     ││
│  │  Type: table | Rows: 1,000 | Elapsed: 119µs | Columns: 3    ││
│  └──────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────┘
         │                                              ▲
         │  POST /lang  {"q": "mavg[10; cpu.usage]"}    │
         │  POST /query {"q": "SELECT ..."}             │
         ▼                                              │
┌──────────────────────────────────────────────────────────────────┐
│                    PulseDB Server (HTTP :8087)                    │
│                                                                  │
│  /lang    → PulseLang interpreter → JSON response                │
│  /query   → PulseQL engine → JSON response                       │
│  /status  → Engine stats → JSON                                  │
│  /health  → Liveness → JSON                                      │
│  /ws      → WebSocket (new: streaming query results)             │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. User types PulseLang expression in the editor
2. On `Cmd+Enter` (or live mode tick), POST to `/lang` with the query
3. Response contains `{ result, type, elapsed_ns }`
4. Result parser inspects `type` field and routes to the appropriate visualization
5. In live mode, a timer re-executes the query at the configured interval
6. Chart components receive new data via Zustand store updates and incrementally redraw

---

## 4. Server-Side Additions

### 4.1 Structured `/lang` Response

The current `/lang` endpoint returns `{ result: string, type: string, elapsed_ns: u64 }`. The result is a display string, which the UI would need to parse. A new `/lang/json` endpoint (or `Accept: application/json` header) should return structured data:

```jsonc
// POST /lang/json  {"q": "cpu.usage_idle"}
{
  "type": "float[]",
  "data": [98.2, 97.8, 96.5, 95.1],
  "elapsed_ns": 119000
}

// POST /lang/json  {"q": "cpu"}
{
  "type": "table",
  "data": {
    "columns": ["ts", "usage_idle", "usage_system"],
    "ts": [1704067200000000000, 1704067500000000000],
    "usage_idle": [98.2, 97.8],
    "usage_system": [1.3, 1.5]
  },
  "elapsed_ns": 184000
}

// POST /lang/json  {"q": "avg cpu.usage_idle"}
{
  "type": "float",
  "data": 97.15,
  "elapsed_ns": 122000
}

// POST /lang/json  {"q": "`a`b`c ! 10 20 30"}
{
  "type": "dict",
  "data": {"a": 10, "b": 20, "c": 30},
  "elapsed_ns": 800
}
```

### 4.2 WebSocket Endpoint (Stretch Goal)

For true real-time streaming without polling:

```
WS /ws/lang
→ Client sends: {"subscribe": "cpu.usage_idle", "interval_ms": 1000}
← Server pushes: {"type": "float[]", "data": [...], "ts": 1704067200000} every 1s
→ Client sends: {"unsubscribe": true}
```

This avoids the overhead of HTTP request/response per tick and enables server-push when new data arrives.

### 4.3 CORS Headers

The HTTP server must add CORS headers for local development:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Accept
```

### 4.4 `/measurements` Endpoint

New endpoint for the autocomplete system:

```jsonc
// GET /measurements
{
  "measurements": [
    {
      "name": "cpu",
      "fields": ["usage_idle", "usage_system", "usage_user"],
      "tags": ["host", "region"],
      "series_count": 42,
      "point_count": 1000000
    }
  ]
}
```

---

## 5. UI Layout

### 5.1 Overall Structure

```
┌─────────────────────────────────────────────────────────────────────┐
│ ▸ PulseDB   │ ⚡ Connected :8087  │  ◉ 42 series  │  12.3M pts    │
├─────────────┴───────────────────────┴───────────────┴───────────────┤
│                                                                     │
│  ┌─────────────────────────────┐  ┌────────────────────────────┐   │
│  │                             │  │                            │   │
│  │    CHART TILE 1             │  │    CHART TILE 2            │   │
│  │    ──────────               │  │    ──────────              │   │
│  │    mavg[10; cpu.usage]      │  │    deltas cpu.usage        │   │
│  │                             │  │                            │   │
│  │    ~~~~~~~~~~~~             │  │    ▃▅▇▅▃▁▃▅▇▅▃           │   │
│  │    ~~~  ~~~~~~~~            │  │                            │   │
│  │                             │  │                            │   │
│  └─────────────────────────────┘  └────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────┐  ┌────────────────────────────┐   │
│  │                             │  │                            │   │
│  │    SCALAR TILE              │  │    TABLE TILE              │   │
│  │    ───────────              │  │    ──────────              │   │
│  │                             │  │    ts    | usage | system  │   │
│  │    avg cpu.usage            │  │    14:00 | 98.2  | 1.3    │   │
│  │         97.15               │  │    14:05 | 97.8  | 1.5    │   │
│  │         ▲ 0.3%              │  │    14:10 | 96.5  | 2.1    │   │
│  │                             │  │                            │   │
│  └─────────────────────────────┘  └────────────────────────────┘   │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  pulse▸ mavg[10; cpu.usage] @ `host = `server01        [▶ Run] [◉] │
│                                                                     │
│  Result: float[] (1000 points)  │  119µs  │  Live: 1s refresh      │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Component Hierarchy

```
App
├── StatusBar                    // Connection indicator, stats, latency
├── TileGrid                     // react-grid-layout container
│   ├── ChartTile                // Individual visualization panel
│   │   ├── TileHeader           // Query text, chart type selector, close/maximize
│   │   ├── ChartRenderer        // Dispatches to correct chart component
│   │   │   ├── TimeSeriesChart  // Lightweight Charts (line/area/candle)
│   │   │   ├── BarChart         // Recharts bar/histogram
│   │   │   ├── ScalarDisplay    // Big number + delta indicator
│   │   │   ├── TableView        // Virtualized data table
│   │   │   ├── HeatmapChart     // Recharts heatmap
│   │   │   └── VectorSpark      // Inline sparkline for vectors
│   │   └── TileFooter           // Elapsed time, point count, type
│   └── AddTileButton            // "+" button to add new tile
├── QueryPanel                   // Bottom panel: editor + controls
│   ├── PulseLangEditor          // CodeMirror 6 with PulseLang mode
│   ├── RunButton                // Execute query
│   ├── LiveToggle               // Enable/disable auto-refresh
│   ├── IntervalSelector         // Refresh interval (100ms–30s)
│   └── ResultMeta               // Type, count, elapsed
└── CommandPalette               // Cmd+K overlay for all actions
```

---

## 6. Visual Design

### 6.1 Color System

Inspired by Bloomberg Terminal / TradingView Pro dark theme:

```
Background:
  --bg-primary:     #0a0a0f       // Main background (near-black)
  --bg-secondary:   #12121a       // Tile/card background
  --bg-tertiary:    #1a1a28       // Editor, inputs
  --bg-hover:       #22223a       // Hover states

Borders:
  --border-subtle:  #1e1e2e       // Tile borders
  --border-active:  #2d2d44       // Focused tile border

Text:
  --text-primary:   #e0e0e8       // Primary text
  --text-secondary: #8888a0       // Labels, metadata
  --text-muted:     #55556a       // Disabled, timestamps

Accent (cyan — matches PulseDB branding):
  --accent-primary: #00d4ff       // Primary actions, chart lines
  --accent-hover:   #33ddff       // Hover state
  --accent-dim:     #00d4ff22     // Chart area fill

Data Colors (chart series):
  --series-1:       #00d4ff       // Cyan (primary)
  --series-2:       #ff6b6b       // Red
  --series-3:       #51cf66       // Green
  --series-4:       #ffd43b       // Yellow
  --series-5:       #cc5de8       // Purple
  --series-6:       #ff922b       // Orange
  --series-7:       #20c997       // Teal
  --series-8:       #748ffc       // Indigo

Semantic:
  --positive:       #00e676       // Up / positive delta
  --negative:       #ff1744       // Down / negative delta
  --neutral:        #8888a0       // Unchanged
  --warning:        #ff9100       // Warnings
  --error:          #ff1744       // Errors
```

### 6.2 Typography

```
Font stack:
  --font-mono:    'JetBrains Mono', 'SF Mono', 'Fira Code', monospace
  --font-sans:    'Inter', -apple-system, sans-serif

Sizes:
  --text-xs:      11px           // Timestamps, metadata
  --text-sm:      12px           // Labels, axis labels
  --text-base:    13px           // Body text, table cells
  --text-lg:      15px           // Tile headers
  --text-xl:      20px           // Scalar values
  --text-2xl:     32px           // Hero scalar (big number tiles)

Everything in the editor and data displays uses the monospace font.
Sans-serif is reserved for UI chrome (buttons, menus, status bar).
```

### 6.3 Chart Styling

All charts follow a consistent visual language:

- **Grid lines**: `#1e1e2e` (barely visible, like trading terminal grids)
- **Axis labels**: `--text-muted` (#55556a), 11px monospace
- **Crosshair**: Dotted line, `--text-secondary`, with value label tooltip
- **Chart line width**: 1.5px (thin, high-density)
- **Area fill**: Series color at 8% opacity
- **No chart borders** — tiles float with subtle box-shadow
- **Animations**: 120ms ease-out transitions for data updates (not bouncy, not sluggish)

### 6.4 Tile Design

```
┌──────────────────────────────────────────┐
│  mavg[10; cpu.usage]    ≡  📊  ↗  ✕     │  ← Header: query, chart type, maximize, close
│──────────────────────────────────────────│
│                                          │
│      ~~~~~~~~                            │  ← Chart area (fills remaining space)
│   ~~~        ~~~~~~~~~~                  │
│  ~                      ~~~~~            │
│                               ~~~~~~~~   │
│                                          │
│──────────────────────────────────────────│
│  float[] │ 1,000 pts │ 119µs │ ◉ 1s     │  ← Footer: type, count, latency, live indicator
└──────────────────────────────────────────┘
```

- **Tile border radius**: 6px
- **Tile gap**: 8px
- **Tile shadow**: `0 2px 8px rgba(0,0,0,0.4)`
- **Active tile**: Cyan left-border accent (3px)
- **Drag handle**: ≡ icon in header (grip dots)

---

## 7. Core Features

### 7.1 PulseLang Editor

**CodeMirror 6 configuration:**

- **Syntax highlighting** for PulseLang tokens:
  - Keywords: `select`, `from`, `by`, `within`, `where` → cyan
  - Builtins: `avg`, `sum`, `mavg`, `ema`, `deltas`, etc. → blue
  - Symbols: `` `host ``, `` `server01 `` → green
  - Strings: `"hello"` → yellow
  - Numbers/durations: `42`, `3.14`, `5m`, `1h` → orange
  - Operators: `+`, `-`, `*`, `%`, `|>`, `@` → white
  - Comments: `/ ...` → muted gray
  - Timestamps: `2024.01.15D14:30:00` → purple
- **Autocomplete** (triggered by `.` or `Ctrl+Space`):
  - Measurement names (fetched from `/measurements`)
  - Field names after `measurement.` prefix
  - Builtin function names
  - Tag names after `@` `` ` ``
  - Duration units after digits (`5` → suggest `m`, `h`, `s`, `d`)
- **Inline error display**: Red squiggly underline at the span reported by the server error
- **Multi-line support**: Shift+Enter for newline, Cmd+Enter to execute
- **History**: Up/Down arrow cycles through previous queries (stored in localStorage)
- **Snippets**: Common patterns accessible via Cmd+K palette

### 7.2 Automatic Chart Type Detection

The UI inspects the PulseLang result type and auto-selects the best visualization:

| Result Type | Auto Chart | Rationale |
|---|---|---|
| `float` / `int` | **Scalar Display** | Single big number with optional delta from previous |
| `float[]` / `int[]` | **Line Chart** | Time-series sparkline (x-axis = index if no timestamps) |
| `table` with `ts` column | **Time-Series Chart** | Full Lightweight Charts with time x-axis |
| `table` without `ts` | **Data Table** | Virtualized scrollable table |
| `dict` | **Bar Chart** | Keys as categories, values as bars |
| `bool` | **Status Indicator** | Green/red dot |
| `bool[]` | **Heatmap Strip** | Horizontal strip of green/red cells |
| `sym[]` / `str[]` | **Data Table** | List view |

Users can override the auto-detected chart type via a dropdown in the tile header.

### 7.3 Chart Types

#### Time-Series Line Chart (Lightweight Charts)

Primary chart for time-series data. Supports:
- **Multiple series overlay** (e.g., `cpu.usage_idle` and `mavg[10; cpu.usage_idle]`)
- **Area fill** below line (toggleable)
- **Crosshair** with synchronized tooltip showing all series values
- **Time axis** auto-formats: nanoseconds → seconds → minutes → hours → days
- **Price scale** (y-axis) with auto-range or manual min/max
- **Zoom**: Mouse wheel on time axis, pinch gesture
- **Pan**: Click-drag on chart area
- **Legend**: Inline, shows current value of each series

#### Candlestick Chart

For OHLC financial data. Triggered when table has `open`, `high`, `low`, `close` columns:
- Green body for close > open, red for close < open
- Wick lines for high/low
- Volume histogram overlay (optional, bottom 20% of chart)

#### Bar Chart (Recharts)

For dictionaries and categorical data:
- Horizontal or vertical bars
- Sorted by value (descending) by default
- Gradient fill matching series colors
- Value labels on bars

#### Scalar Display

For single-value results (`avg cpu.usage`, `count cpu`):
- Large number, center-aligned in tile
- Delta indicator: ▲ 2.3% (green) or ▼ 1.1% (red) vs previous value
- Subtitle: query text in muted gray
- Optional sparkline below the number (last N values in live mode)

#### Data Table

For table and list results:
- Virtualized rows (react-window) — handles 100K+ rows
- Sortable columns (click header)
- Fixed header row
- Alternating row shading (`--bg-secondary` / `--bg-tertiary`)
- Timestamp columns auto-formatted to human-readable
- Float columns right-aligned, 4 decimal places
- Copy cell/row/column to clipboard

#### Heatmap

For matrix data or time-bucketed aggregations:
- Color scale from `--bg-primary` (low) to `--accent-primary` (high)
- Cell labels (value)
- Axis labels

### 7.4 Live Mode (Real-Time Updates)

When live mode is enabled (◉ toggle in the query panel):

1. A timer fires at the configured interval (default: 1000ms)
2. The current query is re-executed against the server
3. New data is diffed against the current dataset
4. Charts update incrementally:
   - **Lightweight Charts**: `series.update()` appends new points, auto-scrolls
   - **Scalars**: Number transitions with CSS animation (count-up effect)
   - **Tables**: New rows prepend at top with a brief highlight flash
5. The status bar shows a pulsing ◉ indicator and the last update timestamp

**Interval options**: 100ms, 250ms, 500ms, 1s, 2s, 5s, 10s, 30s

**Backpressure**: If a query takes longer than the interval, the next tick is skipped. A warning appears in the footer.

### 7.5 Multi-Tile Grid

- **Drag to rearrange** tiles (react-grid-layout handles collision detection)
- **Resize** via bottom-right handle (chart reflows to fill)
- **Maximize** a tile to full-screen (Escape to restore)
- **Default layouts**:
  - Single tile (full width)
  - 2×1 side-by-side
  - 2×2 grid
  - 1 large + 2 small (trading layout)
- **Each tile has its own query and refresh interval**
- **Synchronized crosshair**: Hovering over one time-series chart shows the crosshair at the same timestamp on all other time-series tiles

### 7.6 Keyboard Shortcuts

| Shortcut | Action |
|---|---|
| `Cmd+Enter` | Execute query |
| `Cmd+L` | Focus query editor |
| `Cmd+K` | Open command palette |
| `Cmd+N` | Add new tile |
| `Cmd+W` | Close focused tile |
| `Cmd+Shift+F` | Toggle fullscreen on focused tile |
| `Escape` | Exit fullscreen / close palette |
| `Cmd+S` | Snapshot layout to URL hash |
| `Cmd+1..9` | Focus tile by number |
| `Tab` | Cycle focus between tiles |
| `Cmd+Shift+L` | Toggle live mode |
| `` ` `` | Toggle query panel (show/hide) |
| `Up/Down` | Query history (when editor focused) |

---

## 8. API Client Layer

### 8.1 PulseDB Client

```typescript
interface PulseDBClient {
  // Connection
  connect(url: string): Promise<void>;
  health(): Promise<{ status: string }>;
  status(): Promise<ServerStatus>;

  // PulseLang queries
  queryLang(expr: string): Promise<LangResult>;
  queryLangJson(expr: string): Promise<LangJsonResult>;

  // PulseQL queries
  querySQL(sql: string): Promise<QueryResult>;

  // Metadata
  measurements(): Promise<Measurement[]>;

  // Streaming (future)
  subscribe(expr: string, intervalMs: number, callback: (data: LangJsonResult) => void): Unsubscribe;
}

interface LangJsonResult {
  type: ValueType;
  data: any;         // typed based on `type` field
  elapsed_ns: number;
}

type ValueType =
  | 'int' | 'uint' | 'float' | 'bool' | 'str' | 'sym'
  | 'ts' | 'dur' | 'null'
  | 'int[]' | 'float[]' | 'bool[]' | 'sym[]' | 'str[]' | 'ts[]'
  | 'list' | 'dict' | 'table';

interface ServerStatus {
  version: string;
  series_count: number;
  points_in_memtable: number;
  segment_count: number;
}

interface Measurement {
  name: string;
  fields: string[];
  tags: string[];
  series_count: number;
  point_count: number;
}
```

### 8.2 Result Parser

Transforms raw JSON responses into chart-ready data structures:

```typescript
interface ChartData {
  chartType: 'line' | 'bar' | 'scalar' | 'table' | 'candle' | 'heatmap' | 'status';
  series: SeriesData[];
  meta: ResultMeta;
}

interface SeriesData {
  name: string;
  data: { time: number; value: number }[];  // for line/area
  // or
  categories: string[];                      // for bar
  values: number[];
}

interface ResultMeta {
  type: ValueType;
  pointCount: number;
  elapsedNs: number;
  columns?: string[];
}
```

---

## 9. State Management (Zustand)

```typescript
interface DashboardStore {
  // Connection
  serverUrl: string;
  connected: boolean;
  serverStatus: ServerStatus | null;
  setServerUrl: (url: string) => void;

  // Tiles
  tiles: TileState[];
  addTile: (query: string) => void;
  removeTile: (id: string) => void;
  updateTileQuery: (id: string, query: string) => void;
  updateTileData: (id: string, data: ChartData) => void;
  updateTileLayout: (layout: LayoutItem[]) => void;

  // Editor
  currentQuery: string;
  queryHistory: string[];
  setQuery: (q: string) => void;
  executeQuery: () => Promise<void>;

  // Live mode
  liveEnabled: boolean;
  liveIntervalMs: number;
  toggleLive: () => void;
  setInterval: (ms: number) => void;

  // UI
  focusedTileId: string | null;
  commandPaletteOpen: boolean;
  queryPanelVisible: boolean;
}

interface TileState {
  id: string;
  query: string;
  chartType: ChartType;          // auto-detected or manual override
  chartTypeOverride?: ChartType; // user override
  data: ChartData | null;
  loading: boolean;
  error: string | null;
  liveEnabled: boolean;
  liveIntervalMs: number;
  lastUpdated: number;           // timestamp
}
```

---

## 10. Project Structure

```
ui/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.ts
├── public/
│   └── favicon.svg
├── src/
│   ├── main.tsx                    // Entry point
│   ├── App.tsx                     // Root layout
│   ├── api/
│   │   ├── client.ts               // PulseDB HTTP client
│   │   └── parser.ts               // Result → ChartData transformer
│   ├── store/
│   │   └── dashboard.ts            // Zustand store
│   ├── components/
│   │   ├── StatusBar.tsx            // Top bar: connection, stats
│   │   ├── QueryPanel.tsx           // Bottom bar: editor + controls
│   │   ├── TileGrid.tsx             // react-grid-layout wrapper
│   │   ├── ChartTile.tsx            // Individual tile container
│   │   ├── TileHeader.tsx           // Query label, controls
│   │   ├── TileFooter.tsx           // Metadata display
│   │   ├── CommandPalette.tsx       // Cmd+K overlay
│   │   └── charts/
│   │       ├── TimeSeriesChart.tsx   // Lightweight Charts wrapper
│   │       ├── CandlestickChart.tsx  // OHLC chart
│   │       ├── BarChart.tsx          // Recharts bar
│   │       ├── ScalarDisplay.tsx     // Big number + delta
│   │       ├── DataTable.tsx         // Virtualized table
│   │       ├── HeatmapChart.tsx      // Heatmap
│   │       └── ChartRenderer.tsx     // Type → chart dispatcher
│   ├── editor/
│   │   ├── PulseLangMode.ts         // CodeMirror language mode
│   │   ├── PulseLangComplete.ts     // Autocomplete provider
│   │   └── PulseLangEditor.tsx      // Editor component
│   ├── hooks/
│   │   ├── useQuery.ts              // Query execution hook
│   │   ├── useLive.ts               // Live mode timer hook
│   │   ├── useKeyboard.ts           // Keyboard shortcut hook
│   │   └── useCrosshair.ts          // Synchronized crosshair hook
│   ├── lib/
│   │   ├── colors.ts                // Color constants
│   │   ├── format.ts                // Number/timestamp formatters
│   │   └── detect.ts                // Auto chart type detection
│   └── types/
│       └── index.ts                 // Shared TypeScript types
└── tests/
    ├── parser.test.ts               // Result parser tests
    ├── detect.test.ts               // Chart type detection tests
    └── client.test.ts               // API client tests
```

---

## 11. Implementation Phases

### Phase 1: Foundation (MVP)

**Goal**: Single-tile dashboard with query editor and auto-detected chart.

- [ ] Vite + React + TypeScript + Tailwind scaffold
- [ ] PulseDB HTTP client (`/lang`, `/status`, `/health`)
- [ ] Zustand store (single tile, query state)
- [ ] CodeMirror editor with basic PulseLang syntax highlighting
- [ ] Result parser (type inspection → ChartData)
- [ ] TimeSeriesChart component (Lightweight Charts)
- [ ] ScalarDisplay component
- [ ] DataTable component (basic, no virtualization)
- [ ] StatusBar with connection indicator
- [ ] Execute on Cmd+Enter
- [ ] Dark theme base CSS

**Server changes**: Add CORS headers to `http.rs`, add `/lang/json` endpoint.

### Phase 2: Multi-Tile + Live Mode

**Goal**: Grid of independently-queried tiles with real-time refresh.

- [ ] react-grid-layout integration
- [ ] Multi-tile state management
- [ ] Per-tile query execution
- [ ] Live mode with configurable interval
- [ ] Add/remove/resize tiles
- [ ] Tile header with chart type override dropdown
- [ ] Bar chart component (Recharts)
- [ ] Query history (localStorage)
- [ ] Backpressure handling for slow queries

### Phase 3: Editor Polish + Autocomplete

**Goal**: Professional-grade editor experience.

- [ ] `/measurements` endpoint on server
- [ ] Autocomplete: measurement names, field names, builtins, tags
- [ ] Inline error display with span highlighting
- [ ] Multi-line editing (Shift+Enter)
- [ ] Snippet system via command palette
- [ ] Full keyboard shortcut system
- [ ] Command palette (Cmd+K)

### Phase 4: Advanced Charts + Interactions

**Goal**: Trading-terminal-grade visualization.

- [ ] Candlestick chart for OHLC data
- [ ] Heatmap chart
- [ ] Synchronized crosshair across tiles
- [ ] Chart zoom/pan with time range sync
- [ ] Virtualized DataTable (react-window, 100K+ rows)
- [ ] Tile maximize/fullscreen
- [ ] Delta indicators on scalar displays (vs previous value)
- [ ] Sparkline in scalar tiles (rolling history in live mode)

### Phase 5: Streaming + Polish

**Goal**: True real-time and production polish.

- [ ] WebSocket endpoint on server (`/ws/lang`)
- [ ] Client-side WebSocket subscription
- [ ] Incremental chart updates (append, not replace)
- [ ] Layout persistence (URL hash state)
- [ ] Export chart as PNG
- [ ] Export data as CSV
- [ ] Print-friendly light theme
- [ ] Performance audit: 60fps with 10 tiles, 10K points each

---

## 12. Performance Targets

| Metric | Target |
|---|---|
| Initial load (cold) | < 1.5s (< 200KB gzipped JS) |
| Query → chart render | < 100ms (excluding server latency) |
| Live mode jank | 0 dropped frames at 1s interval |
| Max data points per chart | 100K+ (Lightweight Charts handles this) |
| Max concurrent tiles | 20 |
| Memory (10 tiles, 10K pts each) | < 100MB |
| Tile resize reflow | < 50ms |

---

## 13. Example Workflows

### Real-Time CPU Monitoring

```
Tile 1: cpu.usage_idle @ `host = `server01     → Line chart, live 1s
Tile 2: avg cpu.usage_idle                      → Scalar, live 5s
Tile 3: mavg[60; cpu.usage_idle]               → Line chart (smoothed), live 1s
Tile 4: cpu                                     → Table (all fields), live 5s
```

### Financial Analysis

```
Tile 1: trades.price @ `sym = `AAPL             → Line chart, live 100ms
Tile 2: ema[0.1; trades.price]                  → Line overlay, live 100ms
Tile 3: mavg[20; trades.price] - trades.price   → Area chart (mean reversion)
Tile 4: select avg(price) from trades by 5m     → Candlestick (OHLC bucketed)
```

### IoT Sensor Dashboard

```
Tile 1: sensor.temperature @ `device = `D-42    → Line chart, live 2s
Tile 2: sensor.humidity @ `device = `D-42        → Line chart (separate axis)
Tile 3: avg sensor.temperature                   → Scalar with delta
Tile 4: count sensor                             → Scalar (total readings)
```

---

## 14. Open Questions

1. **PulseLang multi-expression**: Should the UI support `;`-separated multi-line expressions where intermediate results are discarded and only the last is visualized? (Yes — needed for variable assignment then use.)
2. **Chart annotations**: Should users be able to add horizontal/vertical reference lines (e.g., alert thresholds)? (Phase 4+)
3. **Dark/light theme toggle**: Trading terminals are universally dark, but some users prefer light. Add in Phase 5.
4. **Mobile responsive**: Not a priority for v1. Trading terminals are desktop-first. Minimum supported width: 1024px.
5. **Embeddable mode**: Should individual tiles be embeddable in external pages via iframe? (Future consideration.)
