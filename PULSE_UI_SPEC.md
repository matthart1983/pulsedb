# PulseUI — Real-Time Visualization Dashboard for PulseDB

## 1. Overview

PulseUI is a lightweight, modern React-based dashboard for PulseDB that provides real-time data visualization powered by PulseLang queries. Inspired by professional trading platforms (Bloomberg Terminal, TradingView, Refinitiv Eikon), it delivers a dark-themed, information-dense workspace where analysts can write PulseLang expressions and instantly see results rendered as high-performance interactive charts, tables, and monitors — all updating live as new data arrives.

### Design Philosophy

- **Terminal-grade density** — Maximum information per pixel. No wasted whitespace. Every panel earns its space.
- **Dark-first** — Deep charcoal/navy backgrounds (#0a0e17, #111827) with high-contrast data colors. Reduces eye strain during extended sessions.
- **Keyboard-driven** — Power users never leave the keyboard. Every action has a shortcut.
- **Zero-lag rendering** — WebGL-accelerated charts. 60fps panning/zooming on 100K+ data points.
- **Query-first workflow** — PulseLang is the primary interaction model. Type a query, see a chart. Edit the query, the chart redraws.

### Non-Goals (v1)

- Mobile responsiveness (desktop-first, 1440px+ minimum)
- User authentication / multi-tenancy
- Persistent dashboard storage on server (localStorage only in v1)
- Drawing tools / annotations (v2)
- Alerting / notifications (v2)

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         PulseUI (Browser)                       │
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────────────────────────┐ │
│  │  Query Editor     │  │  Visualization Canvas               │ │
│  │  (CodeMirror 6)   │  │  ┌────────┐ ┌────────┐ ┌────────┐  │ │
│  │                   │  │  │ Line   │ │ OHLC   │ │ Heat   │  │ │
│  │  PulseLang input  │  │  │ Chart  │ │ Candle │ │ Map    │  │ │
│  │  with syntax      │  │  │        │ │        │ │        │  │ │
│  │  highlighting     │  │  └────────┘ └────────┘ └────────┘  │ │
│  │                   │  │  ┌────────┐ ┌────────┐ ┌────────┐  │ │
│  │  Query history    │  │  │ Table  │ │ Scalar │ │ Spark  │  │ │
│  │  Autocomplete     │  │  │ Grid   │ │ Cards  │ │ Lines  │  │ │
│  └──────────────────┘  │  └────────┘ └────────┘ └────────┘  │ │
│                        └──────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Connection Layer                                        │   │
│  │  ├─ REST: POST /lang {q: "..."} → JSON result           │   │
│  │  ├─ REST: POST /query {q: "..."} → JSON result          │   │
│  │  ├─ REST: GET /status → engine stats                    │   │
│  │  └─ WebSocket: ws://host:8087/ws → streaming updates    │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ HTTP / WebSocket
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PulseDB Server                               │
│  HTTP :8087                                                     │
│  ├─ POST /lang    → PulseLang eval → JSON result               │
│  ├─ POST /query   → PulseQL eval → JSON result                 │
│  ├─ GET  /status  → engine statistics                          │
│  └─ WS   /ws      → subscribe to query, push on new writes    │
│  TCP :8086                                                      │
│  └─ Line protocol ingestion                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Server-Side Changes Required

The existing PulseDB HTTP server needs two additions:

1. **Enhanced `/lang` response** — Return structured JSON (typed arrays, tables as column-oriented JSON) instead of display strings, so the UI can render charts without re-parsing.

2. **WebSocket endpoint `/ws`** — Accept subscription messages containing a PulseLang query + poll interval. Server re-evaluates the query on each interval (or on memtable write) and pushes delta updates.

```rust
// Enhanced /lang response
{
  "type": "table",           // "int", "float", "int[]", "float[]", "table", "dict", ...
  "columns": ["ts", "usage_idle", "usage_system"],
  "data": {
    "ts": [1704067200000000000, 1704067500000000000, ...],
    "usage_idle": [98.2, 97.8, ...],
    "usage_system": [1.3, 1.5, ...]
  },
  "row_count": 1000,
  "elapsed_ns": 118000
}

// Scalar response
{
  "type": "float",
  "value": 42.5,
  "elapsed_ns": 1200
}

// Vector response
{
  "type": "float[]",
  "values": [1.0, 2.0, 3.0, 4.0, 5.0],
  "elapsed_ns": 850
}
```

```
// WebSocket subscription message (client → server)
{ "action": "subscribe", "id": "panel-1", "query": "cpu.usage_idle @ `host = `server01", "interval_ms": 1000 }

// WebSocket data push (server → client)
{ "id": "panel-1", "type": "table", "data": { ... }, "timestamp": 1704067500000000000 }

// Unsubscribe
{ "action": "unsubscribe", "id": "panel-1" }
```

---

## 3. Tech Stack

| Layer | Library | Rationale |
|---|---|---|
| Framework | React 19 + TypeScript 5 | Standard, wide ecosystem, hooks for state |
| Build | Vite 6 | Sub-second HMR, ESM-native, tiny config |
| State | Zustand | Lightweight (~1KB), no boilerplate, perfect for panel state |
| Charts (primary) | Lightweight Charts™ 5 (TradingView) | WebGL time-series charts, 60fps on 100K points, OHLC/area/line/histogram, free & open source |
| Charts (supplementary) | D3.js 7 (selective) | Heatmaps, custom visualizations, axes only — no full charting framework weight |
| Tables | TanStack Table v8 | Headless, virtualized, sortable, filterable — handles 100K rows |
| Editor | CodeMirror 6 | PulseLang syntax highlighting, autocomplete, inline errors, lightweight |
| Layout | react-grid-layout | Draggable, resizable panels. Serializable to JSON for dashboard persistence |
| Styling | Tailwind CSS 4 + CSS variables | Utility-first, dark theme via CSS custom properties, zero runtime |
| Icons | Lucide React | Clean, consistent, tree-shakeable |
| HTTP | Native fetch + SWR | SWR for caching/revalidation, native fetch for simplicity |
| WebSocket | Native WebSocket + reconnecting logic | No library needed; custom hook with auto-reconnect |
| Fonts | JetBrains Mono (editor), Inter (UI) | Monospace for code/data, proportional for labels |

### Bundle Target

- **Initial load**: < 200KB gzipped (React + Vite + Zustand + Tailwind + CodeMirror)
- **Chart libraries**: Lazy-loaded per panel type (~80KB for Lightweight Charts)
- **Total interactive**: < 400KB gzipped

---

## 4. UI Layout & Design

### 4.1 Overall Layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│ ▸ PulseDB    ⊞ Layout ▾   🔌 Connected    cpu: 3 series    00:14:32   │  ← Top bar
├──────────────────────────────────┬──────────────────────────────────────┤
│                                  │                                      │
│  ┌────────────────────────────┐  │  ┌────────────────────────────────┐  │
│  │     Time-Series Chart      │  │  │     Scalar / KPI Cards        │  │
│  │     (Lightweight Charts)   │  │  │     ┌──────┐┌──────┐┌──────┐  │  │
│  │                            │  │  │     │ AVG  ││ MAX  ││ P99  │  │  │
│  │     Line / Area / OHLC     │  │  │     │98.2  ││100.0 ││99.7  │  │  │
│  │     Crosshair + tooltip    │  │  │     └──────┘└──────┘└──────┘  │  │
│  │     Y-axis auto-scale      │  │  └────────────────────────────────┘  │
│  │                            │  │  ┌────────────────────────────────┐  │
│  └────────────────────────────┘  │  │     Heatmap / Correlation      │  │
│  ┌────────────────────────────┐  │  │     (D3 canvas)               │  │
│  │     Query Editor           │  │  └────────────────────────────────┘  │
│  │     ┌──────────────────┐   │  │                                      │
│  │     │ cpu.usage_idle   │   │  │                                      │
│  │     │ @ `host=`web01   │   │  │                                      │
│  │     │ |> mavg[10; _]   │   │  │                                      │
│  │     └──────────────────┘   │  │                                      │
│  │     ⏱ 118µs  ⟳ 1s        │  │                                      │
│  └────────────────────────────┘  │                                      │
├──────────────────────────────────┴──────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Data Table (TanStack)                                           │   │
│  │  ts                  │ usage_idle │ usage_system │ host           │   │
│  │  2024-01-15 14:00:00 │ 98.2       │ 1.3          │ server01       │   │
│  │  2024-01-15 14:05:00 │ 97.8       │ 1.5          │ server01       │   │
│  │  ... (virtualized, 100K rows)                                    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Color System

```css
/* Background layers (darkest → lightest) */
--bg-base:      #0a0e17;    /* Main background — near-black navy */
--bg-surface:   #111827;    /* Panel backgrounds */
--bg-elevated:  #1a2332;    /* Hover states, active panels */
--bg-overlay:   #1f2937;    /* Dropdowns, modals */

/* Borders */
--border:       #1e293b;    /* Subtle panel dividers */
--border-focus: #3b82f6;    /* Focus rings — electric blue */

/* Text */
--text-primary:   #e2e8f0;  /* Primary text — warm white */
--text-secondary: #94a3b8;  /* Labels, secondary info */
--text-muted:     #64748b;  /* Disabled, timestamps */

/* Data colors — optimized for dark backgrounds */
--chart-blue:     #3b82f6;  /* Primary series */
--chart-cyan:     #06b6d4;  /* Secondary series */
--chart-green:    #10b981;  /* Positive / up */
--chart-red:      #ef4444;  /* Negative / down */
--chart-amber:    #f59e0b;  /* Warning / highlight */
--chart-purple:   #8b5cf6;  /* Tertiary series */
--chart-pink:     #ec4899;  /* Quaternary series */

/* Semantic */
--positive:     #10b981;    /* Green — values going up */
--negative:     #ef4444;    /* Red — values going down */
--neutral:      #6b7280;    /* Gray — unchanged */

/* Glow effects for real-time updates */
--glow-blue:    0 0 20px rgba(59, 130, 246, 0.3);
--glow-green:   0 0 20px rgba(16, 185, 129, 0.3);
```

### 4.3 Typography

```css
/* Editor & data */
font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace;
font-size: 13px;
line-height: 1.5;
font-feature-settings: 'liga' 1;  /* Ligatures for operators */

/* UI labels */
font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
font-size: 12px;
font-weight: 500;
letter-spacing: 0.01em;

/* Numbers in charts/tables — tabular figures for alignment */
font-variant-numeric: tabular-nums;
```

---

## 5. Core Components

### 5.1 Query Editor Panel

The primary interaction surface. A CodeMirror 6 editor with PulseLang-specific features.

**Features:**
- Syntax highlighting for PulseLang (keywords, operators, symbols, strings, timestamps, durations)
- Autocomplete for:
  - Builtin functions (`avg`, `sum`, `mavg`, `ema`, ...)
  - Measurement names (fetched from `/status` or new `/measurements` endpoint)
  - Field names (fetched on `.` after measurement name)
- Inline error display (red underline + tooltip from server error response)
- Query history (↑/↓ arrows cycle through previous queries, stored in localStorage)
- Multi-line support with Shift+Enter for newlines, Enter to execute
- Execution timer showing elapsed microseconds from server response
- Auto-refresh toggle with configurable interval (1s, 5s, 10s, 30s, 1m, off)
- Keyboard shortcut: `Cmd+Enter` / `Ctrl+Enter` to execute

**Auto-visualization:** When a query returns:
- **Scalar** (`int`, `float`) → Large KPI card with delta indicator
- **Vector** (`int[]`, `float[]`) → Sparkline or line chart
- **Table with `ts` column** → Time-series line chart (auto-detect numeric columns as series)
- **Table without `ts`** → Data grid (TanStack Table)
- **Dict** → Key-value card or bar chart
- **BoolVec** → Heatmap strip

The user can override auto-visualization with a chart type selector dropdown.

### 5.2 Time-Series Chart (Lightweight Charts)

The main visualization for time-indexed data. Uses TradingView's Lightweight Charts for WebGL-accelerated rendering.

**Chart types:**
- **Line** — Default for single series. Anti-aliased, configurable width/color.
- **Area** — Line with gradient fill. Good for volume-under-curve.
- **Candlestick / OHLC** — For pre-aggregated OHLC data. Requires `open`, `high`, `low`, `close` columns.
- **Histogram** — Vertical bars for volume, counts, bucketed data.
- **Baseline** — Line with positive/negative coloring around a reference value.

**Interactions:**
- **Crosshair** — Vertical + horizontal lines following cursor, showing exact time + value in tooltip
- **Pan** — Click + drag to pan. Mouse wheel to zoom time axis.
- **Auto-scale** — Y-axis auto-fits to visible data range. Manual override by dragging price scale.
- **Time range selector** — Quick buttons: 1m, 5m, 15m, 1h, 4h, 1d, 1w, All
- **Multi-series overlay** — Up to 8 series on one chart, each with its own color and optional separate Y-axis
- **Legend** — Inline legend showing series name, current value, color swatch. Click to toggle visibility.
- **Snapshot** — Export chart as PNG via right-click menu

**Real-time behavior:**
- New data points append to the right edge
- Chart auto-scrolls to keep the latest point visible (unless user has panned away)
- Latest value shows as a flashing price line on the Y-axis

### 5.3 Scalar / KPI Cards

For single-value results (`avg cpu.usage_idle` → `98.2`).

**Layout:**
```
┌──────────────────────┐
│  CPU Usage (avg)     │  ← label (from query or user-set)
│                      │
│    98.2%             │  ← large value, monospace, --chart-green
│    ▲ +0.3            │  ← delta from previous value, colored
│                      │
│  ⏱ 1.2µs  ⟳ 1s     │  ← timing + refresh indicator
└──────────────────────┘
```

**Features:**
- Value color changes based on delta direction (green up, red down)
- Subtle glow animation on value change (CSS box-shadow transition)
- Configurable thresholds for color zones (e.g., >95 green, 80-95 amber, <80 red)
- Mini sparkline of recent values (last 60 data points) below the main value
- Font size auto-scales to fill available card space

### 5.4 Data Table

For raw tabular results. Uses TanStack Table with virtualization for large datasets.

**Features:**
- Virtual scrolling — renders only visible rows, handles 100K+ rows smoothly
- Column sorting (click header to toggle asc/desc)
- Column resizing (drag header borders)
- Timestamp formatting — nanosecond epoch displayed as human-readable with configurable format
- Number formatting — configurable decimal places, thousands separators
- Conditional cell coloring — numeric cells colored on a gradient (green-to-red) based on value
- Frozen header row
- CSV export button
- Row count indicator: "1,247 of 10,000 rows"

### 5.5 Heatmap

For correlation matrices, time-bucketed data, or any 2D numeric grid.

**Implementation:** D3.js rendering to a `<canvas>` element for performance.

**Features:**
- Color scale: sequential (blue→red) or diverging (red→white→green)
- Cell hover tooltip showing exact value
- Axis labels from column/row names
- Auto-sizing cells to fill available space

### 5.6 Multi-Chart Sparklines

For overview dashboards showing many series at once. Minimal chrome, maximum data.

```
┌────────────────────────────────────────┐
│  cpu.usage_idle   ▁▂▃▄▅▆▇█▇▆▅   98.2  │
│  cpu.usage_system ▁▁▁▁▂▁▁▁▁▁▁    1.3  │
│  mem.available    ████████████  8.0GB  │
│  disk.io_time     ▂▃▂▃▅▇▅▃▂▁▂    4.2  │
└────────────────────────────────────────┘
```

Rendered with inline `<canvas>` elements. Lightweight, no axes, just the shape + latest value.

---

## 6. Panel System & Layout

### 6.1 Grid Layout

Powered by `react-grid-layout`. Each panel occupies grid cells on a 24-column grid.

**Features:**
- Drag panels by title bar to reposition
- Resize from bottom-right corner handle
- Snap-to-grid for clean alignment
- Layout serialization to/from JSON (saved in localStorage)
- Preset layouts: "Single Chart", "Quad View", "Dashboard", "Analysis"

### 6.2 Panel Anatomy

Every panel shares a common frame:

```
┌─ Panel Title ────────────────── [📊▾] [⟳] [⚙] [✕] ─┐
│                                                       │
│  (Visualization content)                              │
│                                                       │
│                                                       │
│                                                       │
├─ Query: cpu.usage_idle |> mavg[10;_]  ──── ⏱ 118µs ─┤
└───────────────────────────────────────────────────────┘

[📊▾]  Chart type selector (line, area, candle, histogram, table, scalar)
[⟳]    Refresh / auto-refresh toggle
[⚙]    Panel settings (colors, axes, thresholds)
[✕]    Close panel
```

### 6.3 Panel Types

| Type | Query Result | Renderer | Use Case |
|---|---|---|---|
| `timeseries` | Table with `ts` column | Lightweight Charts | Time-indexed metrics |
| `scalar` | Single value | KPI Card | Aggregation results |
| `table` | Any table/dict | TanStack Table | Raw data exploration |
| `sparklines` | Multiple vectors | Canvas sparklines | Overview dashboard |
| `heatmap` | 2D numeric data | D3 Canvas | Correlation, distribution |
| `histogram` | Vector | Lightweight Charts histogram | Distribution, counts |
| `editor` | — | CodeMirror | Query input (always present) |

### 6.4 Dashboard Persistence

```typescript
interface Dashboard {
  id: string;
  name: string;
  panels: PanelConfig[];
  layout: LayoutItem[];        // react-grid-layout format
  created: number;
  modified: number;
}

interface PanelConfig {
  id: string;
  type: 'timeseries' | 'scalar' | 'table' | 'sparklines' | 'heatmap' | 'histogram' | 'editor';
  query: string;               // PulseLang expression
  refreshInterval: number;     // milliseconds, 0 = manual only
  settings: {
    title?: string;
    chartType?: string;
    colors?: string[];
    yAxisRange?: [number, number];
    thresholds?: { value: number; color: string }[];
    decimalPlaces?: number;
    timeFormat?: string;
  };
}
```

Dashboards stored in `localStorage` under key `pulseui:dashboards`. Import/export as JSON file.

---

## 7. Real-Time Data Flow

### 7.1 Polling Mode (v1 default)

Each panel with `refreshInterval > 0` runs an independent timer that:
1. Sends `POST /lang` with the panel's query
2. Receives structured JSON response
3. Diffs against previous result
4. Updates chart/table with new data (append for time-series, replace for aggregations)

**Adaptive polling:** If a query consistently returns identical results, double the interval (up to 60s). If data changes, reset to configured interval.

### 7.2 WebSocket Mode (v1 stretch)

For true real-time streaming:

```typescript
// Client subscribes
ws.send(JSON.stringify({
  action: 'subscribe',
  id: 'panel-1',
  query: 'cpu.usage_idle @ `host = `server01',
  interval_ms: 1000,
}));

// Server pushes updates
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  // msg: { id: 'panel-1', type: 'table', data: {...}, timestamp: ... }
  updatePanel(msg.id, msg.data);
};
```

**Server implementation:** A background tokio task per subscription that:
1. Re-evaluates the PulseLang query at the specified interval
2. Compares result hash against previous push
3. If changed, serializes and sends via WebSocket
4. Uses `tokio::select!` to handle unsubscribe or client disconnect

### 7.3 Update Animations

When data changes:
- **Scalar cards**: Value morphs with a CSS transition (0.3s ease), brief green/red glow
- **Time-series charts**: New point appears at right edge with smooth animation
- **Tables**: Changed cells flash briefly (0.5s highlight fade)
- **Sparklines**: Redraw with requestAnimationFrame, left-shift + append

---

## 8. Query Workflow UX

### 8.1 Write → Visualize Loop

```
1. User types PulseLang in editor
   │
2. Presses Cmd+Enter (or waits 500ms debounce for live mode)
   │
3. POST /lang { q: "avg cpu.usage_idle" }
   │
4. Server returns { type: "float", value: 98.2, elapsed_ns: 1200 }
   │
5. UI auto-selects visualization:
   ├─ float/int → Scalar KPI card
   ├─ float[]/int[] → Line chart (x-axis = index)
   ├─ table with ts → Time-series chart
   ├─ table without ts → Data grid
   └─ dict → Key-value display or bar chart
   │
6. User can override: click chart-type dropdown → select different viz
   │
7. User enables auto-refresh (⟳ button) → panel re-queries every N seconds
   │
8. Click "Pin to Dashboard" → panel added to grid layout
```

### 8.2 Multi-Panel Queries

Panels can reference each other. A "variables" panel lets users define shared bindings:

```
/ Variables panel (always evaluated first)
host: `server01
interval: 5m
start: 2024.01.15D00:00:00

/ Panel 1 query
cpu.usage_idle @ `host = host within (start; start + 24h)

/ Panel 2 query
avg cpu.usage_idle @ `host = host
```

Changing a variable re-triggers all dependent panels.

### 8.3 Keyboard Shortcuts

| Shortcut | Action |
|---|---|
| `Cmd+Enter` | Execute query |
| `Cmd+Shift+Enter` | Execute + pin to dashboard |
| `Cmd+K` | Focus query editor (command palette) |
| `Cmd+N` | New panel |
| `Cmd+S` | Save dashboard |
| `Cmd+D` | Duplicate panel |
| `Cmd+W` | Close active panel |
| `Cmd+1..9` | Focus panel 1-9 |
| `Esc` | Unfocus / close dropdown |
| `↑` / `↓` | Cycle query history (in editor) |
| `Cmd+Shift+F` | Toggle fullscreen for active panel |

---

## 9. PulseLang Editor Features

### 9.1 Syntax Highlighting

CodeMirror 6 grammar for PulseLang:

| Token Type | Color | Examples |
|---|---|---|
| Keyword | `#c792ea` (purple) | `select`, `from`, `by`, `within`, `where` |
| Builtin function | `#82aaff` (blue) | `avg`, `sum`, `mavg`, `ema`, `deltas` |
| Number | `#f78c6c` (orange) | `42`, `3.14`, `0n`, `0w` |
| String | `#c3e88d` (green) | `"hello"` |
| Symbol | `#ffcb6b` (amber) | `` `host ``, `` `server01 `` |
| Timestamp | `#89ddff` (cyan) | `2024.01.15D14:30:00` |
| Duration | `#89ddff` (cyan) | `5m`, `1h`, `30s` |
| Operator | `#89ddff` (cyan) | `+`, `-`, `*`, `%`, `|>`, `@` |
| Comment | `#546e7a` (gray) | `/ this is a comment` |
| Lambda brace | `#c792ea` (purple) | `{`, `}` |
| Bracket | `#e2e8f0` (white) | `[`, `]`, `(`, `)` |

### 9.2 Autocomplete

Triggered on:
- Typing a letter (function/variable names)
- After `.` (field names for known measurements)
- After `@` (tag keys)
- After `\`` (symbol autocomplete from known tag values)

**Completion sources:**
1. **Builtin functions** — Static list of all PulseLang builtins (40+ functions)
2. **Measurements** — Fetched from server: `GET /measurements` → `["cpu", "mem", "disk", ...]`
3. **Fields** — Fetched on demand: `GET /fields?measurement=cpu` → `["usage_idle", "usage_system", ...]`
4. **Tag keys/values** — Fetched on demand for `@` context
5. **User variables** — From current session's assignment history

### 9.3 Error Display

When a query fails, the server returns an error with position info:

```json
{ "error": "undefined variable: foo", "line": 1, "col": 15 }
```

The editor:
- Underlines the error location in red
- Shows error message in a tooltip on hover
- Displays the error in a status bar below the editor

---

## 10. Connection & Status Bar

### 10.1 Top Bar

```
┌─────────────────────────────────────────────────────────────────────────┐
│  ◆ PulseDB   │  ⊞ Layout ▾  │  + New Panel  │        │  🔌 Connected  │
│              │              │               │        │  cpu: 3 series │
│              │  Single      │               │        │  mem: 2 series │
│              │  Quad        │               │        │  ⏱ 00:14:32   │
│              │  Dashboard   │               │        │                │
│              │  Analysis    │               │        │                │
└─────────────────────────────────────────────────────────────────────────┘
```

### 10.2 Connection Status

- 🟢 **Connected** — Server responding, WebSocket connected
- 🟡 **Reconnecting** — Connection lost, attempting reconnect (exponential backoff)
- 🔴 **Disconnected** — Server unreachable after max retries

The connection indicator also shows:
- Server URL (configurable)
- Active series count
- Uptime since connection

### 10.3 Status Poller

Every 10 seconds, `GET /status` to display:
- Series count
- Points in memtable
- Segment count
- Connection latency (measured from request timing)

---

## 11. Server API Enhancements

### 11.1 Enhanced `/lang` Endpoint

The current `/lang` endpoint returns a display string. For the UI, we need structured JSON:

```rust
#[derive(Serialize)]
#[serde(tag = "type")]
enum LangResult {
    #[serde(rename = "int")]
    Int { value: i64, elapsed_ns: u64 },
    #[serde(rename = "float")]
    Float { value: f64, elapsed_ns: u64 },
    #[serde(rename = "bool")]
    Bool { value: bool, elapsed_ns: u64 },
    #[serde(rename = "str")]
    Str { value: String, elapsed_ns: u64 },
    #[serde(rename = "int[]")]
    IntVec { values: Vec<i64>, elapsed_ns: u64 },
    #[serde(rename = "float[]")]
    FloatVec { values: Vec<f64>, elapsed_ns: u64 },
    #[serde(rename = "table")]
    Table {
        columns: Vec<String>,
        data: HashMap<String, serde_json::Value>,  // column name → array
        row_count: usize,
        elapsed_ns: u64,
    },
    #[serde(rename = "dict")]
    Dict { entries: HashMap<String, serde_json::Value>, elapsed_ns: u64 },
    #[serde(rename = "null")]
    Null { elapsed_ns: u64 },
}
```

### 11.2 New Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/lang` | POST | Execute PulseLang, return structured JSON |
| `/measurements` | GET | List all measurement names |
| `/fields` | GET | List fields for a measurement (`?measurement=cpu`) |
| `/tags` | GET | List tag keys/values for a measurement |
| `/ws` | WebSocket | Real-time query subscriptions |

### 11.3 CORS Configuration

Add CORS headers to all HTTP responses:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

---

## 12. Project Structure

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
│   ├── main.tsx                      # Entry point
│   ├── App.tsx                       # Root layout, grid system
│   ├── stores/
│   │   ├── dashboard.ts              # Zustand: panels, layout, dashboards
│   │   ├── connection.ts             # Zustand: server URL, status, WebSocket
│   │   └── query-history.ts          # Zustand: query history per panel
│   ├── api/
│   │   ├── client.ts                 # HTTP client (fetch + error handling)
│   │   ├── types.ts                  # API response types
│   │   └── websocket.ts             # WebSocket manager with reconnect
│   ├── components/
│   │   ├── TopBar.tsx                # Connection status, layout selector
│   │   ├── Panel.tsx                 # Panel frame (title bar, controls)
│   │   ├── PanelGrid.tsx             # react-grid-layout wrapper
│   │   ├── QueryEditor.tsx           # CodeMirror 6 with PulseLang mode
│   │   ├── TimeSeriesChart.tsx       # Lightweight Charts wrapper
│   │   ├── ScalarCard.tsx            # KPI card with delta + sparkline
│   │   ├── DataTable.tsx             # TanStack Table with virtualization
│   │   ├── Heatmap.tsx               # D3 canvas heatmap
│   │   ├── SparklineRow.tsx          # Mini sparkline list
│   │   ├── HistogramChart.tsx        # Lightweight Charts histogram
│   │   └── ChartTypeSelector.tsx     # Dropdown for viz type override
│   ├── codemirror/
│   │   ├── pulselang.ts              # Language grammar definition
│   │   ├── theme.ts                  # Dark theme matching PulseUI colors
│   │   └── completions.ts            # Autocomplete sources
│   ├── hooks/
│   │   ├── useQuery.ts               # Execute PulseLang, manage loading/error
│   │   ├── usePolling.ts             # Auto-refresh timer with adaptive interval
│   │   ├── useWebSocket.ts           # WebSocket connection + subscription
│   │   └── useChartData.ts           # Transform API response → chart series
│   ├── utils/
│   │   ├── format.ts                 # Timestamp formatting, number formatting
│   │   ├── colors.ts                 # Chart color palette, series color assignment
│   │   ├── auto-viz.ts               # Auto-detect best visualization for result type
│   │   └── debounce.ts               # Debounce utility for live mode
│   └── styles/
│       └── globals.css               # CSS variables, base styles, Tailwind imports
└── README.md
```

---

## 13. Implementation Phases

### Phase 1 — Foundation (MVP)

**Goal:** Single-panel query → visualization. Prove the loop works.

- [x] Vite + React + TypeScript + Tailwind project setup
- [x] Connection to PulseDB server (`POST /lang`, `GET /status`)
- [x] CORS support on PulseDB server
- [x] Enhanced `/lang` endpoint returning structured JSON
- [x] Query editor (CodeMirror 6 with basic PulseLang highlighting)
- [x] Time-series chart (Lightweight Charts, line mode)
- [x] Scalar KPI card
- [x] Data table (TanStack Table with basic features)
- [x] Auto-visualization selection based on result type
- [x] Dark theme with trading-platform aesthetic
- [x] Top bar with connection status

### Phase 2 — Multi-Panel Dashboard

**Goal:** Draggable, resizable panel grid. Multiple simultaneous queries.

- [x] react-grid-layout integration
- [x] Panel frame component (title bar, controls, close)
- [x] Add/remove panels
- [x] Panel-specific query editors
- [ ] Dashboard save/load (localStorage)
- [ ] Preset layouts (single, quad, dashboard)
- [ ] Chart type selector dropdown per panel
- [x] Multi-series overlay on time-series charts

### Phase 3 — Real-Time & Polish

**Goal:** Live data updates, rich interactions, production-ready.

- [x] Auto-refresh polling with configurable intervals
- [ ] Adaptive polling (slow down when data unchanged)
- [x] Value change animations (glow, morph, flash)
- [ ] Query autocomplete (builtins, measurements, fields)
- [x] `/measurements`, `/fields`, `/tags` endpoints on server
- [ ] Query history (↑/↓ in editor)
- [ ] Inline error display with line/column highlighting
- [ ] Keyboard shortcuts
- [ ] Dashboard import/export (JSON file)
- [ ] Chart snapshot export (PNG)

### Phase 4 — WebSocket Streaming

**Goal:** True real-time push updates, minimal latency.

- [x] WebSocket endpoint on PulseDB server (`/ws`)
- [x] Subscription protocol (subscribe/unsubscribe per panel)
- [x] Server-side query re-evaluation on interval or write trigger
- [x] Delta detection (only push on change)
- [x] Client reconnection with exponential backoff
- [ ] Seamless fallback: WebSocket → polling

### Phase 5 — Advanced Visualizations

**Goal:** Trading-platform-grade visualization toolkit.

- [ ] Candlestick / OHLC charts
- [ ] Histogram charts
- [ ] Heatmap (D3 canvas)
- [ ] Multi-sparkline rows
- [ ] Baseline charts (positive/negative coloring)
- [ ] Configurable color thresholds for scalar cards
- [ ] Shared variables across panels
- [ ] Full-screen panel mode

---

## 14. Performance Targets

| Metric | Target |
|---|---|
| Initial page load | < 1.5s (200KB gzip) |
| Query → chart render | < 200ms (including server round-trip on localhost) |
| Chart render (10K points) | < 50ms |
| Chart render (100K points) | < 200ms |
| Pan/zoom frame rate | 60fps (WebGL) |
| Table scroll (100K rows) | 60fps (virtualized) |
| Polling overhead | < 1% CPU per panel at 1s interval |
| Memory (10 panels, 100K points each) | < 200MB |
| Bundle size (gzipped) | < 400KB total |

---

## 15. Inspiration References

| Platform | Feature to Emulate |
|---|---|
| **Bloomberg Terminal** | Information density, keyboard-driven workflow, dark theme, multi-panel layouts |
| **TradingView** | Chart rendering quality, crosshair UX, time-range selectors, clean dark aesthetic |
| **Grafana** | Dashboard concept, panel system, variable templates, query editor pattern |
| **Datadog** | Real-time update animations, metric cards, smooth streaming |
| **Refinitiv Eikon** | Professional density, multi-chart overlays, financial data presentation |
| **QStudio (kdb+)** | APL/Q query → chart workflow, array-first visualization, power-user focus |

---

*PulseUI v0.1.0 — Designed for speed, built for data density*
