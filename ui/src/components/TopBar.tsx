import { useEffect } from 'react'
import { Activity, Database, Wifi, WifiOff, Plus, Radio, Zap } from 'lucide-react'
import { useConnectionStore } from '../stores/connection'
import { useDashboardStore } from '../stores/dashboard'
import { useWebSocketInit } from '../hooks/useWebSocket'
import { pulseWs } from '../api/websocket'

export function TopBar() {
  const { status, wsStatus, serverInfo, lastPing, connect } = useConnectionStore()
  const addPanel = useDashboardStore((s) => s.addPanel)

  useEffect(() => { connect() }, [connect])
  useWebSocketInit()

  const loadDemo = () => {
    // Unsubscribe any existing live panels
    const { panels: oldPanels } = useDashboardStore.getState()
    oldPanels.forEach((p) => pulseWs.unsubscribe(p.id))

    const ts = Date.now()
    const demoConfigs = [
      // --- PulseLang panels ---
      {
        id: `demo-btc-${ts}`,
        type: 'editor' as const,
        title: 'BTC Price',
        query: 'last crypto.price @ `symbol = `BTC',
        refreshInterval: 1000,
      },
      {
        id: `demo-btc-chart-${ts}`,
        type: 'editor' as const,
        title: 'BTC Chart',
        query: 'crypto @ `symbol = `BTC',
        refreshInterval: 1000,
      },
      {
        id: `demo-eth-${ts}`,
        type: 'editor' as const,
        title: 'ETH Price',
        query: 'last crypto.price @ `symbol = `ETH',
        refreshInterval: 1000,
      },
      {
        id: `demo-market-${ts}`,
        type: 'editor' as const,
        title: 'Market Overview',
        query: 'market',
        refreshInterval: 2000,
      },
      {
        id: `demo-ema-${ts}`,
        type: 'editor' as const,
        title: 'BTC EMA Pipeline',
        query: 'crypto.price @ `symbol = `BTC |> {ema[0.1; x]}',
        refreshInterval: 2000,
      },
      {
        id: `demo-spread-${ts}`,
        type: 'editor' as const,
        title: 'BTC Spread',
        query: '(max crypto.price @ `symbol = `BTC) - (min crypto.price @ `symbol = `BTC)',
        refreshInterval: 2000,
      },
      // --- Python panels ---
      {
        id: `demo-py-overview-${ts}`,
        type: 'editor' as const,
        title: '🐍 DB Overview',
        query: 'measurements = db_measurements()\nfor m in measurements:\n    fields = db_fields(m)\n    print(m + ": " + str(fields))',
        refreshInterval: 0,
        lang: 'python' as const,
      },
      {
        id: `demo-py-alert-${ts}`,
        type: 'editor' as const,
        title: '🐍 Price Alerts',
        query: 'prices = db_query("crypto.price @ `symbol = `BTC")\nhigh = 0.0\nlow = 999999.0\nfor p in prices:\n    if p > high:\n        high = p\n    if p < low:\n        low = p\nspread = high - low\nprint("BTC High: " + str(high))\nprint("BTC Low:  " + str(low))\nprint("Spread:   " + str(spread))\nif spread > 100.0:\n    print("ALERT: High volatility!")',
        refreshInterval: 0,
        lang: 'python' as const,
      },
    ]

    // Atomic state replace — avoids intermediate empty state
    useDashboardStore.getState().setPanels(demoConfigs)

    // Subscribe live PulseLang panels via WebSocket after panels are mounted
    setTimeout(() => {
      demoConfigs.forEach((cfg) => {
        if (cfg.refreshInterval > 0 && (!('lang' in cfg) || cfg.lang !== 'python')) {
          useDashboardStore.getState().updatePanel(cfg.id, { live: true })
          pulseWs.subscribe(cfg.id, cfg.query, cfg.refreshInterval)
        }
      })
    }, 500)
  }

  const statusColor = status === 'connected' ? 'text-chart-green' : status === 'reconnecting' ? 'text-chart-amber' : 'text-chart-red'
  const StatusIcon = status === 'connected' ? Wifi : WifiOff

  return (
    <div className="h-10 bg-pulse-surface border-b border-pulse-border flex items-center px-4 gap-4 shrink-0">
      <div className="flex items-center gap-2">
        <Activity className="w-4 h-4 text-chart-blue" />
        <span className="font-semibold text-sm">PulseDB</span>
      </div>

      <div className="h-5 w-px bg-pulse-border" />

      <button
        onClick={() => addPanel({
          id: `panel-${Date.now()}`,
          type: 'editor',
          query: '',
          refreshInterval: 0,
          title: 'New Panel',
        })}
        className="flex items-center gap-1.5 text-xs text-pulse-text-secondary hover:text-pulse-text transition-colors px-2 py-1 rounded hover:bg-pulse-elevated"
      >
        <Plus className="w-3.5 h-3.5" />
        New Panel
      </button>

      <button
        onClick={loadDemo}
        className="flex items-center gap-1.5 text-xs text-chart-amber hover:text-chart-amber/80 transition-colors px-2 py-1 rounded hover:bg-pulse-elevated"
      >
        <Zap className="w-3.5 h-3.5" />
        Demo
      </button>

      <div className="flex-1" />

      <div className="flex items-center gap-3 text-xs text-pulse-text-secondary">
        {serverInfo && (
          <>
            <div className="flex items-center gap-1.5">
              <Database className="w-3.5 h-3.5" />
              <span>{serverInfo.series_count} series</span>
            </div>
            <div className="h-4 w-px bg-pulse-border" />
          </>
        )}
        <div className={`flex items-center gap-1.5 ${statusColor}`}>
          <StatusIcon className="w-3.5 h-3.5" />
          <span className="capitalize">{status}</span>
          {lastPing !== null && <span className="text-pulse-text-muted">({lastPing}ms)</span>}
        </div>
        <div className="h-4 w-px bg-pulse-border" />
        <div className={`flex items-center gap-1.5 ${wsStatus === 'connected' ? 'text-chart-green' : wsStatus === 'reconnecting' ? 'text-chart-amber' : 'text-pulse-text-muted'}`}>
          <Radio className="w-3.5 h-3.5" />
          <span>WS</span>
        </div>
      </div>
    </div>
  )
}
