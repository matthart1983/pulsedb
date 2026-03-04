import { useEffect } from 'react'
import { Activity, Database, Wifi, WifiOff, Plus } from 'lucide-react'
import { useConnectionStore } from '../stores/connection'
import { useDashboardStore } from '../stores/dashboard'

export function TopBar() {
  const { status, serverInfo, lastPing, connect } = useConnectionStore()
  const addPanel = useDashboardStore((s) => s.addPanel)

  useEffect(() => { connect() }, [connect])

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
      </div>
    </div>
  )
}
