import { useState, useCallback } from 'react'
import { X, RefreshCw, Maximize2, Minimize2 } from 'lucide-react'
import { useDashboardStore } from '../stores/dashboard'
import { usePanelQuery, detectVizType } from '../hooks/useQuery'
import { QueryEditor } from './QueryEditor'
import { TimeSeriesChart } from './TimeSeriesChart'
import { ScalarCard } from './ScalarCard'
import { DataTable } from './DataTable'

interface PanelProps {
  panelId: string
}

export function Panel({ panelId }: PanelProps) {
  const panel = useDashboardStore((s) => s.panels.find((p) => p.id === panelId))
  const updatePanel = useDashboardStore((s) => s.updatePanel)
  const removePanel = useDashboardStore((s) => s.removePanel)
  const { execute, result, loading, error } = usePanelQuery(panelId)
  const [expanded, setExpanded] = useState(false)

  const handleExecute = useCallback((query: string) => {
    execute(query)
  }, [execute])

  const handleQueryChange = useCallback((query: string) => {
    updatePanel(panelId, { query })
  }, [panelId, updatePanel])

  if (!panel) return null

  const vizType = detectVizType(result)
  const elapsedNs = result && 'elapsed_ns' in result ? (result.elapsed_ns as number) : undefined

  return (
    <div className="flex flex-col h-full bg-pulse-surface border border-pulse-border rounded-lg overflow-hidden">
      {/* Title bar */}
      <div className="flex items-center h-8 px-3 bg-pulse-elevated/50 border-b border-pulse-border shrink-0 cursor-move drag-handle">
        <span className="text-xs font-medium text-pulse-text-secondary truncate flex-1">
          {panel.title || 'Untitled'}
        </span>
        <div className="flex items-center gap-1">
          <button
            onClick={() => execute()}
            className="p-1 rounded hover:bg-pulse-overlay text-pulse-text-muted hover:text-pulse-text transition-colors"
            title="Refresh"
          >
            <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => setExpanded(!expanded)}
            className="p-1 rounded hover:bg-pulse-overlay text-pulse-text-muted hover:text-pulse-text transition-colors"
          >
            {expanded ? <Minimize2 className="w-3 h-3" /> : <Maximize2 className="w-3 h-3" />}
          </button>
          <button
            onClick={() => removePanel(panelId)}
            className="p-1 rounded hover:bg-chart-red/20 text-pulse-text-muted hover:text-chart-red transition-colors"
          >
            <X className="w-3 h-3" />
          </button>
        </div>
      </div>

      {/* Content area */}
      <div className="flex-1 min-h-0 flex flex-col">
        {/* Visualization */}
        <div className="flex-1 min-h-0">
          {result ? (
            vizType === 'timeseries' ? (
              <TimeSeriesChart result={result} />
            ) : vizType === 'scalar' ? (
              <ScalarCard result={result} title={panel.title} />
            ) : (
              <DataTable result={result} />
            )
          ) : (
            <div className="flex items-center justify-center h-full text-pulse-text-muted text-xs">
              Enter a query and press Cmd+Enter
            </div>
          )}
        </div>

        {/* Query editor */}
        <div className="h-24 shrink-0 border-t border-pulse-border">
          <QueryEditor
            value={panel.query}
            onChange={handleQueryChange}
            onExecute={handleExecute}
            loading={loading}
            error={error}
            elapsedNs={elapsedNs}
          />
        </div>
      </div>
    </div>
  )
}
