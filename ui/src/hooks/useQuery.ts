import { useCallback, useEffect, useRef } from 'react'
import { queryLang, queryPython } from '../api/client'
import { useDashboardStore } from '../stores/dashboard'
import type { LangResponse } from '../api/types'

export function usePanelQuery(panelId: string) {
  const panel = useDashboardStore((s) => s.panels.find((p) => p.id === panelId))
  const updatePanel = useDashboardStore((s) => s.updatePanel)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const execute = useCallback(async (query?: string) => {
    const q = query ?? panel?.query
    if (!q) return

    updatePanel(panelId, { loading: true, error: null })
    try {
      const queryFn = panel?.lang === 'python' ? queryPython : queryLang
      const result = await queryFn(q)
      updatePanel(panelId, { result, loading: false, query: q })
    } catch (e) {
      updatePanel(panelId, {
        loading: false,
        error: e instanceof Error ? e.message : 'Query failed',
      })
    }
  }, [panelId, panel?.query, panel?.lang, updatePanel])

  useEffect(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current)
      intervalRef.current = null
    }
    if (panel?.refreshInterval && panel.refreshInterval > 0) {
      intervalRef.current = setInterval(() => execute(), panel.refreshInterval)
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [panel?.refreshInterval, execute])

  return { execute, result: panel?.result ?? null, loading: panel?.loading ?? false, error: panel?.error ?? null }
}

export function detectVizType(result: LangResponse | null): 'timeseries' | 'scalar' | 'table' | 'editor' {
  if (!result) return 'editor'
  switch (result.type) {
    case 'int':
    case 'uint':
    case 'float':
    case 'bool':
      return 'scalar'
    case 'table': {
      const tableResult = result as { columns?: string[] }
      if (tableResult.columns?.includes('ts')) return 'timeseries'
      return 'table'
    }
    case 'int[]':
    case 'float[]':
      return 'timeseries'
    case 'python_output':
      return 'table'
    default:
      return 'table'
  }
}
