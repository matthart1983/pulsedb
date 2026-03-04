import { useEffect, useCallback } from 'react'
import { pulseWs } from '../api/websocket'
import { useDashboardStore } from '../stores/dashboard'
import { useConnectionStore } from '../stores/connection'

let initialized = false

/** Initialize the global WebSocket connection and wire handlers to stores. */
export function useWebSocketInit() {
  useEffect(() => {
    if (initialized) return
    initialized = true

    pulseWs.setHandlers({
      onMessage(id, data) {
        useDashboardStore.getState().updatePanel(id, { result: data, loading: false, error: null })
      },
      onError(id, error) {
        useDashboardStore.getState().updatePanel(id, { error, loading: false })
      },
      onStatus(status) {
        useConnectionStore.getState().setWsStatus(status)
      },
    })

    pulseWs.connect()

    return () => {
      pulseWs.disconnect()
      initialized = false
    }
  }, [])
}

/** Subscribe/unsubscribe a panel to live updates via WebSocket. */
export function usePanelSubscription(panelId: string) {
  const panel = useDashboardStore((s) => s.panels.find((p) => p.id === panelId))

  const subscribe = useCallback((query: string, intervalMs: number = 1000) => {
    useDashboardStore.getState().updatePanel(panelId, { live: true, refreshInterval: intervalMs })
    pulseWs.subscribe(panelId, query, intervalMs)
  }, [panelId])

  const unsubscribe = useCallback(() => {
    useDashboardStore.getState().updatePanel(panelId, { live: false })
    pulseWs.unsubscribe(panelId)
  }, [panelId])

  // Clean up subscription when panel unmounts
  useEffect(() => {
    return () => {
      if (panel?.live) {
        pulseWs.unsubscribe(panelId)
      }
    }
  }, [panelId, panel?.live])

  return { subscribe, unsubscribe, isLive: panel?.live ?? false }
}
