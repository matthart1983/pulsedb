import { create } from 'zustand'
import type { StatusResponse } from '../api/types'
import { getStatus } from '../api/client'

interface ConnectionState {
  status: 'connected' | 'reconnecting' | 'disconnected'
  wsStatus: 'connected' | 'reconnecting' | 'disconnected'
  serverInfo: StatusResponse | null
  lastPing: number | null
  error: string | null
  connect: () => void
  poll: () => Promise<void>
  setWsStatus: (status: 'connected' | 'reconnecting' | 'disconnected') => void
}

export const useConnectionStore = create<ConnectionState>((set, get) => ({
  status: 'disconnected',
  wsStatus: 'disconnected',
  serverInfo: null,
  lastPing: null,
  error: null,

  connect() {
    get().poll()
    setInterval(() => get().poll(), 10_000)
  },

  async poll() {
    try {
      const start = performance.now()
      const info = await getStatus()
      const ping = Math.round(performance.now() - start)
      set({ status: 'connected', serverInfo: info, lastPing: ping, error: null })
    } catch (e) {
      set((s) => ({
        status: s.status === 'connected' ? 'reconnecting' : 'disconnected',
        error: e instanceof Error ? e.message : 'Connection failed',
      }))
    }
  },

  setWsStatus(wsStatus) {
    set({ wsStatus })
  },
}))
