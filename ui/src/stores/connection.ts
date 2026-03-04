import { create } from 'zustand'
import type { StatusResponse } from '../api/types'
import { getStatus } from '../api/client'

interface ConnectionState {
  status: 'connected' | 'reconnecting' | 'disconnected'
  serverInfo: StatusResponse | null
  lastPing: number | null
  error: string | null
  connect: () => void
  poll: () => Promise<void>
}

export const useConnectionStore = create<ConnectionState>((set, get) => ({
  status: 'disconnected',
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
}))
