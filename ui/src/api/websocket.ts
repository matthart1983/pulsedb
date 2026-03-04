import type { LangResponse } from './types'

type MessageHandler = (id: string, data: LangResponse) => void
type ErrorHandler = (id: string, error: string) => void
type StatusHandler = (status: 'connected' | 'reconnecting' | 'disconnected') => void

export class PulseWebSocket {
  private ws: WebSocket | null = null
  private url: string
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectDelay = 1000
  private maxReconnectDelay = 30000
  private onMessage: MessageHandler | null = null
  private onError: ErrorHandler | null = null
  private onStatus: StatusHandler | null = null
  private pendingSubscriptions = new Map<string, { query: string; interval_ms: number }>()

  constructor(url: string) {
    this.url = url
  }

  setHandlers(opts: { onMessage: MessageHandler; onError: ErrorHandler; onStatus: StatusHandler }) {
    this.onMessage = opts.onMessage
    this.onError = opts.onError
    this.onStatus = opts.onStatus
  }

  connect() {
    if (this.ws?.readyState === WebSocket.OPEN || this.ws?.readyState === WebSocket.CONNECTING) {
      return
    }

    this.ws = new WebSocket(this.url)

    this.ws.onopen = () => {
      this.reconnectDelay = 1000
      this.onStatus?.('connected')
      // Re-subscribe any pending subscriptions (after reconnect)
      for (const [id, sub] of this.pendingSubscriptions) {
        this.sendRaw({ action: 'subscribe', id, query: sub.query, interval_ms: sub.interval_ms })
      }
    }

    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data)
        if (msg.error && msg.id) {
          this.onError?.(msg.id, msg.error)
        } else if (msg.id) {
          // Extract the LangResponse part (everything except id and timestamp)
          const { id, timestamp, ...data } = msg
          this.onMessage?.(id, data as LangResponse)
        }
      } catch {
        // ignore malformed messages
      }
    }

    this.ws.onclose = () => {
      this.onStatus?.(this.pendingSubscriptions.size > 0 ? 'reconnecting' : 'disconnected')
      this.scheduleReconnect()
    }

    this.ws.onerror = () => {
      // onclose will fire after onerror
    }
  }

  private scheduleReconnect() {
    if (this.reconnectTimer) return
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay)
      this.connect()
    }, this.reconnectDelay)
  }

  private sendRaw(msg: Record<string, unknown>) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg))
    }
  }

  subscribe(id: string, query: string, intervalMs: number) {
    this.pendingSubscriptions.set(id, { query, interval_ms: intervalMs })
    this.sendRaw({ action: 'subscribe', id, query, interval_ms: intervalMs })
    // Auto-connect if not connected
    if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
      this.connect()
    }
  }

  unsubscribe(id: string) {
    this.pendingSubscriptions.delete(id)
    this.sendRaw({ action: 'unsubscribe', id })
  }

  disconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.pendingSubscriptions.clear()
    if (this.ws) {
      this.ws.onclose = null // prevent reconnect
      this.ws.close()
      this.ws = null
    }
    this.onStatus?.('disconnected')
  }

  get connected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN
  }
}

// Singleton instance — resolves WS URL from current location or env
function resolveWsUrl(): string {
  const base = import.meta.env.VITE_API_URL
  if (base) {
    return base.replace(/^http/, 'ws') + '/ws'
  }
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${location.host}/ws`
}

export const pulseWs = new PulseWebSocket(resolveWsUrl())
