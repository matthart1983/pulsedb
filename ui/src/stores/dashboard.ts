import { create } from 'zustand'
import type { Layout } from 'react-grid-layout'
import type { LangResponse } from '../api/types'

export type PanelType = 'timeseries' | 'scalar' | 'table' | 'editor'

export interface PanelConfig {
  id: string
  type: PanelType
  query: string
  refreshInterval: number
  title: string
  result: LangResponse | null
  loading: boolean
  error: string | null
  live: boolean
}

interface DashboardState {
  panels: PanelConfig[]
  layout: Layout[]
  addPanel: (panel: Omit<PanelConfig, 'result' | 'loading' | 'error' | 'live'>) => void
  removePanel: (id: string) => void
  updatePanel: (id: string, updates: Partial<PanelConfig>) => void
  updateLayout: (layout: Layout[]) => void
  setPanels: (panels: Omit<PanelConfig, 'result' | 'loading' | 'error' | 'live'>[]) => void
}

let nextId = 1

export const useDashboardStore = create<DashboardState>((set) => ({
  panels: [],
  layout: [],

  addPanel(panel) {
    const id = panel.id || `panel-${nextId++}`
    set((s) => ({
      panels: [...s.panels, { ...panel, id, result: null, loading: false, error: null, live: false }],
      layout: [
        ...s.layout,
        { i: id, x: (s.layout.length * 6) % 12, y: Infinity, w: 6, h: 4 },
      ],
    }))
  },

  removePanel(id) {
    set((s) => ({
      panels: s.panels.filter((p) => p.id !== id),
      layout: s.layout.filter((l) => l.i !== id),
    }))
  },

  updatePanel(id, updates) {
    set((s) => ({
      panels: s.panels.map((p) => (p.id === id ? { ...p, ...updates } : p)),
    }))
  },

  updateLayout(layout) {
    set({ layout })
  },

  setPanels(configs) {
    const panels = configs.map((p) => ({
      ...p,
      result: null,
      loading: false,
      error: null,
      live: false,
    }))
    const layout = panels.map((p, i) => ({
      i: p.id,
      x: (i % 2) * 6,
      y: Math.floor(i / 2) * 4,
      w: 6,
      h: 4,
    }))
    set({ panels, layout })
  },
}))
