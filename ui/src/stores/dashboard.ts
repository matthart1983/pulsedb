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
}

interface DashboardState {
  panels: PanelConfig[]
  layout: Layout[]
  addPanel: (panel: Omit<PanelConfig, 'result' | 'loading' | 'error'>) => void
  removePanel: (id: string) => void
  updatePanel: (id: string, updates: Partial<PanelConfig>) => void
  updateLayout: (layout: Layout[]) => void
}

let nextId = 1

export const useDashboardStore = create<DashboardState>((set) => ({
  panels: [],
  layout: [],

  addPanel(panel) {
    const id = panel.id || `panel-${nextId++}`
    set((s) => ({
      panels: [...s.panels, { ...panel, id, result: null, loading: false, error: null }],
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
}))
