import { useCallback } from 'react'
import GridLayout, { type Layout } from 'react-grid-layout'
import 'react-grid-layout/css/styles.css'
import { useDashboardStore } from '../stores/dashboard'
import { Panel } from './Panel'

export function PanelGrid() {
  const panels = useDashboardStore((s) => s.panels)
  const layout = useDashboardStore((s) => s.layout)
  const updateLayout = useDashboardStore((s) => s.updateLayout)

  const onLayoutChange = useCallback((newLayout: Layout[]) => {
    updateLayout(newLayout)
  }, [updateLayout])

  if (panels.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center text-pulse-text-muted">
        <div className="text-center">
          <div className="text-lg mb-2">Welcome to PulseUI</div>
          <div className="text-xs">Click "+ New Panel" to start querying your data</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto p-2">
      <GridLayout
        className="layout"
        layout={layout}
        cols={12}
        rowHeight={80}
        width={window.innerWidth - 16}
        onLayoutChange={onLayoutChange}
        draggableHandle=".drag-handle"
        compactType="vertical"
        margin={[8, 8]}
      >
        {panels.map((panel) => (
          <div key={panel.id}>
            <Panel panelId={panel.id} />
          </div>
        ))}
      </GridLayout>
    </div>
  )
}
