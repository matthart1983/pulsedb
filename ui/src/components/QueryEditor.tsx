import { useEffect, useRef, useCallback, useState } from 'react'
import { EditorView, keymap, placeholder } from '@codemirror/view'
import { EditorState } from '@codemirror/state'
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands'
import { Play, Loader2 } from 'lucide-react'

interface QueryEditorProps {
  value: string
  onChange: (value: string) => void
  onExecute: (query: string) => void
  loading: boolean
  error: string | null
  elapsedNs?: number
}

const darkTheme = EditorView.theme({
  '&': {
    backgroundColor: '#111827',
    color: '#e2e8f0',
    fontSize: '13px',
    fontFamily: "'JetBrains Mono', monospace",
  },
  '.cm-content': {
    caretColor: '#3b82f6',
    padding: '8px 0',
  },
  '.cm-cursor': {
    borderLeftColor: '#3b82f6',
  },
  '.cm-activeLine': {
    backgroundColor: '#1a233220',
  },
  '.cm-gutters': {
    backgroundColor: '#111827',
    borderRight: '1px solid #1e293b',
    color: '#64748b',
  },
  '.cm-selectionBackground': {
    backgroundColor: '#3b82f630 !important',
  },
  '&.cm-focused .cm-selectionBackground': {
    backgroundColor: '#3b82f640 !important',
  },
  '.cm-placeholder': {
    color: '#64748b',
  },
})

export function QueryEditor({ value, onChange, onExecute, loading, error, elapsedNs }: QueryEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const viewRef = useRef<EditorView | null>(null)
  const [localValue, setLocalValue] = useState(value)

  const handleExecute = useCallback(() => {
    onExecute(localValue)
  }, [localValue, onExecute])

  useEffect(() => {
    if (!containerRef.current) return

    const state = EditorState.create({
      doc: value,
      extensions: [
        history(),
        keymap.of([
          ...defaultKeymap,
          ...historyKeymap,
          {
            key: 'Mod-Enter',
            run: (view) => {
              const q = view.state.doc.toString()
              onExecute(q)
              return true
            },
          },
        ]),
        darkTheme,
        placeholder('Enter PulseLang expression... (Cmd+Enter to execute)'),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            const newVal = update.state.doc.toString()
            setLocalValue(newVal)
            onChange(newVal)
          }
        }),
        EditorView.lineWrapping,
      ],
    })

    const view = new EditorView({ state, parent: containerRef.current })
    viewRef.current = view

    return () => view.destroy()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="flex flex-col h-full">
      <div ref={containerRef} className="flex-1 min-h-0 overflow-auto" />
      <div className="flex items-center gap-2 px-3 py-1.5 border-t border-pulse-border bg-pulse-surface">
        <button
          onClick={handleExecute}
          disabled={loading}
          className="flex items-center gap-1.5 text-xs px-2.5 py-1 rounded bg-chart-blue/20 text-chart-blue hover:bg-chart-blue/30 transition-colors disabled:opacity-50"
        >
          {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
          Run
        </button>
        {elapsedNs !== undefined && elapsedNs > 0 && (
          <span className="text-[10px] text-pulse-text-muted tabular-nums">
            ⏱ {elapsedNs < 1_000_000 ? `${(elapsedNs / 1000).toFixed(1)}µs` : `${(elapsedNs / 1_000_000).toFixed(1)}ms`}
          </span>
        )}
        {error && (
          <span className="text-[10px] text-chart-red truncate ml-2">{error}</span>
        )}
      </div>
    </div>
  )
}
