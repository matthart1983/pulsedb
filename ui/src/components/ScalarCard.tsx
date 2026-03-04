import { useRef, useEffect, useState } from 'react'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'
import type { LangResponse } from '../api/types'

interface ScalarCardProps {
  result: LangResponse
  title?: string
}

export function ScalarCard({ result, title }: ScalarCardProps) {
  const prevValue = useRef<number | null>(null)
  const [delta, setDelta] = useState<number | null>(null)
  const [flash, setFlash] = useState(false)

  const value = 'value' in result ? result.value : null
  const numValue = typeof value === 'number' ? value : typeof value === 'boolean' ? (value ? 1 : 0) : null

  useEffect(() => {
    if (numValue !== null && prevValue.current !== null && numValue !== prevValue.current) {
      setDelta(numValue - prevValue.current)
      setFlash(true)
      const timer = setTimeout(() => setFlash(false), 300)
      return () => clearTimeout(timer)
    }
    prevValue.current = numValue
  }, [numValue])

  const deltaColor = delta !== null ? (delta > 0 ? 'text-chart-green' : delta < 0 ? 'text-chart-red' : 'text-pulse-text-muted') : ''
  const DeltaIcon = delta !== null ? (delta > 0 ? TrendingUp : delta < 0 ? TrendingDown : Minus) : null

  const formatValue = (v: unknown) => {
    if (typeof v === 'number') {
      return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(4)
    }
    if (typeof v === 'boolean') return v ? 'true' : 'false'
    if (v === null) return 'null'
    return String(v)
  }

  return (
    <div className={`flex flex-col items-center justify-center h-full p-4 transition-shadow duration-300 ${flash ? 'shadow-[0_0_20px_rgba(59,130,246,0.3)]' : ''}`}>
      {title && (
        <div className="text-xs text-pulse-text-secondary mb-2 uppercase tracking-wider">{title}</div>
      )}
      <div className="text-3xl font-mono font-semibold tabular-nums">
        {formatValue(value)}
      </div>
      {delta !== null && DeltaIcon && (
        <div className={`flex items-center gap-1 mt-1.5 text-sm ${deltaColor}`}>
          <DeltaIcon className="w-3.5 h-3.5" />
          <span className="tabular-nums">{delta > 0 ? '+' : ''}{delta.toFixed(4)}</span>
        </div>
      )}
      <div className="text-[10px] text-pulse-text-muted mt-2">
        {result.type} · {('elapsed_ns' in result ? `${((result.elapsed_ns as number) / 1000).toFixed(1)}µs` : '')}
      </div>
    </div>
  )
}
