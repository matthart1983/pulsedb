import { useEffect, useRef } from 'react'
import { createChart, type IChartApi, type ISeriesApi, type UTCTimestamp } from 'lightweight-charts'
import type { LangResponse } from '../api/types'

interface TimeSeriesChartProps {
  result: LangResponse
}

const CHART_COLORS = ['#3b82f6', '#06b6d4', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#ef4444', '#6366f1']

export function TimeSeriesChart({ result }: TimeSeriesChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRefs = useRef<ISeriesApi<'Line'>[]>([])
  const disposed = useRef(false)

  useEffect(() => {
    if (!containerRef.current) return
    disposed.current = false

    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: '#111827' },
        textColor: '#94a3b8',
        fontFamily: "'Inter', sans-serif",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: '#1e293b' },
        horzLines: { color: '#1e293b' },
      },
      crosshair: {
        vertLine: { color: '#3b82f6', width: 1, style: 2, labelBackgroundColor: '#3b82f6' },
        horzLine: { color: '#3b82f6', width: 1, style: 2, labelBackgroundColor: '#3b82f6' },
      },
      rightPriceScale: {
        borderColor: '#1e293b',
      },
      timeScale: {
        borderColor: '#1e293b',
        timeVisible: true,
        secondsVisible: false,
      },
      handleScroll: true,
      handleScale: true,
    })

    chartRef.current = chart

    const resizeObserver = new ResizeObserver(() => {
      if (containerRef.current && !disposed.current) {
        chart.applyOptions({
          width: containerRef.current.clientWidth,
          height: containerRef.current.clientHeight,
        })
      }
    })
    resizeObserver.observe(containerRef.current)

    return () => {
      disposed.current = true
      resizeObserver.disconnect()
      chart.remove()
      chartRef.current = null
      seriesRefs.current = []
    }
  }, [])

  useEffect(() => {
    const chart = chartRef.current
    if (!chart || disposed.current) return

    try {
      // Remove old series
      for (const s of seriesRefs.current) {
        try { chart.removeSeries(s) } catch { /* already removed */ }
      }
      seriesRefs.current = []

      if (result.type === 'table') {
        const tableResult = result as { columns: string[]; data: Record<string, number[]> }
        const tsCol = tableResult.data['ts']
        if (!tsCol || tsCol.length === 0) return

        const numericCols = tableResult.columns.filter(
          (c) => c !== 'ts' && Array.isArray(tableResult.data[c]) && tableResult.data[c].length > 0 && typeof tableResult.data[c][0] === 'number'
        )

        numericCols.forEach((col, idx) => {
          const series = chart.addLineSeries({
            color: CHART_COLORS[idx % CHART_COLORS.length],
            lineWidth: 2,
            title: col,
            priceLineVisible: idx === 0,
          })

          // Build data points, convert ns timestamps to seconds, filter invalid
          const points: { time: UTCTimestamp; value: number }[] = []
          const seen = new Set<number>()
          for (let i = 0; i < tsCol.length; i++) {
            const value = tableResult.data[col]?.[i]
            if (typeof value !== 'number' || isNaN(value) || !isFinite(value)) continue
            const time = Math.floor(tsCol[i] / 1_000_000_000)
            if (seen.has(time)) continue // Lightweight Charts requires unique timestamps
            seen.add(time)
            points.push({ time: time as UTCTimestamp, value })
          }

          // Sort by time (required by Lightweight Charts)
          points.sort((a, b) => (a.time as number) - (b.time as number))

          if (points.length > 0) {
            series.setData(points)
          }
          seriesRefs.current.push(series)
        })
      } else if (result.type === 'float[]' || result.type === 'int[]') {
        const vecResult = result as { values: number[] }
        const series = chart.addLineSeries({
          color: CHART_COLORS[0],
          lineWidth: 2,
          priceLineVisible: true,
        })

        const data = vecResult.values
          .map((v, i) => ({ time: i as UTCTimestamp, value: v }))
          .filter((d) => !isNaN(d.value) && isFinite(d.value))

        if (data.length > 0) {
          series.setData(data)
        }
        seriesRefs.current.push(series)
      }

      chart.timeScale().fitContent()
    } catch (e) {
      console.warn('Chart update error:', e)
    }
  }, [result])

  return <div ref={containerRef} className="w-full h-full" />
}
