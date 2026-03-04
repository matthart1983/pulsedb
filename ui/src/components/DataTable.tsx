import { useMemo } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table'
import { useState } from 'react'
import { ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react'
import type { LangResponse } from '../api/types'

interface DataTableProps {
  result: LangResponse
}

function formatTimestamp(ns: number): string {
  const ms = ns / 1_000_000
  const d = new Date(ms)
  if (isNaN(d.getTime())) return String(ns)
  return d.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19)
}

function formatCell(value: unknown, col: string): string {
  if (value === null || value === undefined) return '—'
  if (col === 'ts' && typeof value === 'number') return formatTimestamp(value)
  if (typeof value === 'number') {
    if (Number.isInteger(value) && Math.abs(value) < 1e15) return value.toLocaleString()
    return value.toFixed(4)
  }
  return String(value)
}

export function DataTable({ result }: DataTableProps) {
  const [sorting, setSorting] = useState<SortingState>([])

  const { columns, data } = useMemo(() => {
    if (result.type === 'table') {
      const tableResult = result as { columns: string[]; data: Record<string, unknown[]>; row_count: number }
      const cols: ColumnDef<Record<string, unknown>>[] = tableResult.columns.map((col) => ({
        accessorKey: col,
        header: col,
        cell: (info) => (
          <span className="tabular-nums">{formatCell(info.getValue(), col)}</span>
        ),
      }))

      const rows: Record<string, unknown>[] = []
      for (let i = 0; i < tableResult.row_count; i++) {
        const row: Record<string, unknown> = {}
        for (const col of tableResult.columns) {
          row[col] = tableResult.data[col]?.[i] ?? null
        }
        rows.push(row)
      }

      return { columns: cols, data: rows }
    }

    if (result.type === 'dict') {
      const dictResult = result as { entries: Record<string, unknown> }
      const cols: ColumnDef<Record<string, unknown>>[] = [
        { accessorKey: 'key', header: 'Key' },
        { accessorKey: 'value', header: 'Value', cell: (info) => <span className="tabular-nums">{formatCell(info.getValue(), 'value')}</span> },
      ]
      const rows = Object.entries(dictResult.entries).map(([key, value]) => ({
        key,
        value: typeof value === 'object' && value !== null && 'value' in value ? (value as { value: unknown }).value : value,
      }))
      return { columns: cols, data: rows }
    }

    return { columns: [], data: [] }
  }, [result])

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  })

  if (data.length === 0) {
    return <div className="flex items-center justify-center h-full text-pulse-text-muted text-sm">No data</div>
  }

  return (
    <div className="h-full overflow-auto">
      <table className="w-full text-xs">
        <thead className="sticky top-0 bg-pulse-surface z-10">
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id}>
              {hg.headers.map((header) => (
                <th
                  key={header.id}
                  onClick={header.column.getToggleSortingHandler()}
                  className="px-3 py-2 text-left font-medium text-pulse-text-secondary border-b border-pulse-border cursor-pointer hover:text-pulse-text select-none"
                >
                  <div className="flex items-center gap-1">
                    {flexRender(header.column.columnDef.header, header.getContext())}
                    {header.column.getIsSorted() === 'asc' ? (
                      <ArrowUp className="w-3 h-3" />
                    ) : header.column.getIsSorted() === 'desc' ? (
                      <ArrowDown className="w-3 h-3" />
                    ) : (
                      <ArrowUpDown className="w-3 h-3 opacity-30" />
                    )}
                  </div>
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id} className="hover:bg-pulse-elevated/50 border-b border-pulse-border/50">
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id} className="px-3 py-1.5 font-mono text-pulse-text">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="px-3 py-1.5 text-[10px] text-pulse-text-muted border-t border-pulse-border sticky bottom-0 bg-pulse-surface">
        {data.length.toLocaleString()} rows
      </div>
    </div>
  )
}
