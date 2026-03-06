import type { LangResponse } from '../api/types'

interface PythonOutputProps {
  result: LangResponse
}

export function PythonOutput({ result }: PythonOutputProps) {
  if (result.type !== 'python_output') return null
  const output = (result as { output: string[] }).output

  if (output.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-pulse-text-muted text-xs">
        (no output)
      </div>
    )
  }

  return (
    <div className="h-full overflow-auto p-3 font-mono text-xs leading-relaxed">
      {output.map((line, i) => (
        <div key={i} className="text-pulse-text whitespace-pre-wrap">
          {line}
        </div>
      ))}
    </div>
  )
}
