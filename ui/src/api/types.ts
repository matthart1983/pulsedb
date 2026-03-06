export type LangResultType =
  | 'int' | 'uint' | 'float' | 'bool' | 'str' | 'sym'
  | 'ts' | 'dur' | 'null'
  | 'int[]' | 'float[]' | 'bool[]' | 'sym[]' | 'str[]' | 'ts[]'
  | 'list' | 'dict' | 'table' | 'fn'

export interface ScalarResult {
  type: 'int' | 'uint' | 'float' | 'bool' | 'str' | 'sym' | 'ts' | 'dur' | 'null'
  value: number | boolean | string | null
  elapsed_ns: number
}

export interface VectorResult {
  type: 'int[]' | 'float[]' | 'bool[]' | 'sym[]' | 'str[]' | 'ts[]'
  values: (number | boolean | string)[]
  elapsed_ns: number
}

export interface TableResult {
  type: 'table'
  columns: string[]
  data: Record<string, (number | boolean | string)[]>
  row_count: number
  elapsed_ns: number
}

export interface DictResult {
  type: 'dict'
  entries: Record<string, LangResponse>
  elapsed_ns: number
}

export interface PythonResult {
  type: 'python_output'
  output: string[]
  elapsed_ns: number
}

export type LangResponse = ScalarResult | VectorResult | TableResult | DictResult | PythonResult | {
  type: string
  elapsed_ns: number
  [key: string]: unknown
}

export interface StatusResponse {
  version: string
  series_count: number
  points_in_memtable: number
  segment_count: number
  measurements: string[]
}

export interface MeasurementsResponse {
  measurements: string[]
}

export interface FieldsResponse {
  measurement: string
  fields: string[]
}

export interface ErrorResponse {
  error: string
}
