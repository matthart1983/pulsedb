import type { LangResponse, StatusResponse, MeasurementsResponse, FieldsResponse } from './types'

const BASE_URL = import.meta.env.VITE_API_URL || ''

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }
  return res.json()
}

export async function queryLang(q: string): Promise<LangResponse> {
  return request<LangResponse>('/api/lang', {
    method: 'POST',
    body: JSON.stringify({ q }),
  })
}

export async function getStatus(): Promise<StatusResponse> {
  return request<StatusResponse>('/api/status')
}

export async function getMeasurements(): Promise<MeasurementsResponse> {
  return request<MeasurementsResponse>('/api/measurements')
}

export async function getFields(measurement: string): Promise<FieldsResponse> {
  return request<FieldsResponse>(`/api/fields?measurement=${encodeURIComponent(measurement)}`)
}
