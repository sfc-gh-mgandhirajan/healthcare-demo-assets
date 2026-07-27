"use client"

import { useQuery } from "@tanstack/react-query"

export interface PanelResult {
  rows: Record<string, any>[]
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url)
  const body = await res.json()
  if (!res.ok) throw new Error(body?.error ?? `Request failed (${res.status})`)
  return body as T
}

/** Fetch a whitelisted panel's live rows from /api/panel/[key]. */
export function usePanel(key: string) {
  return useQuery({
    queryKey: ["panel", key],
    queryFn: () => fetchJson<PanelResult>(`/api/panel/${key}`),
    staleTime: 5 * 60 * 1000,
  })
}

export function useKpi() {
  return useQuery({
    queryKey: ["kpi"],
    queryFn: () => fetchJson<Record<string, number | null>>("/api/kpi"),
    staleTime: 5 * 60 * 1000,
  })
}

export interface Insight {
  tabOrder: number
  tabKey: string
  tabTitle: string
  persona: string
  md: string
  tags: string[]
  generatedAt: string | null
}

export function useInsights() {
  return useQuery({
    queryKey: ["insights"],
    queryFn: () => fetchJson<{ insights: Insight[] }>("/api/insights"),
    staleTime: 5 * 60 * 1000,
  })
}
