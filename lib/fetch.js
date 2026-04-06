import fetch from 'node-fetch'
import pRetry from 'p-retry'

const UA = 'AnaCare/2.0 (price transparency research; contact@anacare.ai)'

// ── JSON fetch (small files) ──────────────────────────────────────
export async function fetchJSON(url, { timeout = 60_000, retries = 3 } = {}) {
  return pRetry(async () => {
    const ctrl = new AbortController()
    const t = setTimeout(() => ctrl.abort(), timeout)
    try {
      const r = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': UA } })
      if (!r.ok) throw new Error(`HTTP ${r.status} ${url.slice(0,80)}`)
      return r.json()
    } finally { clearTimeout(t) }
  }, {
    retries,
    minTimeout: 2000,
    maxTimeout: 30000,
    factor: 2,
    onFailedAttempt: e => console.warn(`  ↻ retry ${e.attemptNumber}: ${e.message.slice(0,80)}`),
  })
}

// ── Text fetch (small CSVs) ───────────────────────────────────────
export async function fetchText(url, { timeout = 120_000, retries = 2 } = {}) {
  return pRetry(async () => {
    const ctrl = new AbortController()
    const t = setTimeout(() => ctrl.abort(), timeout)
    try {
      const r = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': UA } })
      if (!r.ok) throw new Error(`HTTP ${r.status} ${url.slice(0,80)}`)
      return r.text()
    } finally { clearTimeout(t) }
  }, { retries, minTimeout: 3000, factor: 2 })
}

// ── Streaming fetch (multi-GB files) ─────────────────────────────
// Returns the raw Response so caller can pipe res.body to a stream parser.
// Does NOT buffer — safe for files of any size.
export async function fetchStream(url, { timeout = 600_000 } = {}) {
  const ctrl = new AbortController()
  // Don't set abort on timeout here — caller controls the stream lifetime
  const r = await fetch(url, {
    signal: ctrl.signal,
    headers: {
      'User-Agent': UA,
      'Accept-Encoding': 'gzip, deflate, br',
    },
    compress: true,
  })
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url.slice(0,80)}`)
  return r
}

// ── Detect file format from URL/content-type ──────────────────────
export function detectFormat(url = '', contentType = '') {
  const u  = url.toLowerCase()
  const ct = contentType.toLowerCase()
  if (ct.includes('json') || u.includes('.json')) return 'json'
  if (ct.includes('csv')  || u.includes('.csv'))  return 'csv'
  if (u.includes('.txt')  || u.includes('.tsv'))  return 'csv'
  if (u.includes('.gz'))  return u.includes('.csv') ? 'csv.gz' : 'json.gz'
  return 'unknown'
}

// ── Head request — get content-length without downloading ─────────
export async function headRequest(url) {
  try {
    const r = await fetch(url, { method: 'HEAD', headers: { 'User-Agent': UA } })
    return {
      ok: r.ok,
      status: r.status,
      contentType: r.headers.get('content-type') || '',
      contentLength: parseInt(r.headers.get('content-length') || '0'),
    }
  } catch { return { ok: false, status: 0, contentType: '', contentLength: 0 } }
}
