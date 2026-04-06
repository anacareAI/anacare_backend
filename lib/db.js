import 'dotenv/config'
import pg from 'pg'
import { createWriteStream, mkdirSync } from 'fs'
import { join } from 'path'

const { Pool } = pg

// ── Connection pool ───────────────────────────────────────────────
export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000,
  ssl: { rejectUnauthorized: false },
})

pool.on('error', (err) => console.error('[DB pool error]', err.message))

export async function query(sql, params = []) {
  const client = await pool.connect()
  try { return await client.query(sql, params) }
  finally { client.release() }
}

// ── Buffered batch writer ─────────────────────────────────────────
// Accumulates rows and flushes to DB in configurable batches.
// Handles the high-throughput writes needed for payer MRF processing.
export class BatchWriter {
  constructor(tableName, conflictSql, batchSize = parseInt(process.env.DB_BATCH || '200')) {
    this.table       = tableName
    this.conflict    = conflictSql
    this.batchSize   = batchSize
    this.buffer      = []
    this.totalWritten = 0
    this.errors      = 0
  }

  push(row) {
    this.buffer.push(row)
    if (this.buffer.length >= this.batchSize) return this.flush()
    return Promise.resolve()
  }

  async flush() {
    if (!this.buffer.length) return
    const rows = this.buffer.splice(0)
    const client = await pool.connect()
    try {
      const keys  = Object.keys(rows[0])
      const CHUNK = 50  // Neon free tier: keep param count low
      for (let i = 0; i < rows.length; i += CHUNK) {
        const chunk = rows.slice(i, i + CHUNK)
        const vals  = chunk.flatMap(r => keys.map(k => r[k]))
        const ph    = chunk.map((_, ri) =>
          `(${keys.map((_, ki) => `$${ri * keys.length + ki + 1}`).join(',')})`
        ).join(',')
        await client.query(
          `INSERT INTO ${this.table} (${keys.join(',')}) VALUES ${ph} ${this.conflict}`,
          vals
        )
        this.totalWritten += chunk.length
      }
    } catch (e) {
      this.errors++
      // Don't crash pipeline on individual batch failure
      console.error(`[BatchWriter:${this.table}] batch error: ${e.message.slice(0,120)}`)
    } finally {
      client.release()
    }
  }

  async done() {
    await this.flush()
    return { written: this.totalWritten, errors: this.errors }
  }
}

// ── Logger ────────────────────────────────────────────────────────
const LOG_DIR = process.env.LOG_DIR || './logs'
let _stream = null

export function initLog(name) {
  try { mkdirSync(LOG_DIR, { recursive: true }) } catch {}
  const file = join(LOG_DIR, `${name}_${new Date().toISOString().slice(0,10)}.log`)
  _stream = createWriteStream(file, { flags: 'a' })
  return file
}

function _log(level, msg, data) {
  const line = `[${new Date().toISOString()}] [${level}] ${msg}${data ? ' ' + JSON.stringify(data) : ''}`
  console.log(line)
  _stream?.write(line + '\n')
}

export const log = {
  info:  (m, d) => _log('INFO',  m, d),
  ok:    (m, d) => _log('OK',    m, d),
  warn:  (m, d) => _log('WARN',  m, d),
  error: (m, d) => _log('ERROR', m, d),
}

// ── Progress tracker ─────────────────────────────────────────────
export class Progress {
  constructor(total, label = '') {
    this.total   = total
    this.done    = 0
    this.ok      = 0
    this.errors  = 0
    this.skipped = 0
    this.label   = label
    this.start   = Date.now()
  }
  tick(status = 'ok') {
    this.done++
    if (status === 'ok')      this.ok++
    else if (status === 'error')   this.errors++
    else if (status === 'skipped') this.skipped++
    if (this.done % 100 === 0 || this.done === this.total) this.print()
  }
  print() {
    const pct  = this.total ? ((this.done / this.total) * 100).toFixed(1) : '?'
    const elpsd = Math.round((Date.now() - this.start) / 1000)
    const eta  = this.done > 0
      ? Math.round((elpsd / this.done) * (this.total - this.done))
      : '?'
    log.info(
      `${this.label} [${this.done}/${this.total}] ${pct}% ` +
      `✅${this.ok} ❌${this.errors} ⏭${this.skipped} ` +
      `elapsed:${elpsd}s eta:${eta}s`
    )
  }
}
