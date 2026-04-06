#!/usr/bin/env node
/**
 * 05_layer2_payer_mrfs.js
 *
 * Fetches negotiated rates for NON-HOSPITAL providers only
 * (ASCs, urgent cares, imaging centers, standalone clinics).
 *
 * HOSPITALS are already covered — script 04b extracted ALL payer rates
 * (Aetna, BCBS, Cigna, UHC, Humana, Anthem, etc.) directly from each
 * hospital's own CMS MRF file. No hospital payer scraping needed here.
 *
 * For non-hospital providers this script handles UHC only.
 * UHC's API at transparency-in-coverage.uhc.com is stable and well-documented.
 * BCBS state plans for non-hospital providers are handled in 05b_bcbs_plans.js.
 *
 * Aetna, Cigna, Humana, Anthem rates for non-hospital providers:
 * These payers' MRF files are 500GB-1TB with complex access patterns
 * (expiring signed URLs, JS SPAs, rate-limited multi-step APIs).
 * Non-hospital facilities that have none of the above will be filled
 * with regional median estimates by script 06.
 */
import 'dotenv/config'
import { pool, log, initLog, BatchWriter } from '../lib/db.js'
import { fetchJSON, fetchStream } from '../lib/fetch.js'
import { normalizePayerItem } from '../lib/parser.js'
import pLimit from 'p-limit'

const DB_BATCH   = parseInt(process.env.DB_BATCH          || '200')
const MAX_ROWS   = parseInt(process.env.LAYER2_MAX_ROWS   || '5000000')
const CONCUR     = parseInt(process.env.LAYER2_CONCURRENCY || '3')

// UHC stable API endpoint — verified working as of April 2026
const UHC_INDEX  = 'https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/'

async function loadTargets() {
  const client = await pool.connect()
  try {
    // Only non-hospital providers — hospitals already have payer rates from 04b
    const npis = new Set(
      (await client.query(`SELECT npi FROM providers WHERE type != 'hospital'`))
        .rows.map(r => r.npi)
    )
    // Pass empty set for CPTs — normalizePayerItem no longer filters by CPT
    // procedure_catalog is built dynamically from what hospitals actually publish
    log.info(`Non-hospital targets: ${npis.size} NPIs — accepting all CPT codes`)
    return { npis, cpts: new Set() }
  } finally { client.release() }
}

async function runUHC(targets) {
  log.info('\n=== UHC: Non-hospital provider rates ===')

  let indexData
  try {
    indexData = await fetchJSON(UHC_INDEX, { timeout: 30000 })
  } catch (e) {
    log.error(`UHC index fetch failed: ${e.message}`)
    log.warn(`Manual check: ${UHC_INDEX}`)
    return 0
  }

  const files = (indexData.blobs || [])
    .filter(b => b.name?.toLowerCase().includes('in-network'))
    .slice(0, 50)

  log.info(`UHC: processing ${files.length} in-network files`)

  const writer = new BatchWriter('negotiated_rates', `
    ON CONFLICT (npi, cpt_code, payer_name, COALESCE(plan_name,''))
    WHERE cpt_code IS NOT NULL
    DO UPDATE SET
      negotiated_rate = EXCLUDED.negotiated_rate,
      ingested_at     = NOW()
  `, DB_BATCH)

  let totalRows = 0
  const lim = pLimit(CONCUR)

  const tasks = files.map(file => lim(async () => {
    if (MAX_ROWS > 0 && totalRows >= MAX_ROWS) return
    try {
      const res  = await fetchStream(file.downloadUrl || file.location)
      let buf    = '', inArray = false, depth = 0, item = ''

      await new Promise((resolve, reject) => {
        res.body.on('data', chunk => {
          buf += chunk.toString()

          if (!inArray) {
            const idx = buf.indexOf('"in_network"')
            if (idx >= 0) {
              const a = buf.indexOf('[', idx)
              if (a >= 0) { inArray = true; buf = buf.slice(a + 1) }
            }
            if (!inArray && buf.length > 8192) buf = buf.slice(-4096)
            return
          }

          for (let i = 0; i < buf.length; i++) {
            const ch = buf[i]
            if (ch === '{') { depth++; item += ch }
            else if (ch === '}') {
              depth--; item += ch
              if (depth === 0 && item.length > 10) {
                try {
                  const obj = JSON.parse(item)
                  obj.payer_name = 'United Health Care'
                  const rows = normalizePayerItem(obj, targets.npis, targets.cpts)
                  for (const row of rows) { writer.push(row); totalRows++ }
                } catch {}
                item = ''
              }
            } else if (depth > 0) item += ch
          }
          buf = ''
        })
        res.body.on('end', resolve)
        res.body.on('error', reject)
      })
    } catch (e) {
      log.warn(`UHC file failed: ${e.message.slice(0, 80)}`)
    }
  }))

  await Promise.all(tasks)
  const { written } = await writer.done()
  log.ok(`UHC complete: ${written} rates written for non-hospital providers`)
  return written
}

async function main() {
  initLog('05_layer2')
  log.info('=== Layer 2: Non-hospital provider payer rates ===')
  log.info('Note: Hospital payer rates already extracted in script 04b')

  const targets = await loadTargets()
  if (!targets.npis.size) {
    log.warn('No non-hospital providers found — run 03_ingest_providers.js first')
    await pool.end(); return
  }

  await runUHC(targets)

  // Summary
  const client = await pool.connect()
  try {
    const r = await client.query(`
      SELECT payer_name, COUNT(*) rows, COUNT(DISTINCT npi) providers
      FROM negotiated_rates
      GROUP BY payer_name
      ORDER BY rows DESC
    `)
    log.ok('\nNegotiated rates by payer (all sources):')
    console.table(r.rows)
  } finally {
    client.release()
    await pool.end()
  }
}

main().catch(e => { log.error('Fatal', { message: e.message }); process.exit(1) })
