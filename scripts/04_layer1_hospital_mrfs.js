#!/usr/bin/env node
/**
 * 04_layer1_hospital_mrfs.js — Hospital MRF Price Scraping
 *
 * For each hospital in our DB that has a known MRF URL (stored in providers.url),
 * streams the CMS price transparency file (CSV or JSON) and writes prices to
 * shoppable_services and procedure_catalog.
 *
 * URL population: run enrich_mrf_urls.mjs beforehand to populate providers.url
 * from the TPAFS CSV by CCN matching.
 *
 * File format: CMS v2 CSV wide (mandatory since July 2024)
 *   Row 0: metadata headers (hospital_name, last_updated_on, ...)
 *   Row 1: metadata values
 *   Row 2: price column headers (description, code|1, standard_charge|gross, ...)
 *   Row 3+: price data
 */
import 'dotenv/config'
import { createGunzip } from 'zlib'
import { Readable } from 'stream'
import { parse as csvParse } from 'csv-parse'
import { pool, log, initLog, BatchWriter, Progress } from '../lib/db.js'
import { fetchText, fetchStream, detectFormat } from '../lib/fetch.js'
import { normalizeMRFItem, parseCSVRow } from '../lib/parser.js'
import pLimit from 'p-limit'

const CONCURRENCY = parseInt(process.env.LAYER1_CONCURRENCY || '8')
const LIMIT_N     = parseInt(process.env.LAYER1_LIMIT || '0')
const RESUME      = process.env.RESUME === 'true'
const SKIP_DAYS   = parseInt(process.env.SKIP_SCRAPED_DAYS || '30')

// ── Get hospitals with known MRF URLs ────────────────────────────
async function getFacilities() {
  const client = await pool.connect()
  try {
    let sql = `
      SELECT npi, name, type, url, state, cms_cert_num
      FROM providers
      WHERE type = 'hospital'
        AND url IS NOT NULL
        AND url LIKE 'http%'
    `
    if (RESUME) {
      sql += ` AND (last_scraped_at IS NULL
               OR last_scraped_at < NOW() - INTERVAL '${SKIP_DAYS} days'
               OR last_scrape_status = 'error')`
    }
    sql += ` ORDER BY last_scraped_at ASC NULLS FIRST`
    if (LIMIT_N > 0) sql += ` LIMIT ${LIMIT_N}`
    const r = await client.query(sql)
    return r.rows
  } finally { client.release() }
}

// ── Parse v2 CMS wide CSV (mandatory format since July 2024) ─────
// Row 0: metadata column names
// Row 1: metadata values
// Row 2: actual price column headers
// Row 3+: price data rows
async function streamCSV(url, npi, writer) {
  const text = await fetchText(url, { timeout: 300000 })
  if (!text || text.length < 100) throw new Error('Empty or too-small response')

  // Detect v2 by first line containing metadata keys
  const firstLine = text.slice(0, 300).split('\n')[0] || ''
  const isV2 = firstLine.includes('hospital_name') || firstLine.includes('last_updated_on')

  let rowNum = 0
  let headers = null
  let count = 0

  await new Promise((resolve, reject) => {
    const parser = csvParse({
      relaxColumnCount: true,
      skipEmptyLines: true,
      trim: true,
      bom: true,
    })

    parser.on('readable', () => {
      let record
      while ((record = parser.read()) !== null) {
        rowNum++

        if (isV2) {
          if (rowNum <= 2) continue           // skip metadata rows 1 and 2
          if (rowNum === 3) {                  // row 3 = real column headers
            headers = record.map(h => (h || '').trim())
            log.info(`  v2 CSV headers: ${headers.slice(0,5).join(' | ')} ... (${headers.length} cols)`)
            continue
          }
        } else {
          if (!headers) {                      // v1: row 1 = column headers
            headers = record.map(h => (h || '').trim())
            log.info(`  v1 CSV headers: ${headers.slice(0,5).join(' | ')} ... (${headers.length} cols)`)
            continue
          }
        }

        if (!headers) continue
        const row = Object.fromEntries(headers.map((h, i) => [h, record[i]]))
        const parsed = parseCSVRow(row, headers, npi, url)
        if (parsed) { writer.push(parsed); count++ }
      }
    })
    parser.on('error', reject)
    parser.on('end', resolve)
    Readable.from([text]).pipe(parser)
  })

  return count
}

// ── Parse JSON MRF ───────────────────────────────────────────────
async function streamJSON(url, npi, writer) {
  const res = await fetchStream(url)
  let count = 0
  let buffer = ''
  let inArray = false
  let depth = 0
  let item = ''
  let effectiveDate = null

  let source = res.body
  const ct = res.headers.get('content-type') || ''
  if (ct.includes('gzip') || url.endsWith('.gz')) source = res.body.pipe(createGunzip())

  await new Promise((resolve, reject) => {
    source.on('data', chunk => {
      buffer += chunk.toString()

      if (!effectiveDate) {
        const m = buffer.match(/"last_updated_on"\s*:\s*"([^"]+)"/)
        if (m) effectiveDate = m[1]
      }

      if (!inArray) {
        const idx = buffer.indexOf('"standard_charge_information"')
        if (idx >= 0) {
          const a = buffer.indexOf('[', idx)
          if (a >= 0) { inArray = true; buffer = buffer.slice(a + 1) }
        }
        if (!inArray) {
          const idx2 = buffer.indexOf('"items"')
          if (idx2 >= 0) {
            const a = buffer.indexOf('[', idx2)
            if (a >= 0) { inArray = true; buffer = buffer.slice(a + 1) }
          }
        }
        if (!inArray && buffer.length > 4096) buffer = buffer.slice(-2048)
        return
      }

      for (let i = 0; i < buffer.length; i++) {
        const ch = buffer[i]
        if (ch === '{') { depth++; item += ch }
        else if (ch === '}') {
          depth--; item += ch
          if (depth === 0 && item.trim().length > 2) {
            try {
              const obj = JSON.parse(item)
              const rows = normalizeMRFItem(obj, npi, url)
              for (const row of rows) {
                if (effectiveDate) row.effective_date = effectiveDate
                writer.push(row)
                count++
              }
            } catch {}
            item = ''
          }
        } else if (depth > 0) item += ch
      }
      buffer = ''
    })
    source.on('end', resolve)
    source.on('error', reject)
  })

  return count
}

// ── Mark outcomes ────────────────────────────────────────────────
async function markDone(npi, url, rows, written, fmt, start) {
  const client = await pool.connect()
  try {
    const score = written > 0 ? Math.min(1, written / 300) : 0
    await client.query(`
      UPDATE providers SET
        data_score = $1, service_count = $2,
        has_transparency_data = true,
        last_scraped_at = NOW(), last_scrape_status = 'ok', last_scrape_error = NULL
      WHERE npi = $3
    `, [score, written, npi])
    await client.query(`
      INSERT INTO scrape_log
        (npi, job_type, status, rows_parsed, rows_inserted, file_url, source_file_url, file_format, duration_ms, completed_at)
      VALUES ($1,'hospital_mrf','ok',$2,$3,$4,$4,$5,$6,NOW())
    `, [npi, rows, written, url, fmt, Date.now() - start])
  } finally { client.release() }
}

async function markError(npi, url, msg, start) {
  const client = await pool.connect()
  try {
    await client.query(`
      UPDATE providers SET last_scraped_at = NOW(), last_scrape_status = 'error', last_scrape_error = $1 WHERE npi = $2
    `, [msg.slice(0, 500), npi])
    await client.query(`
      INSERT INTO scrape_log (npi, job_type, status, file_url, source_file_url, error_msg, duration_ms, completed_at)
      VALUES ($1,'hospital_mrf','error',$2,$2,$3,$4,NOW())
    `, [npi, url, msg.slice(0, 500), Date.now() - start])
  } finally { client.release() }
}

// ── Scrape one hospital ──────────────────────────────────────────
// Uses in-memory deduplication: aggregate all rows by CPT code before
// writing to DB. This avoids ON CONFLICT duplicate-in-batch errors and
// reduces DB writes from millions to thousands per hospital.
async function scrapeOne(facility) {
  const { npi, name, url } = facility
  const start = Date.now()

  // In-memory maps — deduplicated before writing
  const ssMap = new Map()  // cpt_code → aggregated prices
  const nrMap = new Map()  // cpt_code|payer_name → best negotiated rate

  try {
    // Parse the MRF file into memory maps
    const fmt = detectFormat(url)
    let totalParsed = 0

    const processRow = (parsed) => {
      if (!parsed) return
      totalParsed++
      const code = parsed.cpt_code || parsed.hcpcs_code
      if (!code) return

      // Always update ssMap with cash/gross/min/max
      const ex = ssMap.get(code) || {
        npi, cpt_code: parsed.cpt_code, hcpcs_code: parsed.hcpcs_code,
        description: (parsed.description || 'Unknown').slice(0, 500),
        cash_price: null, chargemaster_price: null,
        min_negotiated: null, max_negotiated: null,
        source_file_url: url,
      }
      if (parsed.cash_price && !ex.cash_price) ex.cash_price = parsed.cash_price
      if (parsed.chargemaster_price && !ex.chargemaster_price) ex.chargemaster_price = parsed.chargemaster_price
      if (parsed.min_negotiated) {
        ex.min_negotiated = ex.min_negotiated
          ? Math.min(ex.min_negotiated, parsed.min_negotiated)
          : parsed.min_negotiated
      }
      if (parsed.max_negotiated) {
        ex.max_negotiated = ex.max_negotiated
          ? Math.max(ex.max_negotiated, parsed.max_negotiated)
          : parsed.max_negotiated
      }
      ssMap.set(code, ex)

      // Payer-specific rate → nrMap
      if (parsed.payer_name && parsed.min_negotiated) {
        const key = `${code}|${parsed.payer_name}`
        if (!nrMap.has(key) || parsed.min_negotiated < nrMap.get(key).negotiated_rate) {
          nrMap.set(key, {
            npi, cpt_code: parsed.cpt_code, hcpcs_code: parsed.hcpcs_code,
            description: (parsed.description || 'Unknown').slice(0, 500),
            negotiated_rate: parsed.min_negotiated,
            payer_name: parsed.payer_name.slice(0, 100),
            plan_name: parsed.plan_name?.slice(0, 100) || null,
            source_file_url: url,
          })
        }
      }
    }

    if (fmt === 'csv' || fmt === 'csv.gz') {
      await streamCSVToMaps(url, npi, processRow)
    } else {
      try {
        await streamJSONToMaps(url, npi, processRow)
        if (ssMap.size === 0) await streamCSVToMaps(url, npi, processRow)
      } catch (e) {
        log.warn(`  JSON failed for ${name}, trying CSV: ${e.message.slice(0,60)}`)
        await streamCSVToMaps(url, npi, processRow)
      }
    }

    log.info(`  ${name}: ${totalParsed} parsed → ${ssMap.size} unique CPTs, ${nrMap.size} payer rates`)

    // Write shoppable_services in batches of 500
    const ssRows = [...ssMap.values()]
    let ssWritten = 0
    const ssClient = await pool.connect()
    try {
      for (let i = 0; i < ssRows.length; i += 500) {
        const batch = ssRows.slice(i, i + 500)
        for (const row of batch) {
          try {
            await ssClient.query(`
              INSERT INTO shoppable_services
                (npi, cpt_code, hcpcs_code, description, cash_price, chargemaster_price,
                 min_negotiated, max_negotiated, source_file_url)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
              ON CONFLICT (npi, cpt_code) WHERE cpt_code IS NOT NULL
              DO UPDATE SET
                cash_price         = COALESCE(EXCLUDED.cash_price, shoppable_services.cash_price),
                chargemaster_price = COALESCE(EXCLUDED.chargemaster_price, shoppable_services.chargemaster_price),
                min_negotiated     = LEAST(COALESCE(EXCLUDED.min_negotiated, shoppable_services.min_negotiated), COALESCE(shoppable_services.min_negotiated, EXCLUDED.min_negotiated)),
                max_negotiated     = GREATEST(COALESCE(EXCLUDED.max_negotiated, shoppable_services.max_negotiated), COALESCE(shoppable_services.max_negotiated, EXCLUDED.max_negotiated)),
                scraped_at         = NOW()
            `, [row.npi, row.cpt_code, row.hcpcs_code, row.description,
                row.cash_price, row.chargemaster_price, row.min_negotiated,
                row.max_negotiated, row.source_file_url])
            ssWritten++
          } catch(e) { /* skip duplicate errors */ }
        }
      }
    } finally { ssClient.release() }

    // Write negotiated_rates in batches of 500
    const nrRows = [...nrMap.values()]
    let nrWritten = 0
    for (let i = 0; i < nrRows.length; i += 500) {
      const batch = nrRows.slice(i, i + 500)
      const nrClient = await pool.connect()
      try {
        const keys = ['npi','cpt_code','hcpcs_code','description','negotiated_rate','payer_name','plan_name','source_file_url']
        const vals = batch.flatMap(r => keys.map(k => r[k] ?? null))
        const ph = batch.map((_, ri) =>
          `(${keys.map((_, ki) => `$${ri * keys.length + ki + 1}`).join(',')})`
        ).join(',')
        await nrClient.query(`
          INSERT INTO negotiated_rates (${keys.join(',')}) VALUES ${ph}
          ON CONFLICT (npi, cpt_code, payer_name) WHERE cpt_code IS NOT NULL
          DO UPDATE SET negotiated_rate = EXCLUDED.negotiated_rate, ingested_at = NOW()
        `, vals)
        nrWritten += batch.length
      } catch(e) {
        log.warn(`  NR batch error: ${e.message.slice(0,60)}`)
      } finally { nrClient.release() }
    }

    // Upsert CPT codes into procedure_catalog
    const cat = await pool.connect()
    try {
      for (const [code, data] of ssMap) {
        await cat.query(`
          INSERT INTO procedure_catalog (cpt_code, description, is_cms_shoppable)
          VALUES ($1, $2, false) ON CONFLICT (cpt_code) DO NOTHING
        `, [code, (data.description || 'Unknown').slice(0, 500)])
      }
    } finally { cat.release() }

    await markDone(npi, url, totalParsed, ssWritten, fmt, start)
    return { status: 'ok', rows: ssWritten, nr: nrWritten, codes: ssMap.size }

  } catch (err) {
    await markError(npi, url, err.message, start)
    return { status: 'error', error: err.message }
  }
}

// ── Wrapper: stream CSV → processRow callback ────────────────────
async function streamCSVToMaps(url, npi, processRow) {
  const text = await fetchText(url, { timeout: 300000 })
  if (!text || text.length < 100) throw new Error('Empty response')
  const firstLine = text.slice(0, 300).split('\n')[0] || ''
  const isV2 = firstLine.includes('hospital_name') || firstLine.includes('last_updated_on')
  let rowNum = 0, headers = null

  await new Promise((resolve, reject) => {
    const parser = csvParse({ relaxColumnCount: true, skipEmptyLines: true, trim: true, bom: true })
    parser.on('readable', () => {
      let record
      while ((record = parser.read()) !== null) {
        rowNum++
        if (isV2) {
          if (rowNum <= 2) continue
          if (rowNum === 3) { headers = record.map(h => (h||'').trim()); continue }
        } else {
          if (!headers) { headers = record.map(h => (h||'').trim()); continue }
        }
        if (!headers) continue
        const row = Object.fromEntries(headers.map((h,i) => [h, record[i]]))
        processRow(parseCSVRow(row, headers, npi, url))
      }
    })
    parser.on('error', reject)
    parser.on('end', resolve)
    Readable.from([text]).pipe(parser)
  })
}

// ── Wrapper: stream JSON → processRow callback ───────────────────
async function streamJSONToMaps(url, npi, processRow) {
  const res = await fetchStream(url)
  let buffer = '', inArray = false, depth = 0, item = ''
  let source = res.body
  const ct = res.headers.get('content-type') || ''
  if (ct.includes('gzip') || url.endsWith('.gz')) source = res.body.pipe(createGunzip())

  await new Promise((resolve, reject) => {
    source.on('data', chunk => {
      buffer += chunk.toString()
      if (!inArray) {
        const idx = buffer.indexOf('"standard_charge_information"')
        if (idx >= 0) { const a = buffer.indexOf('[', idx); if (a >= 0) { inArray = true; buffer = buffer.slice(a+1) } }
        if (!inArray) { const idx2 = buffer.indexOf('"items"'); if (idx2 >= 0) { const a = buffer.indexOf('[', idx2); if (a >= 0) { inArray = true; buffer = buffer.slice(a+1) } } }
        if (!inArray && buffer.length > 4096) buffer = buffer.slice(-2048)
        return
      }
      for (let i = 0; i < buffer.length; i++) {
        const ch = buffer[i]
        if (ch === '{') { depth++; item += ch }
        else if (ch === '}') {
          depth--; item += ch
          if (depth === 0 && item.trim().length > 2) {
            try { const rows = normalizeMRFItem(JSON.parse(item), npi, url); rows.forEach(processRow) } catch {}
            item = ''
          }
        } else if (depth > 0) item += ch
      }
      buffer = ''
    })
    source.on('end', resolve)
    source.on('error', reject)
  })
}

// ── Main ─────────────────────────────────────────────────────────
async function main() {
  initLog('04_layer1')
  log.info('=== Layer 1: Hospital MRF Price Scraping ===')
  log.info(`Concurrency: ${CONCURRENCY} | Resume: ${RESUME} | Limit: ${LIMIT_N || 'all'}`)

  const facilities = await getFacilities()
  log.info(`Hospitals with MRF URLs to process: ${facilities.length}`)

  if (!facilities.length) {
    log.warn('No hospitals with MRF URLs found.')
    log.warn('Run: node enrich_mrf_urls.mjs  to populate URLs from TPAFS CSV')
    await pool.end(); return
  }

  const prog = new Progress(facilities.length, 'Layer1')
  const lim  = pLimit(CONCURRENCY)

  const tasks = facilities.map(fac => lim(async () => {
    log.info(`Scraping: ${fac.name} → ${fac.url.slice(0, 70)}`)
    const result = await scrapeOne(fac)

    if (result.status === 'ok') {
      log.ok(`  ✅ ${fac.name}: ${result.rows} SS rows, ${result.nr} NR rows, ${result.codes} CPTs`)
      prog.tick('ok')
    } else {
      log.warn(`  ❌ ${fac.name}: ${result.error?.slice(0,80)}`)
      prog.tick('error')
    }
  }))

  await Promise.all(tasks)

  // Final stats
  const client = await pool.connect()
  try {
    const r = await client.query(`
      SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT npi) as providers_with_prices,
        COUNT(DISTINCT cpt_code) as unique_cpts
      FROM shoppable_services
    `)
    log.ok('\nLayer 1 complete!')
    console.table(r.rows)
  } finally {
    client.release()
    await pool.end()
  }
}

main().catch(e => { log.error('Fatal', { message: e.message }); process.exit(1) })
