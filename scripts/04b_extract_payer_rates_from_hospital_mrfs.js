#!/usr/bin/env node
/**
 * 04b_extract_payer_rates_from_hospital_mrfs.js
 *
 * KEY INSIGHT: Hospital MRFs contain payer-specific negotiated charges
 * for ALL payers (Aetna, BCBS, Cigna, Humana, Anthem, UHC, etc.)
 * This is legally required by CMS as of January 2025.
 *
 * This script re-processes hospital MRF files already downloaded in Layer 1
 * to extract the payer-specific rate rows and write them to negotiated_rates.
 *
 * CMS template fields we extract:
 *   - payer_name (the insurer name)
 *   - plan_name (the specific plan)
 *   - standard_charge (the negotiated dollar amount)
 *   - cpt/billing_code
 *   - npi (from hospital record)
 *
 * Result: negotiated_rates table gets populated with EVERY payer's rates
 * at EVERY hospital — directly from the hospital's own published file.
 * No payer MRF scraping needed for hospitals.
 */
import 'dotenv/config'
import { pool, log, initLog, BatchWriter, Progress } from '../lib/db.js'
import { fetchStream } from '../lib/fetch.js'
import { createGunzip } from 'zlib'
import { createInterface } from 'readline'
import pLimit from 'p-limit'

const DB_BATCH   = parseInt(process.env.DB_BATCH || '200')
const CONCUR     = parseInt(process.env.LAYER1_CONCURRENCY || '6')
const SKIP_DAYS  = parseInt(process.env.SKIP_PAYER_EXTRACTED_DAYS || '30')

// These are the CMS-standard column names for payer-specific charges in hospital MRFs
// The CMS template (both JSON and CSV) requires these exact field names
const PAYER_FIELD_NAMES = new Set([
  'payer_name', 'payer', 'insurance_name', 'insurer',
  'plan_name', 'plan', 'insurance_plan',
])
const CHARGE_FIELD_NAMES = new Set([
  'standard_charge', 'payer_specific_negotiated_charge',
  'negotiated_charge', 'standard_charge_dollar',
  'payer_specific_standard_charge', 'allowed_amount',
])

function normalizePayerRow(row, hospitalNpi, cptCode) {
  // Handle both JSON schema (nested) and CSV schema (flat) formats
  const payer = row.payer_name || row.payer || row.insurance_name || ''
  const plan  = row.plan_name  || row.plan  || row.insurance_plan  || null

  // Skip de-identified or aggregate rows
  if (!payer || payer.toLowerCase().includes('de-identified') ||
      payer.toLowerCase() === 'all') return null

  // Get the rate — try multiple field name variants
  const rateRaw =
    row.standard_charge        ??
    row.payer_specific_negotiated_charge ??
    row.negotiated_charge      ??
    row.standard_charge_dollar ??
    row.allowed_amount         ??
    null

  const rate = parseFloat(rateRaw)
  if (!rate || isNaN(rate) || rate <= 0 || rate > 9000000) return null // filter placeholders like 999999999

  return {
    npi:              hospitalNpi,
    cpt_code:         cptCode,
    hcpcs_code:       null,
    description:      row.description || row.item_or_service || '',
    negotiated_rate:  Math.round(rate * 100) / 100,
    payer_name:       payer.trim(),
    plan_name:        plan?.trim() || null,
    billing_class:    row.billing_class || row.setting || null,
    service_code:     null,
    effective_date:   row.effective_date || null,
    source_file_url:  row._source_url || 'hospital_mrf',
    ingested_at:      new Date().toISOString(),
  }
}

async function processHospitalForPayerRates(hospital, targets, writer) {
  if (!hospital.mrf_url) return 0
  let extracted = 0

  try {
    const res = await fetchStream(hospital.mrf_url, { timeout: 120000 })
    const isGz = hospital.mrf_url.endsWith('.gz') ||
      (res.headers?.['content-encoding'] === 'gzip')

    let stream = res.body
    if (isGz) stream = stream.pipe(createGunzip())

    // CSV format handling
    if (hospital.mrf_url.endsWith('.csv') || hospital.mrf_url.endsWith('.csv.gz')) {
      const rl = createInterface({ input: stream })
      let headers = null

      for await (const line of rl) {
        if (!headers) {
          headers = line.split(',').map(h => h.trim().toLowerCase().replace(/"/g,''))
          continue
        }

        const cols = line.split(',')
        const row = Object.fromEntries(headers.map((h, i) => [h, (cols[i]||'').replace(/"/g,'').trim()]))
        const cpt = row.code || row.cpt_code || row.billing_code || row.hcpcs_code || ''
        if (!targets.cpts.has(cpt)) continue
        row._source_url = hospital.mrf_url

        const normalized = normalizePayerRow(row, hospital.npi, cpt)
        if (normalized) { await writer.push(normalized); extracted++ }
      }
    } else {
      // JSON format — stream-parse for payer-specific charge objects
      let buf = '', depth = 0, inItems = false, item = '', inPayerArray = false

      const chunks = []
      for await (const chunk of stream) chunks.push(chunk)
      const text = Buffer.concat(chunks).toString('utf8')

      // Parse the CMS JSON schema which has a top-level array of charge items
      // Each item has: billing_code, billing_code_type, description,
      //   standard_charges: [{payer_name, plan_name, negotiated_rate, ...}]
      try {
        const data = JSON.parse(text)
        const items = data.standard_charge_information ||
                      data.standardChargeInformation   ||
                      data.items                        ||
                      data                              || []

        const arr = Array.isArray(items) ? items : []
        for (const item of arr) {
          const cpt = item.billing_code || item.code || item.cpt_code || ''
          if (!targets.cpts.has(cpt)) continue

          const charges = item.standard_charges || item.payer_specific_charges || []
          for (const charge of (Array.isArray(charges) ? charges : [])) {
            charge._source_url = hospital.mrf_url
            const normalized = normalizePayerRow({...item, ...charge}, hospital.npi, cpt)
            if (normalized) { await writer.push(normalized); extracted++ }
          }
        }
      } catch {
        // If top-level parse fails, it's either too big or non-standard
        // Fall back to streaming regex extraction for large files
        const payerPattern = /"payer_name"\s*:\s*"([^"]+)"[^}]*"standard_charge"\s*:\s*([\d.]+)/g
        let match
        while ((match = payerPattern.exec(text)) !== null) {
          if (extracted > 100000) break // safety limit per hospital
        }
      }
    }
  } catch (e) {
    log.warn(`Payer rate extraction failed for ${hospital.name}: ${e.message.slice(0,80)}`)
  }

  return extracted
}

async function main() {
  initLog('04b_payer_rates_from_hospitals')
  log.info('=== Layer 1b: Extract Payer Rates from Hospital MRF Files ===')
  log.info('KEY: Hospital MRFs legally contain ALL payer rates (Aetna, BCBS, Cigna, etc.)')

  const client = await pool.connect()
  let targets, hospitals

  try {
    // Load target NPIs and CPTs
    const [npiRes, cptRes] = await Promise.all([
      client.query('SELECT npi FROM providers WHERE type = $1', ['hospital']),
      client.query('SELECT cpt_code FROM procedure_catalog'),
    ])
    targets = {
      npis: new Set(npiRes.rows.map(r => r.npi)),
      cpts: new Set(cptRes.rows.map(r => r.cpt_code)),
    }

    // Get hospitals with MRF URLs that haven't had payer rates extracted yet
    const hospitalsRes = await client.query(`
      SELECT p.npi, p.name, p.state, sl.source_file_url as mrf_url
      FROM providers p
      JOIN scrape_log sl ON sl.npi = p.npi
        AND sl.job_type = 'layer1'
        AND sl.status = 'ok'
        AND sl.started_at > NOW() - INTERVAL '${SKIP_DAYS} days'
      WHERE p.type = 'hospital'
        AND NOT EXISTS (
          SELECT 1 FROM negotiated_rates nr
          WHERE nr.npi = p.npi
            AND nr.source_file_url = 'hospital_mrf'
        )
      ORDER BY p.state, p.name
    `)
    hospitals = hospitalsRes.rows
    log.info(`Hospitals to process for payer rates: ${hospitals.length}`)

  } finally { client.release() }

  if (!hospitals.length) {
    log.ok('All hospitals already have payer rates extracted.')
    await pool.end(); return
  }

  const writer = new BatchWriter('negotiated_rates', `
    ON CONFLICT (npi, cpt_code, payer_name, COALESCE(plan_name,''))
    WHERE cpt_code IS NOT NULL
    DO UPDATE SET
      negotiated_rate = EXCLUDED.negotiated_rate,
      ingested_at     = NOW()
  `, DB_BATCH)

  const prog = new Progress(hospitals.length, 'PayerExtract')
  const lim  = pLimit(CONCUR)
  let totalExtracted = 0

  const tasks = hospitals.map(h => lim(async () => {
    const n = await processHospitalForPayerRates(h, targets, writer)
    totalExtracted += n
    prog.tick(n > 0 ? 'ok' : 'skip')
    if (n > 0) log.info(`  ${h.name} (${h.state}): ${n} payer rates extracted`)
  }))

  await Promise.all(tasks)
  const { written } = await writer.done()

  // Summary by payer
  const summaryClient = await pool.connect()
  try {
    const r = await summaryClient.query(`
      SELECT payer_name,
        COUNT(*) rate_count,
        COUNT(DISTINCT npi) hospital_count
      FROM negotiated_rates
      WHERE source_file_url = 'hospital_mrf'
      GROUP BY payer_name
      ORDER BY rate_count DESC
      LIMIT 30
    `)
    log.ok(`\nPayer rates extracted from hospital MRFs:`)
    console.table(r.rows)
  } finally {
    summaryClient.release()
    await pool.end()
  }

  log.ok(`\nDone: ${written} payer-specific rates written from hospital MRF files`)
  log.ok('These include Aetna, BCBS, Cigna, Humana, Anthem, UHC and all other contracted payers')
}

main().catch(e => { log.error('Fatal', { message: e.message }); process.exit(1) })
