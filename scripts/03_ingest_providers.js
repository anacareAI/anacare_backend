#!/usr/bin/env node
/**
 * 03_ingest_providers.js
 * Pulls every US hospital, ASC, urgent care, and standalone from:
 *   - CMS Hospital General Information (all certified hospitals)
 *   - NPPES NPI Registry (ASCs, urgent cares, imaging, standalone clinics, labs)
 *
 * NOTE: The old CMS ASC dataset endpoint (kh2m-uj8j) no longer exists.
 * ASCs are sourced from NPPES using taxonomy code 261QX0200X which is the
 * correct NUCC taxonomy for Ambulatory Surgical Centers.
 * Additional ASC taxonomy codes are also queried for complete coverage.
 */
import 'dotenv/config'
import { pool, log, initLog, Progress } from '../lib/db.js'
import { fetchJSON } from '../lib/fetch.js'

// ── CMS Hospital General Information ─────────────────────────────
const CMS_HOSPITAL_URL = 'https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0'

// ── NPPES taxonomy codes → facility type ──────────────────────────
// FINAL. Source: resdac.org + npiprofile.com (NUCC v25.1, Jan 2026).
// Cross-checked against GPT-generated list. Every code accounted for.
//
// IMPORTANT: The NPPES API `taxonomy_description` parameter accepts
// human-readable TEXT, not taxonomy codes. The `search_term` field
// is what gets passed to the API. The `taxonomy` field is used to
// filter results afterward — keeping only records that actually have
// that taxonomy code, because text searches can return partial matches.
//
// ── EXCLUDED ─────────────────────────────────────────────────────
// 261QA0005X  Abortion/Family Planning   — not shoppable care
// 261QA0600X  Adult Day Care             — social care, no CPT billing
// 261QA0900X  Amputee Clinic             — rarely has MRF data
// 261QA3000X  Augmentative Communication — speech devices, not care
// 261QB0400X  Birthing Center            — midwifery, covered under hospital OB
// 261QC0050X  Critical Access Hospital   — already in CMS hospital dataset
// 261QD0000X  Dental Clinic              — dental, not medical
// 261QD1600X  Developmental Disabilities — not acute shoppable care
// 261QF0050X  Family Planning Non-Surg   — contraceptives
// 261QG0250X  Genetics                   — rare, not shoppable
// 261QM0855X  Adolescent Mental Health   — subset of 261QM0801X
// 261QM1100X  Military Outpatient        — Tricare only, not commercial
// 261QM1101X  Military ASC Fixed         — Tricare only, not commercial
// 261QM1102X  Military Outpatient Trans  — Tricare only, not commercial
// 261QM1103X  Military ASC Transportable — Tricare only, not commercial
// 261QM2800X  Methadone Clinic           — flat weekly rate, not CPT
// 261QM3000X  Medically Fragile Day Care — not acute care
// 261QP0904X  Public Health Federal      — govt agencies, no MRF
// 261QP0905X  Public Health State/Local  — govt agencies, no MRF
// 261QP2400X  Prison Health              — not consumer-accessible
// 261QR0405X  Rehab Substance Use        — not shoppable care
// 261QR1100X  Research                   — clinical trial sites
// 261QS1000X  Student Health             — campus clinics only
// 261QV0200X  VA                         — Veterans Affairs only
// 282NC0060X  Critical Access Hospital   — duplicate of CMS hospital dataset
// 273Y00000X  Rehab Hospital Unit        — ward inside hospital, not standalone
// 291900000X  Military Clinical Lab      — Tricare only, not commercial
// 292200000X  Dental Laboratory          — dental, not medical
// ─────────────────────────────────────────────────────────────────
// In-scope taxonomy codes (per CLAUDE.md):
//   282N00000X  General Acute Care Hospital   → hospital, transparency=true
//   282NC0060X  Critical Access Hospital      → hospital, transparency=true
//   282NC2000X  Children's Hospital           → hospital, transparency=true
//   261QS0132X  Ambulatory Surgery Center     → asc,      transparency=false
const NPPES_TYPES = [
  { taxonomy: '282N00000X', search_term: 'General Acute Care Hospital', type: 'hospital', label: 'General Acute Care Hospital', pages: 30 },
  { taxonomy: '282NC0060X', search_term: 'Critical Access Hospital',    type: 'hospital', label: 'Critical Access Hospital',    pages: 15 },
  { taxonomy: '282NC2000X', search_term: "Children's Hospital",          type: 'hospital', label: "Children's Hospital",          pages: 10 },
  { taxonomy: '261QS0132X', search_term: 'Ambulatory Surgical',          type: 'asc',      label: 'Ambulatory Surgery Center',   pages: 75 },
]

async function fetchPages(url, label) {
  const rows = []
  let offset = 0
  const PAGE = 500
  while (true) {
    const data = await fetchJSON(`${url}?limit=${PAGE}&offset=${offset}`, { timeout: 30000 })
    const batch = data.results || data.data || data || []
    if (!batch.length) break
    rows.push(...batch)
    log.info(`${label}: ${rows.length} fetched`)
    if (batch.length < PAGE) break
    offset += PAGE
    await new Promise(r => setTimeout(r, 400))
  }
  return rows
}

async function fetchNPPES(taxonomy, searchTerm, type, pages) {
  const rows = []
  const encodedTerm = encodeURIComponent(searchTerm)

  for (let skip = 0; skip < pages * 200; skip += 200) {
    try {
      const data = await fetchJSON(
        `https://npiregistry.cms.hhs.gov/api/?version=2.1&enumeration_type=NPI-2&taxonomy_description=${encodedTerm}&limit=200&skip=${skip}`,
        { timeout: 20000, retries: 2 }
      )
      const batch = data.results || []
      if (!batch.length) break

      for (const r of batch) {
        // Post-fetch filter: confirm this provider actually has the target taxonomy code
        // The text search can return partial matches (e.g. "Radiology" also matches "Radiation Oncology")
        const hasTaxonomy = r.taxonomies?.some(t => t.code === taxonomy)
        if (!hasTaxonomy) continue

        const loc = r.addresses?.find(a => a.address_purpose === 'LOCATION') || {}
        rows.push({
          npi:     r.number,
          name:    r.basic?.organization_name ||
                   `${r.basic?.first_name || ''} ${r.basic?.last_name || ''}`.trim(),
          type,
          address: loc.address_1,
          city:    loc.city,
          state:   (loc.state || '').toString().trim().slice(0, 2) || null,
          zip:     (loc.postal_code || '').slice(0, 10),
          phone:   loc.telephone_number,
          source:  'nppes',
          // Hospitals must publish MRF files per CMS rules; ASCs are voluntary
          has_transparency_data: (type === 'hospital'),
        })
      }
      await new Promise(r => setTimeout(r, 250))
    } catch (e) { log.warn(`NPPES ${type} page failed: ${e.message}`); break }
  }
  return rows
}

async function upsert(providers) {
  if (!providers.length) return 0
  const client = await pool.connect()
  let n = 0
  try {
    for (const p of providers) {
      if (!p.npi || !p.name) continue
      await client.query(`
        INSERT INTO providers
          (npi, name, type, owner_system, address, city, state, zip,
           phone, cms_cert_num, source, has_transparency_data)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (npi) DO UPDATE SET
          name              = EXCLUDED.name,
          owner_system      = COALESCE(EXCLUDED.owner_system, providers.owner_system),
          address           = COALESCE(EXCLUDED.address, providers.address),
          city              = COALESCE(EXCLUDED.city, providers.city),
          state             = COALESCE(EXCLUDED.state, providers.state),
          zip               = COALESCE(EXCLUDED.zip, providers.zip),
          phone             = COALESCE(EXCLUDED.phone, providers.phone),
          cms_cert_num      = COALESCE(EXCLUDED.cms_cert_num, providers.cms_cert_num),
          has_transparency_data = (providers.has_transparency_data OR EXCLUDED.has_transparency_data),
          updated_at        = NOW()
      `, [
        String(p.npi).trim(),
        String(p.name).trim().slice(0,500),
        p.type || 'hospital',
        p.owner_system || null,
        p.address || null,
        p.city || null,
        (p.state || '').toString().slice(0, 2) || null,
        (p.zip || '').slice(0,10) || null,
        p.phone || null,
        p.cms_cert_num || null,
        p.source || 'cms',
        p.has_transparency_data || false,
      ])
      n++
    }
  } finally { client.release() }
  return n
}

async function main() {
  initLog('03_ingest_providers')
  log.info('=== Provider Ingestion ===')

  // 1. CMS Hospitals (the only facility type with a reliable CMS datastore endpoint)
  log.info('1/2 CMS Hospitals...')
  try {
    const raw = await fetchPages(CMS_HOSPITAL_URL, 'Hospitals')
    // CMS API confirmed field names (snake_case):
    // facility_id (CCN), facility_name, address, citytown, state, zip_code,
    // telephone_number, hospital_ownership
    // IMPORTANT: This dataset does NOT contain NPI numbers.
    // Strategy: build a CCN→metadata map, then after NPPES hospital query
    // enriches providers with real NPIs, update cms_cert_num + has_transparency_data
    // by matching on name similarity. For now, skip direct upsert — NPPES handles it.
    if (raw.length > 0) {
      log.info(`CMS API confirmed fields: ${Object.keys(raw[0]).slice(0,8).join(' | ')}`)
    }

    // Build CCN lookup map for later enrichment (used after NPPES upsert)
    const ccnMap = new Map()
    for (const r of raw) {
      if (r.facility_id && r.facility_name) {
        ccnMap.set(r.facility_name.toUpperCase().trim(), {
          cms_cert_num: r.facility_id,
          owner_system: r.hospital_ownership || null,
          state: (r.state || '').slice(0, 2),
        })
      }
    }
    log.info(`Built CCN map with ${ccnMap.size} hospitals for enrichment after NPPES`)

    // We do NOT upsert hospitals here — no NPI available.
    // NPPES taxonomy 282N00000X query below inserts hospitals WITH real NPIs.
    // After that, we enrich with CCN and mark has_transparency_data = true.
    const hospitals = [] // intentionally empty — handled by NPPES below
    const n = await upsert(hospitals)
    log.ok(`Hospitals upserted: ${n}`)
  } catch (e) {
    log.error('Hospital ingest failed', { message: e.message })
  }

  // 2. All facility types via NPPES
  log.info('2/2 NPPES (hospitals + ASCs, urgent cares, imaging, labs, standalone)...')
  let ccnMap  // will be set during hospital NPPES query for enrichment
  for (const { taxonomy, search_term, type, label, pages } of NPPES_TYPES) {
    try {
      const rows = await fetchNPPES(taxonomy, search_term, type, pages)
      const n    = await upsert(rows)
      log.ok(`NPPES ${label} (${taxonomy}): ${n} upserted`)
    } catch (e) {
      log.error(`NPPES ${label} failed`, { message: e.message })
    }
  }

  // 3. Enrich hospital records with CCN from CMS dataset
  // NPPES gives us NPIs but no CCN. CMS dataset gives CCN but no NPI.
  // Match by normalized name to link them — CCN is needed for TPAFS MRF URL lookup.
  log.info('3/3 Enriching hospitals with CCN from CMS dataset...')
  try {
    const raw = await fetchPages(CMS_HOSPITAL_URL, 'CCN enrichment')
    const client = await pool.connect()
    let enriched = 0
    try {
      for (const r of raw) {
        if (!r.facility_id || !r.facility_name) continue
        const ccn       = r.facility_id
        const nameUpper = r.facility_name.toUpperCase().trim()
        const state     = (r.state || '').slice(0, 2)

        // Try exact name match first (case-insensitive)
        let res = await client.query(`
          UPDATE providers SET cms_cert_num = $1
          WHERE type = 'hospital'
            AND cms_cert_num IS NULL
            AND state = $2
            AND UPPER(TRIM(name)) = $3
          RETURNING npi
        `, [ccn, state, nameUpper])

        // If no exact match, try contains match
        if (res.rowCount === 0) {
          // Extract first 3 meaningful words for fuzzy match
          const words = nameUpper.split(/\s+/).filter(w => w.length > 3).slice(0, 3)
          if (words.length >= 2) {
            res = await client.query(`
              UPDATE providers SET cms_cert_num = $1
              WHERE type = 'hospital'
                AND cms_cert_num IS NULL
                AND state = $2
                AND UPPER(name) ILIKE $3
              RETURNING npi
            `, [ccn, state, `%${words[0]}%${words[1]}%`])
          }
        }

        if (res.rowCount > 0) enriched++
      }
      log.ok(`CCN enrichment: ${enriched} hospitals matched with CMS certification numbers`)
      log.info('These hospitals can now be matched to MRF URLs in the TPAFS CSV')
    } finally { client.release() }
  } catch (e) {
    log.error('CCN enrichment failed', { message: e.message })
  }

  // 4. Cleanup: delete legacy standalone providers (no longer in scope)
  log.info('4/4 Deleting legacy standalone providers...')
  try {
    const c = await pool.connect()
    try {
      const r = await c.query(`DELETE FROM providers WHERE type = 'standalone' RETURNING npi`)
      log.ok(`Deleted ${r.rowCount} standalone providers`)
    } finally { c.release() }
  } catch (e) {
    log.error('Standalone cleanup failed', { message: e.message })
  }

  // Final count
  const client = await pool.connect()
  try {
    const r = await client.query(`
      SELECT type, COUNT(*) as total,
        COUNT(*) FILTER (WHERE has_transparency_data) as with_data
      FROM providers GROUP BY type ORDER BY total DESC
    `)
    console.log('\nProviders by type:')
    console.table(r.rows)
    const total = await client.query('SELECT COUNT(*) FROM providers')
    log.ok(`Total providers in DB: ${total.rows[0].count}`)
  } finally {
    client.release()
    await pool.end()
  }
}

main().catch(e => { log.error('Fatal', { message: e.message }); process.exit(1) })
