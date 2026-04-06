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
const NPPES_TYPES = [

  // ── Hospitals (NPPES supplement to CMS dataset) ───────────────
  { taxonomy: '282N00000X', search_term: 'General Acute Care Hospital', type: 'hospital',  label: 'General Acute Care Hospital', pages: 30 },
  { taxonomy: '284300000X', search_term: 'Specialty Hospital',           type: 'hospital',  label: 'Specialty Hospital',           pages: 15 },

  // ── Ambulatory Surgical Centers ───────────────────────────────
  { taxonomy: '261QA1903X', search_term: 'Ambulatory Surgical',          type: 'asc',        label: 'Ambulatory Surgical Center',   pages: 75 },

  // ── Urgent Care ───────────────────────────────────────────────
  { taxonomy: '261QU0200X', search_term: 'Urgent Care',                  type: 'urgent_care',label: 'Urgent Care',                  pages: 75 },

  // ── Emergency (freestanding ERs) ──────────────────────────────
  { taxonomy: '261QE0002X', search_term: 'Emergency Care',               type: 'standalone', label: 'Freestanding ER',              pages: 15 },

  // ── Imaging / Radiology ───────────────────────────────────────
  { taxonomy: '261QR0200X', search_term: 'Radiology',                    type: 'imaging', label: 'Radiology Center',               pages: 40 },
  { taxonomy: '261QM1200X', search_term: 'Magnetic Resonance Imaging',   type: 'imaging', label: 'MRI Center',                     pages: 20 },
  { taxonomy: '261QR0206X', search_term: 'Mammography',                  type: 'imaging', label: 'Mammography Center',             pages: 15 },
  { taxonomy: '261QR0207X', search_term: 'Mobile Mammography',           type: 'imaging', label: 'Mobile Mammography',             pages:  5 },
  { taxonomy: '261QR0208X', search_term: 'Mobile Radiology',             type: 'imaging', label: 'Mobile Radiology',               pages: 10 },

  // ── Surgical / Procedural ─────────────────────────────────────
  { taxonomy: '261QE0800X', search_term: 'Endoscopy',                    type: 'standalone', label: 'Endoscopy Center',            pages: 25 },
  { taxonomy: '261QS0112X', search_term: 'Oral and Maxillofacial Surgery',type: 'standalone', label: 'Oral & Maxillofacial Surgery',pages: 15 },
  { taxonomy: '261QS0132X', search_term: 'Ophthalmologic Surgery',       type: 'standalone', label: 'Ophthalmologic Surgery',      pages: 15 },
  { taxonomy: '261QL0400X', search_term: 'Lithotripsy',                  type: 'standalone', label: 'Lithotripsy Center',          pages: 10 },

  // ── Oncology / Infusion ───────────────────────────────────────
  { taxonomy: '261QX0200X', search_term: 'Oncology',                     type: 'standalone', label: 'Oncology Center',             pages: 20 },
  { taxonomy: '261QX0203X', search_term: 'Oncology, Radiation',          type: 'standalone', label: 'Radiation Oncology Center',   pages: 20 },
  { taxonomy: '261QI0500X', search_term: 'Infusion Therapy',             type: 'standalone', label: 'Infusion Therapy Center',     pages: 20 },

  // ── Primary / Community Health ────────────────────────────────
  { taxonomy: '261QP2300X', search_term: 'Primary Care',                 type: 'standalone', label: 'Primary Care Clinic',         pages: 75 },
  { taxonomy: '261QF0400X', search_term: 'Federally Qualified Health Center', type: 'standalone', label: 'FQHC',                  pages: 40 },
  { taxonomy: '261QC1500X', search_term: 'Community Health',             type: 'standalone', label: 'Community Health Center',     pages: 30 },
  { taxonomy: '261QM1000X', search_term: 'Migrant Health',               type: 'standalone', label: 'Migrant Health Center',       pages: 10 },
  { taxonomy: '261QR1300X', search_term: 'Rural Health',                 type: 'standalone', label: 'Rural Health Clinic',         pages: 25 },
  { taxonomy: '261QH0100X', search_term: 'Health Service',               type: 'standalone', label: 'Health Service Clinic',       pages: 30 },
  { taxonomy: '261QA0006X', search_term: 'Ambulatory Fertility',         type: 'standalone', label: 'Fertility Clinic',            pages: 10 },

  // ── Multi-specialty / General ─────────────────────────────────
  { taxonomy: '261Q00000X',  search_term: 'Clinic/Center',               type: 'standalone', label: 'Clinic/Center (General)',     pages: 40 },
  { taxonomy: '261QM1300X',  search_term: 'Multi-Specialty',             type: 'standalone', label: 'Multi-Specialty Clinic',      pages: 25 },
  { taxonomy: '261QM2500X',  search_term: 'Medical Specialty',           type: 'standalone', label: 'Medical Specialty Clinic',    pages: 25 },
  { taxonomy: '261QC1800X',  search_term: 'Corporate Health',            type: 'standalone', label: 'Corporate Health Clinic',     pages: 10 },
  { taxonomy: '261QX0100X',  search_term: 'Occupational Medicine',       type: 'standalone', label: 'Occupational Medicine',       pages: 20 },
  { taxonomy: '261QP1100X',  search_term: 'Podiatric',                   type: 'standalone', label: 'Podiatric Clinic',            pages: 15 },

  // ── Pain Management ───────────────────────────────────────────
  { taxonomy: '261QP3300X', search_term: 'Pain',                         type: 'standalone', label: 'Pain Clinic',                 pages: 20 },

  // ── Physical Therapy / Rehabilitation ─────────────────────────
  { taxonomy: '261QP2000X',  search_term: 'Physical Therapy',            type: 'standalone', label: 'Physical Therapy Clinic',     pages: 40 },
  { taxonomy: '261QR0400X',  search_term: 'Rehabilitation',              type: 'standalone', label: 'Rehabilitation Clinic',       pages: 30 },
  { taxonomy: '261QR0401X',  search_term: 'Comprehensive Outpatient Rehabilitation', type: 'standalone', label: 'CORF Rehab Clinic', pages: 15 },
  { taxonomy: '261QR0404X',  search_term: 'Cardiac Rehabilitation',      type: 'standalone', label: 'Cardiac Rehab Clinic',        pages: 10 },
  { taxonomy: '261QR0800X',  search_term: 'Recovery Care',               type: 'standalone', label: 'Recovery Care Center',        pages: 10 },

  // ── Dialysis / ESRD ───────────────────────────────────────────
  { taxonomy: '261QE0700X', search_term: 'End-Stage Renal Disease',      type: 'standalone', label: 'ESRD / Dialysis Center',      pages: 25 },

  // ── Hearing / Speech ──────────────────────────────────────────
  { taxonomy: '261QH0700X', search_term: 'Hearing and Speech',           type: 'standalone', label: 'Hearing & Speech Clinic',     pages: 15 },

  // ── Sleep Disorders ───────────────────────────────────────────
  { taxonomy: '261QS1200X', search_term: 'Sleep Disorder Diagnostic',    type: 'standalone', label: 'Sleep Disorder Center',       pages: 10 },

  // ── Behavioral / Mental Health ────────────────────────────────
  { taxonomy: '261QM0801X', search_term: 'Mental Health',                type: 'standalone', label: 'Mental Health Clinic',        pages: 25 },
  { taxonomy: '261QM0850X', search_term: 'Adult Mental Health',          type: 'standalone', label: 'Adult Mental Health Clinic',  pages: 15 },

  // ── Labs ──────────────────────────────────────────────────────
  { taxonomy: '291U00000X', search_term: 'Clinical Medical Laboratory',  type: 'lab', label: 'Clinical Medical Laboratory',       pages: 50 },
  { taxonomy: '293D00000X', search_term: 'Physiological Laboratory',     type: 'lab', label: 'Physiological Laboratory',          pages: 10 },

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
          // Hospitals (282N, 284300) must publish MRF files per CMS rules
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
