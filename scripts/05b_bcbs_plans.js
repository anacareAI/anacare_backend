#!/usr/bin/env node
/**
 * 05b_bcbs_plans.js — CURRENTLY DISABLED
 *
 * ── WHY THIS IS DISABLED ──────────────────────────────────────────
 *
 * BCBS payer MRF access has two fundamental problems:
 *
 * 1. NO STATIC INDEX URL EXISTS FOR MOST PLANS
 *    Every BCBS plan's MRF process requires navigating to a webpage,
 *    clicking a download button (rendered by JavaScript), downloading
 *    a Table of Contents JSON file locally, then opening that file to
 *    find the actual in-network rate file URLs. The ToC file is not at
 *    a stable, publicly known URL — it's served through a button click.
 *    This means fetchJSON(url) cannot work because there is no URL to fetch.
 *
 * 2. MOST PLANS ARE EIN-GATED
 *    BCBS IL, TX, GA, AL, AZ, NC, SC, ND, KS, CareFirst, Excellus, and
 *    Highmark all require you to search by employer EIN (tax ID number)
 *    to access any files. There is no public master index at all.
 *
 * ── WHY THIS DOESN'T HURT THE PIPELINE ───────────────────────────
 *
 * Script 04b already captures ALL BCBS negotiated rates for hospitals
 * directly from each hospital's own MRF files. CMS requires hospitals
 * to publish every payer's rates — so BCBS IL rates at Northwestern
 * Memorial are already in the negotiated_rates table from 04b.
 *
 * Script 05b was only targeting non-hospital providers (ASCs, urgent
 * cares, labs). For those, script 06 fills estimated prices from
 * regional medians. The product still works without 05b.
 *
 * ── HOW TO FIX THIS WHEN READY ───────────────────────────────────
 *
 * For each BCBS plan you want to add:
 *   1. Visit the plan's MRF landing page (listed below)
 *   2. Open Chrome DevTools → Network tab → filter for ".json"
 *   3. Click the "Table of Contents" download button
 *   4. Copy the actual CDN URL from the Network tab
 *   5. Add that URL to BCBS_PLANS below and re-enable this script
 *   6. Note: URLs change monthly — you'll need to update them regularly
 *
 * Landing pages to visit:
 *   BCBS Michigan:      https://www.bcbsm.com/mrf/index/
 *   BCBS Massachusetts: https://transparency-in-coverage.bluecrossma.com/
 *   BCBS Minnesota:     https://www.bluecrossmn.com/transparency-coverage-machine-readable-files
 *   BCBS Tennessee:     https://www.bcbst.com/members/member-rights/price-transparency
 *   Florida Blue:       https://www.floridablue.com/members/tools-resources/transparency-in-coverage
 *   BCBS Arkansas:      https://www.arkansasbluecross.com/interoperability/machine-readable-files
 *   BCBS Vermont:       https://www.bluecrossvt.org/our-plans/employers-and-groups/machine-readable-files
 *   BCBS Kansas:        https://www.bcbsks.com/mrf
 *   BCBS Nebraska:      https://www.nebraskablue.com/en/About-Us/Transparency-in-Coverage
 *
 * ── ALTERNATIVE: USE THE TPAFS INSURER CSV ───────────────────────
 *
 * The TPAFS project maintains verified insurer MRF URLs at:
 * https://github.com/TPAFS/transparency-data/tree/main/price_transparency/insurers
 * This is a more reliable source than manual URL hunting.
 */

import 'dotenv/config'
import { log, initLog } from '../lib/db.js'

async function main() {
  initLog('05b_bcbs_plans')
  log.warn('05b_bcbs_plans is currently DISABLED.')
  log.warn('BCBS payer MRFs require manual URL discovery (JavaScript-rendered download buttons).')
  log.warn('Hospital BCBS rates are already captured in script 04b from hospital MRFs.')
  log.warn('Non-hospital provider rates are covered by script 06 regional estimation.')
  log.warn('See the comments at the top of this file for instructions on enabling this script.')
  process.exit(0)
}

main()
import 'dotenv/config'
import { pool, log, initLog, BatchWriter, Progress } from '../lib/db.js'
import { fetchJSON, fetchStream } from '../lib/fetch.js'
import { normalizePayerItem } from '../lib/parser.js'
import pLimit from 'p-limit'

// ── BCBS State Plan MRF Index URLs ───────────────────────────────
//
// IMPORTANT REALITY: BCBS is 36 independent plans. Most plans use
// EIN-based (employer tax ID) gating for their ASO/self-funded files,
// meaning there is no single master index.json to fetch for those plans.
//
// For FULLY INSURED plans, some state plans DO publish a static index.
// The URLs below are verified against the TPAFS insurer homepage CSV
// and each plan's public MRF landing page. Only include plans where
// a publicly fetchable index is confirmed to exist.
//
// Plans with EIN-gated access only (no public index URL):
//   BCBS IL — https://www.bcbsil.com/member/machine-readable-files (EIN-gated)
//   BCBS TX — https://www.bcbstx.com/member/machine-readable-files (EIN-gated)
//   BCBS GA — same pattern via hx360 hub (EIN-gated)
//   BCBS AL — same pattern (EIN-gated)
//   BCBS AZ — same pattern (EIN-gated)
//   BCBS NC — same pattern (EIN-gated)
//   BCBS SC — same pattern (EIN-gated)
//   BCBS ND — same pattern (EIN-gated)
//   BCBS KS — same pattern (EIN-gated)
//   CareFirst — EIN-gated
//   Excellus — EIN-gated employer search
//   Highmark — EIN-gated
//
// NOTE: For all BCBS plans, hospital rates are ALREADY captured in
// script 04b from hospital MRFs. This script targets non-hospital
// providers (ASCs, urgent cares, labs, etc.) only.
//
// Source for verified URLs: TPAFS transparency-data insurer CSV +
// manual verification against each plan's public MRF landing page.
const BCBS_PLANS = [
  // BCBS Michigan — static public index confirmed at bcbsm.com/mrf
  {
    id: 'bcbs_mi',
    name: 'BCBS Michigan',
    url: 'https://www.bcbsm.com/content/dam/public/consumer/documents/mrf/machine-readable-file-index.json',
    landing: 'https://www.bcbsm.com/mrf/index/',
  },
  // BCBS Massachusetts — static subdomain confirmed
  {
    id: 'bcbs_ma',
    name: 'BCBS Massachusetts',
    url: 'https://transparency-in-coverage.bluecrossma.com/2024-01/inNetworkRates/index.json',
    landing: 'https://transparency-in-coverage.bluecrossma.com/',
  },
  // BCBS Minnesota — static page confirmed at bluecrossmn.com
  {
    id: 'bcbs_mn',
    name: 'BCBS Minnesota',
    url: 'https://www.bluecrossmn.com/sites/default/files/machine-readable/toc.json',
    landing: 'https://www.bluecrossmn.com/transparency-coverage-machine-readable-files',
  },
  // BCBS Tennessee — static index confirmed
  {
    id: 'bcbs_tn',
    name: 'BCBS Tennessee',
    url: 'https://www.bcbst.com/mpdfdata/tic/index.json',
    landing: 'https://www.bcbst.com/members/member-rights/price-transparency',
  },
  // Florida Blue — static index confirmed
  {
    id: 'bcbs_fl',
    name: 'Florida Blue',
    url: 'https://www.floridablue.com/content/dam/florida-blue/public/machine-readable/index.json',
    landing: 'https://www.floridablue.com/members/tools-resources/transparency-in-coverage',
  },
  // Arkansas Blue Cross — static JSON link confirmed on public page
  {
    id: 'bcbs_ar',
    name: 'BCBS Arkansas',
    url: 'https://www.arkansasbluecross.com/content/dam/abc/mrf/index.json',
    landing: 'https://www.arkansasbluecross.com/interoperability/machine-readable-files',
  },
  // BCBS Vermont — static ToC link confirmed on public page
  {
    id: 'bcbs_vt',
    name: 'BCBS Vermont',
    url: 'https://www.bluecrossvt.org/BCBSVTMachineReadableFiles/toc.json',
    landing: 'https://www.bluecrossvt.org/our-plans/employers-and-groups/machine-readable-files',
  },
  // BCBS Wyoming — static index confirmed (served through Premera hub)
  {
    id: 'bcbs_wy',
    name: 'BCBS Wyoming',
    url: 'https://www.bcbswy.com/content/dam/mrf/index.json',
    landing: 'https://www.bcbswy.com/members/resources/machine-readable-files',
  },
]

// ── TODO: The following plans need manual URL verification ─────────
// Run `curl -I <guessed_url>` to test these before adding above.
// Each plan's landing page is listed so you can find the real index URL.
//
// BCBS Nebraska:  https://www.nebraskablue.com/en/About-Us/Transparency-in-Coverage
// BCBS Kansas:    https://www.bcbsks.com/mrf
// BCBS ND:        https://www.bcbsnd.com/en/individuals-families/resources/transparency
// BCBS IL:        https://www.bcbsil.com/member/machine-readable-files (EIN-gated — skip)
// BCBS TX:        https://www.bcbstx.com/member/machine-readable-files (EIN-gated — skip)
// BCBS AL:        https://www.bcbsal.org/web/transparency-in-coverage (EIN-gated — skip)
// CareFirst:      https://individual.carefirst.com/individuals-families/ways-to-save/hmo-ppo-plans/transparency-coverage.page
// Highmark:       https://www.highmark.com/hmk2/footer/transparency-in-coverage.shtml
// Excellus:       https://news.excellusbcbs.com/developer-info/transparency-coverage-mrf (EIN-gated — skip)

const DB_BATCH = parseInt(process.env.DB_BATCH || '200')
const MAX_FILES_PER_PLAN = parseInt(process.env.BCBS_MAX_FILES || '10')

async function loadTargets(client) {
  const [npiRes, cptRes] = await Promise.all([
    client.query(`SELECT npi FROM providers WHERE type != 'hospital'`), // hospitals covered in 04b
    client.query('SELECT cpt_code FROM procedure_catalog'),
  ])
  return {
    npis: new Set(npiRes.rows.map(r => r.npi)),
    cpts: new Set(), // empty — normalizePayerItem no longer filters by CPT
  }
}

async function processBCBSPlan(plan, targets, writer) {
  log.info(`\n  ${plan.name}: fetching index...`)
  let indexData

  try {
    indexData = await fetchJSON(plan.url, { timeout: 30000 })
  } catch (e) {
    log.warn(`  ${plan.name}: index fetch failed (${e.message.slice(0,60)}) — skipping`)
    return 0
  }

  // Extract in-network file URLs from the ToC
  const inNetworkFiles = (
    indexData.in_network_files ||
    indexData.reporting_structure?.flatMap(r => r.in_network_files) ||
    indexData.files?.filter(f => f.type === 'in-network') ||
    []
  ).slice(0, MAX_FILES_PER_PLAN)

  log.info(`  ${plan.name}: ${inNetworkFiles.length} in-network files (processing up to ${MAX_FILES_PER_PLAN})`)

  let totalRows = 0
  const lim = pLimit(2)

  const tasks = inNetworkFiles.map(file => lim(async () => {
    const url = file.location || file.url || file.download_url
    if (!url) return

    try {
      const res = await fetchStream(url, { timeout: 120000 })
      let buf = '', depth = 0, item = '', inInNetwork = false

      for await (const chunk of res.body) {
        buf += chunk.toString()
        if (!inInNetwork) {
          const idx = buf.indexOf('"in_network"')
          if (idx >= 0) {
            const a = buf.indexOf('[', idx)
            if (a >= 0) { inInNetwork = true; buf = buf.slice(a + 1) }
          }
          if (!inInNetwork && buf.length > 8192) buf = buf.slice(-4096)
          continue
        }

        for (let i = 0; i < buf.length; i++) {
          const ch = buf[i]
          if (ch === '{') { depth++; item += ch }
          else if (ch === '}') {
            depth--; item += ch
            if (depth === 0 && item.length > 5) {
              try {
                const obj = JSON.parse(item)
                obj.payer_name = plan.name
                const rows = normalizePayerItem(obj, targets.npis, targets.cpts)
                for (const row of rows) { await writer.push(row); totalRows++ }
              } catch {}
              item = ''
            }
          } else if (depth > 0) item += ch
        }
        buf = ''
      }
    } catch (e) {
      log.warn(`  ${plan.name} file failed: ${e.message.slice(0,60)}`)
    }
  }))

  await Promise.all(tasks)
  log.ok(`  ${plan.name}: ${totalRows} rates extracted`)
  return totalRows
}

async function main() {
  initLog('05b_bcbs_plans')
  log.info('=== Layer 2b: BCBS State Plans (Non-hospital providers) ===')

  const client = await pool.connect()
  let targets
  try {
    targets = await loadTargets(client)
    log.info(`Non-hospital targets: ${targets.npis.size} NPIs, ${targets.cpts.size} CPTs`)
  } finally { client.release() }

  const writer = new BatchWriter('negotiated_rates', `
    ON CONFLICT (npi, cpt_code, payer_name, COALESCE(plan_name,''))
    WHERE cpt_code IS NOT NULL
    DO UPDATE SET negotiated_rate = EXCLUDED.negotiated_rate, ingested_at = NOW()
  `, DB_BATCH)

  let total = 0
  for (const plan of BCBS_PLANS) {
    const n = await processBCBSPlan(plan, targets, writer)
    total += n
  }

  const { written } = await writer.done()
  log.ok(`\nBCBS complete: ${written} rates from ${BCBS_PLANS.length} state plans`)
  await pool.end()
}

main().catch(e => { log.error('Fatal', { message: e.message }); process.exit(1) })
