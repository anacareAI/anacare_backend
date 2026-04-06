#!/usr/bin/env node
/**
 * 00_run_all.js — Full pipeline v5
 *
 * EXPANDED APPROACH: Every CPT code from every hospital file is ingested.
 * procedure_catalog is built dynamically from real hospital data,
 * not pre-filtered to a fixed list.
 *
 * Pipeline order:
 * 1.  Migrate schema (procedure_catalog starts empty)
 * 2.  Ingest all US providers (CMS hospitals + NPPES ASCs/UCs)
 * 3.  Layer 1a: Stream every hospital MRF file
 *     → writes ALL procedures to shoppable_services
 *     → upserts every CPT/HCPCS code seen into procedure_catalog
 * 4.  Enrich catalog: add canonical descriptions + CMS shoppable flags
 *     to the codes Layer 1 discovered (02_seed_catalog.js)
 * 5.  Layer 1b: Extract ALL payer rates from hospital MRF files
 *     → Aetna, BCBS, Cigna, UHC, Humana, Anthem etc. for every hospital
 * 6.  Verify payer URLs (UHC + BCBS) before scraping
 * 7.  Layer 2a: UHC negotiated rates for non-hospital providers
 * 8.  Layer 2b: BCBS state plans for non-hospital providers
 * 9.  Fill estimated prices for facilities with no real data
 * 10. Final stats
 */
import { execSync } from 'child_process'

const STEPS = [
  { script: '01_migrate.js',
    label:    'Schema migration',
    critical: true },

  { script: '03_ingest_providers.js',
    label:    'Provider ingestion — CMS hospitals + NPPES',
    critical: false },

  { script: '04_layer1_hospital_mrfs.js',
    label:    'Layer 1a — Hospital cash prices + build procedure catalog',
    critical: false },

  { script: '02_seed_catalog.js',
    label:    'Enrich procedure catalog — canonical names + CMS shoppable flags',
    critical: false },

  { script: '04b_extract_payer_rates_from_hospital_mrfs.js',
    label:    'Layer 1b — All payer rates from hospital MRF files',
    critical: false },

  { script: '00_discover_payer_urls.js',
    label:    'Verify payer URLs (UHC + BCBS)',
    critical: false },

  { script: '05_layer2_payer_mrfs.js',
    label:    'Layer 2a — UHC rates for non-hospital providers',
    critical: false },

  { script: '05b_bcbs_plans.js',
    label:    'Layer 2b — BCBS state plans for non-hospital providers',
    critical: false },

  { script: '06_fill_estimated_prices.js',
    label:    'Fill estimated prices for facilities with no real data',
    critical: false },

  { script: '99_stats.js',
    label:    'Final stats',
    critical: false },
]

console.log('\n╔══════════════════════════════════════════════════════════════╗')
console.log('║       AnaCare Full Data Pipeline v5.0                        ║')
console.log('║  Expanded: every CPT from every hospital, all payers         ║')
console.log('╚══════════════════════════════════════════════════════════════╝\n')

for (let i = 0; i < STEPS.length; i++) {
  const { script, label, critical } = STEPS[i]
  console.log(`\n── Step ${i + 1}/${STEPS.length}: ${label}`)
  const start = Date.now()
  try {
    execSync(`node scripts/${script}`, {
      stdio:   'inherit',
      cwd:     process.cwd(),
      timeout: 24 * 60 * 60 * 1000,
      env:     { ...process.env },
    })
    const secs = Math.round((Date.now() - start) / 1000)
    console.log(`✅ Done in ${secs}s`)
  } catch (e) {
    console.error(`❌ Failed: ${e.message?.slice(0, 100)}`)
    if (critical) {
      console.error('Critical step failed — stopping pipeline.')
      process.exit(1)
    }
    console.log('Non-critical — continuing to next step...')
  }
}

console.log('\n╔══════════════════════════════════════════════════════════════╗')
console.log('║  Pipeline complete!                                           ║')
console.log('╚══════════════════════════════════════════════════════════════╝')
