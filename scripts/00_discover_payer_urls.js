#!/usr/bin/env node
/**
 * 00_discover_payer_urls.js
 *
 * Verifies that the URLs we actually use in the pipeline are live
 * before starting Layer 2.
 *
 * WHAT WE ACTUALLY SCRAPE:
 *   - UHC via transparency-in-coverage.uhc.com (stable API)
 *   - BCBS state plans via their individual index URLs (05b_bcbs_plans.js)
 *
 * WHAT WE DON'T SCRAPE DIRECTLY:
 *   - Aetna / Cigna / Humana / Anthem for non-hospital providers
 *     Their files are 500GB-1TB with complex access patterns.
 *     Non-hospital providers missing these payers get regional median
 *     estimates from script 06 instead.
 *   - Hospitals don't need any payer scraping — script 04b extracts
 *     ALL payer rates directly from each hospital's own CMS MRF file.
 *
 * If UHC URL is dead:
 *   1. Check CMS registry page
 *   2. If still dead, use Anthropic web search to find current URL
 *   3. Update Neon payer_urls table with verified URL
 *   4. Write verified_payer_urls.json for Layer 2 to read
 */
import 'dotenv/config'
import { pool, log, initLog } from '../lib/db.js'
import { headRequest, fetchJSON } from '../lib/fetch.js'
import { writeFileSync } from 'fs'

const CMS_REGISTRY = 'https://www.cms.gov/healthplan-price-transparency/resources'

// The only URLs we need to verify — what the pipeline actually uses
const URLS_TO_VERIFY = [
  {
    id:           'uhc',
    name:         'United Health Care',
    url:          'https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/',
    search_hint:  'UHC United Healthcare transparency in coverage machine readable file index URL 2025',
    used_by:      '05_layer2_payer_mrfs.js',
  },
]

// BCBS state plan URLs verified separately in 05b_bcbs_plans.js
// Each plan self-reports failure gracefully — no need to pre-verify all 18

async function ensureTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS payer_urls (
      payer_id         VARCHAR(20)  PRIMARY KEY,
      payer_name       TEXT         NOT NULL,
      index_url        TEXT         NOT NULL,
      last_verified_at TIMESTAMPTZ  DEFAULT NOW(),
      last_status      VARCHAR(10)  DEFAULT 'unknown',
      discovery_method VARCHAR(20),
      notes            TEXT
    )
  `)
}

async function findViaWebSearch(entry) {
  const apiKey = process.env.ANTHROPIC_API_KEY
  if (!apiKey) {
    log.warn('  No ANTHROPIC_API_KEY — cannot web search for updated URL')
    return null
  }

  log.info(`  Searching web for current ${entry.name} URL...`)
  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method:  'POST',
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model:      'claude-sonnet-4-6',
        max_tokens: 512,
        tools: [{ type: 'web_search_20250305', name: 'web_search' }],
        messages: [{
          role:    'user',
          content: `Find the current machine-readable file index URL for ${entry.name} price transparency.
Search: ${entry.search_hint}
Return ONLY the direct URL. It must be a working HTTPS URL pointing to a JSON or API endpoint.`,
        }],
      }),
    })

    const data  = await response.json()
    const text  = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join(' ')
    const match = text.match(/https?:\/\/[^\s"'<>]+/i)
    if (match?.[0]) {
      log.ok(`  Web search found: ${match[0]}`)
      return match[0]
    }
    return null
  } catch (e) {
    log.warn(`  Web search failed: ${e.message}`)
    return null
  }
}

async function verifyEntry(entry, client) {
  log.info(`\n── ${entry.name}`)
  log.info(`  Checking: ${entry.url}`)

  // 1. Check stored URL in Neon
  const stored = await client.query(
    'SELECT index_url FROM payer_urls WHERE payer_id = $1', [entry.id]
  )
  const urlToCheck = stored.rows[0]?.index_url || entry.url
  log.info(`  URL: ${urlToCheck.slice(0, 80)}`)

  // 2. HEAD check
  const check = await headRequest(urlToCheck)
  if (check.ok) {
    log.ok(`  ✅ Live (HTTP ${check.status})`)
    await client.query(`
      INSERT INTO payer_urls (payer_id, payer_name, index_url, last_verified_at, last_status, discovery_method)
      VALUES ($1, $2, $3, NOW(), 'ok', 'stored')
      ON CONFLICT (payer_id) DO UPDATE SET
        last_verified_at = NOW(), last_status = 'ok'
    `, [entry.id, entry.name, urlToCheck])
    return { id: entry.id, name: entry.name, url: urlToCheck, ok: true }
  }

  log.warn(`  ❌ Dead (HTTP ${check.status || check.error?.slice(0,50)})`)

  // 3. Web search fallback
  const foundUrl = await findViaWebSearch(entry)
  if (foundUrl) {
    const recheck = await headRequest(foundUrl)
    if (recheck.ok) {
      log.ok(`  ✅ Found working URL via web search`)
      await client.query(`
        INSERT INTO payer_urls (payer_id, payer_name, index_url, last_verified_at, last_status, discovery_method, notes)
        VALUES ($1, $2, $3, NOW(), 'ok', 'web_search', 'Auto-discovered via Anthropic web search')
        ON CONFLICT (payer_id) DO UPDATE SET
          index_url = EXCLUDED.index_url, last_verified_at = NOW(),
          last_status = 'ok', discovery_method = 'web_search', notes = EXCLUDED.notes
      `, [entry.id, entry.name, foundUrl])
      return { id: entry.id, name: entry.name, url: foundUrl, ok: true }
    }
  }

  // 4. Failed — log it, pipeline will skip or use fallback
  log.error(`  ❌ Cannot find working URL for ${entry.name}`)
  log.error(`  Manual fix: go to ${CMS_REGISTRY} and find the current URL`)
  log.error(`  Then update payer_urls in Neon: UPDATE payer_urls SET index_url='[url]' WHERE payer_id='${entry.id}'`)
  await client.query(`
    INSERT INTO payer_urls (payer_id, payer_name, index_url, last_verified_at, last_status, notes)
    VALUES ($1, $2, $3, NOW(), 'dead', 'All discovery methods failed — manual fix needed')
    ON CONFLICT (payer_id) DO UPDATE SET
      last_verified_at = NOW(), last_status = 'dead',
      notes = 'All discovery methods failed — manual fix needed'
  `, [entry.id, entry.name, urlToCheck])
  return { id: entry.id, name: entry.name, url: urlToCheck, ok: false }
}

async function main() {
  initLog('00_discover_payer_urls')
  log.info('=== Payer URL Verification ===')
  log.info('Verifying URLs for scripts that do active payer scraping')
  log.info('(UHC for non-hospital providers + BCBS state plans)')
  log.info('')
  log.info('Hospital payer rates come from hospital MRF files directly (script 04b)')
  log.info('No Aetna/Cigna/Humana/Anthem URL verification needed')

  const client = await pool.connect()
  const results = []

  try {
    await ensureTable(client)

    for (const entry of URLS_TO_VERIFY) {
      const result = await verifyEntry(entry, client)
      results.push(result)
    }

    // Write verified URLs for Layer 2 to read
    const verified = Object.fromEntries(
      results.filter(r => r.ok).map(r => [r.id, r.url])
    )
    writeFileSync('./verified_payer_urls.json', JSON.stringify(verified, null, 2))

    // Summary
    const ok     = results.filter(r => r.ok)
    const failed = results.filter(r => !r.ok)

    log.info('\n=== Summary ===')
    for (const r of results) {
      log.info(`  ${r.ok ? '✅' : '❌'} ${r.name}`)
    }

    if (failed.length) {
      log.warn(`\n${failed.length} URL(s) could not be verified:`)
      for (const f of failed) log.warn(`  - ${f.name}: Layer 2 will skip`)
    } else {
      log.ok('\nAll URLs verified — Layer 2 is ready to run')
    }

  } finally {
    client.release()
    await pool.end()
  }
}

main().catch(e => { log.error('Fatal', { message: e.message }); process.exit(1) })
