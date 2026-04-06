#!/usr/bin/env node
/**
 * 06_fill_estimated_prices.js
 *
 * Fills every facility that has no real price data with estimated prices
 * computed from regional medians. Falls back statewide then national.
 *
 * Stored with confidence = 'estimated' so UI shows a clear disclaimer.
 * Never overwrites real prices.
 *
 * Run AFTER Layer 1 + Layer 2.
 */
import 'dotenv/config'
import { pool, log, initLog, BatchWriter, Progress } from '../lib/db.js'

const RADIUS_MILES = 50
const MIN_SOURCES  = 3
const DB_BATCH     = parseInt(process.env.DB_BATCH || '100')

async function main() {
  initLog('06_fill_estimated')
  log.info('=== Script 06: Fill Estimated Prices ===')
  log.info(`Radius: ${RADIUS_MILES}mi | Min sources: ${MIN_SOURCES}`)

  const client = await pool.connect()
  let totalWritten = 0, totalFacilities = 0

  try {
    // CPTs to fill
    const cpts = (await client.query(
      'SELECT cpt_code, description FROM procedure_catalog ORDER BY cpt_code'
    )).rows

    // Facilities needing fill: no real prices at all
    const facilities = (await client.query(`
      SELECT p.npi, p.name, p.type, p.lat, p.lng, p.city, p.state
      FROM providers p
      WHERE p.lat IS NOT NULL AND p.lng IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM shoppable_services ss
          WHERE ss.npi = p.npi AND ss.confidence != 'estimated'
        )
      ORDER BY p.type, p.state
    `)).rows

    log.info(`CPTs: ${cpts.length} | Facilities needing fill: ${facilities.length}`)
    if (!facilities.length) { log.ok('Nothing to fill.'); return }

    const prog = new Progress(facilities.length, 'Fill')

    const writer = new BatchWriter('shoppable_services', `
      ON CONFLICT (npi, cpt_code) WHERE cpt_code IS NOT NULL AND price_type = 'cash'
      DO NOTHING
    `, DB_BATCH)

    for (const fac of facilities) {
      let filledCount = 0

      for (const { cpt_code, description } of cpts) {
        // 1. Regional median (50 miles, same type)
        let row = (await client.query(`
          SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ss.cash_price) AS median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ss.cash_price) AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ss.cash_price) AS p75,
            COUNT(*) AS n
          FROM shoppable_services ss
          JOIN providers p ON p.npi = ss.npi
          WHERE ss.cpt_code = $1 AND p.type = $2
            AND ss.cash_price > 0 AND ss.confidence != 'estimated'
            AND p.lat IS NOT NULL
            AND (3959 * acos(LEAST(1.0,
              cos(radians($3))*cos(radians(p.lat))*cos(radians(p.lng)-radians($4))
              + sin(radians($3))*sin(radians(p.lat))
            ))) <= $5
        `, [cpt_code, fac.type, fac.lat, fac.lng, RADIUS_MILES])).rows[0]

        let scope = 'regional'

        // 2. Statewide fallback
        if (!row?.median || parseInt(row.n) < MIN_SOURCES) {
          row = (await client.query(`
            SELECT
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ss.cash_price) AS median,
              PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ss.cash_price) AS p25,
              PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ss.cash_price) AS p75,
              COUNT(*) AS n
            FROM shoppable_services ss
            JOIN providers p ON p.npi = ss.npi
            WHERE ss.cpt_code = $1 AND p.type = $2 AND p.state = $3
              AND ss.cash_price > 0 AND ss.confidence != 'estimated'
          `, [cpt_code, fac.type, fac.state])).rows[0]
          scope = 'statewide'
        }

        // 3. National fallback
        if (!row?.median || parseInt(row.n) < MIN_SOURCES) {
          row = (await client.query(`
            SELECT
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ss.cash_price) AS median,
              COUNT(*) AS n
            FROM shoppable_services ss
            JOIN providers p ON p.npi = ss.npi
            WHERE ss.cpt_code = $1 AND p.type = $2
              AND ss.cash_price > 0 AND ss.confidence != 'estimated'
          `, [cpt_code, fac.type])).rows[0]
          scope = 'national'
        }

        if (!row?.median) continue

        await writer.push({
          npi:               fac.npi,
          cpt_code:          cpt_code,
          hcpcs_code:        null,
          description:       description,
          cash_price:        Math.round(parseFloat(row.median) * 100) / 100,
          chargemaster_price: null,
          min_negotiated:    row.p25 ? Math.round(parseFloat(row.p25) * 100) / 100 : null,
          max_negotiated:    row.p75 ? Math.round(parseFloat(row.p75) * 100) / 100 : null,
          avg_negotiated:    null,
          payer_name:        null,
          plan_name:         null,
          price_type:        'cash',
          confidence:        'estimated',
          effective_date:    null,
          source_file_url:   `estimated:${scope}:n${row.n}`,
        })
        filledCount++
      }

      if (filledCount > 0) {
        await client.query(`
          UPDATE providers SET
            has_transparency_data = true,
            service_count = $1,
            data_score = 0.25,
            last_scraped_at = NOW(),
            last_scrape_status = 'estimated'
          WHERE npi = $2
        `, [filledCount, fac.npi])
        totalFacilities++
        totalWritten += filledCount
      }

      prog.tick('ok')
    }

    const { written } = await writer.done()
    log.ok(`Done: ${written} estimated prices for ${totalFacilities} facilities`)

    const breakdown = await client.query(`
      SELECT confidence, COUNT(*) count
      FROM shoppable_services GROUP BY confidence ORDER BY count DESC
    `)
    console.log('\nPrice confidence breakdown:')
    console.table(breakdown.rows)

  } finally {
    client.release()
    await pool.end()
  }
}

main().catch(e => { log.error('Fatal', { message: e.message }); process.exit(1) })
