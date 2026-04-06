#!/usr/bin/env node
import 'dotenv/config'
import { pool } from '../lib/db.js'

async function stats() {
  const c = await pool.connect()
  try {
    console.log('\n╔══════════════════════════════════════════════════╗')
    console.log('║           AnaCare Database Stats                  ║')
    console.log('╚══════════════════════════════════════════════════╝\n')

    const cov = await c.query(`SELECT * FROM coverage_stats`)
    console.log('── Coverage Summary ──')
    console.table(cov.rows)

    const byType = await c.query(`
      SELECT type, COUNT(*) total,
        COUNT(*) FILTER (WHERE has_transparency_data) with_data,
        ROUND(AVG(service_count)::numeric,0) avg_services,
        ROUND(AVG(data_score)::numeric,3) avg_score
      FROM providers GROUP BY type ORDER BY total DESC
    `)
    console.log('\n── By Facility Type ──')
    console.table(byType.rows)

    const topProcs = await c.query(`
      SELECT ss.cpt_code, pc.description,
        COUNT(DISTINCT ss.npi) facilities,
        ROUND(MIN(ss.cash_price)::numeric,2) min_cash,
        ROUND(AVG(ss.cash_price)::numeric,2) avg_cash,
        ROUND(MAX(ss.cash_price)::numeric,2) max_cash
      FROM shoppable_services ss
      LEFT JOIN procedure_catalog pc ON pc.cpt_code = ss.cpt_code
      WHERE ss.cash_price IS NOT NULL
      GROUP BY ss.cpt_code, pc.description
      ORDER BY facilities DESC LIMIT 20
    `)
    console.log('\n── Top 20 Procedures by Facility Count ──')
    console.table(topProcs.rows)

    const payers = await c.query(`
      SELECT payer_name,
        COUNT(*) rows,
        COUNT(DISTINCT npi) providers,
        COUNT(DISTINCT cpt_code) cpts,
        ROUND(AVG(negotiated_rate)::numeric,2) avg_rate
      FROM negotiated_rates GROUP BY payer_name ORDER BY rows DESC
    `)
    if (payers.rows.length) {
      console.log('\n── Payer Negotiated Rates ──')
      console.table(payers.rows)
    }

    const pipe = await c.query(`
      SELECT job_type, status, COUNT(*) count,
        ROUND(AVG(rows_inserted)::numeric,0) avg_rows,
        ROUND(AVG(duration_ms/1000.0)::numeric,1) avg_sec
      FROM scrape_log WHERE started_at > NOW()-INTERVAL '7 days'
      GROUP BY job_type, status ORDER BY count DESC
    `)
    if (pipe.rows.length) {
      console.log('\n── Pipeline Health (7d) ──')
      console.table(pipe.rows)
    }

    console.log('\n✅ Stats complete.\n')
  } finally {
    c.release()
    await pool.end()
  }
}

stats().catch(e => { console.error(e.message); process.exit(1) })
