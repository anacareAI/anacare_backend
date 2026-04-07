import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import pg from 'pg'

const { Pool } = pg
const pool = new Pool({ connectionString: process.env.DATABASE_URL })

const app = express()
app.use(cors())
app.use(express.json())

const PORT = process.env.PORT || 3000

// GET /api/health
app.get('/api/health', async (req, res) => {
  try {
    const [h, s, n] = await Promise.all([
      pool.query('SELECT COUNT(DISTINCT npi)::int AS c FROM shoppable_services'),
      pool.query('SELECT COUNT(*)::int AS c FROM shoppable_services'),
      pool.query('SELECT COUNT(*)::int AS c FROM negotiated_rates'),
    ])
    res.json({
      status: 'ok',
      hospitals_with_prices: h.rows[0].c,
      ss_rows: s.rows[0].c,
      nr_rows: n.rows[0].c,
    })
  } catch (e) { res.status(500).json({ error: e.message }) }
})

// GET /api/procedures/search?q=MRI
app.get('/api/procedures/search', async (req, res) => {
  const q = req.query.q
  if (!q) return res.status(400).json({ error: 'q param required' })
  try {
    const r = await pool.query(`
      SELECT DISTINCT cpt_code, description,
        COUNT(*) OVER (PARTITION BY cpt_code) hospital_count
      FROM shoppable_services
      WHERE description ILIKE $1 AND cpt_code IS NOT NULL
      ORDER BY hospital_count DESC, cpt_code
      LIMIT 20
    `, ['%' + q + '%'])
    res.json(r.rows)
  } catch (e) { res.status(500).json({ error: e.message }) }
})

// GET /api/providers?state=PA&cpt=70553
app.get('/api/providers', async (req, res) => {
  const { state, cpt } = req.query
  if (!state || !cpt) return res.status(400).json({ error: 'state and cpt params required' })
  try {
    const r = await pool.query(`
      SELECT p.npi, p.name, p.city, p.state,
        ss.description, ss.cash_price,
        ss.min_negotiated, ss.max_negotiated,
        COUNT(DISTINCT nr.payer_name) payer_count
      FROM providers p
      JOIN shoppable_services ss ON ss.npi = p.npi
      LEFT JOIN negotiated_rates nr
        ON nr.npi = p.npi AND nr.cpt_code = ss.cpt_code
      WHERE p.state = $1 AND ss.cpt_code = $2 AND ss.cpt_code IS NOT NULL
      GROUP BY p.npi, p.name, p.city, p.state,
        ss.description, ss.cash_price,
        ss.min_negotiated, ss.max_negotiated
      ORDER BY ss.cash_price ASC NULLS LAST
      LIMIT 20
    `, [state, cpt])
    res.json(r.rows)
  } catch (e) { res.status(500).json({ error: e.message }) }
})

// GET /api/compare?cpt=70553&state=PA&payer=Aetna
app.get('/api/compare', async (req, res) => {
  const { cpt, state, payer } = req.query
  if (!cpt || !state || !payer) return res.status(400).json({ error: 'cpt, state and payer params required' })
  try {
    const r = await pool.query(`
      SELECT p.npi, p.name, p.city, p.state,
        ss.description, ss.cash_price,
        nr.negotiated_rate, nr.payer_name, nr.plan_name
      FROM providers p
      JOIN shoppable_services ss ON ss.npi = p.npi
      JOIN negotiated_rates nr
        ON nr.npi = ss.npi AND nr.cpt_code = ss.cpt_code
      WHERE ss.cpt_code = $1
        AND ss.cpt_code IS NOT NULL
        AND p.state = $2
        AND nr.payer_name ILIKE $3
      ORDER BY nr.negotiated_rate ASC
      LIMIT 20
    `, [cpt, state, '%' + payer + '%'])
    res.json(r.rows)
  } catch (e) { res.status(500).json({ error: e.message }) }
})

app.listen(PORT, () => console.log(`API listening on :${PORT}`))
