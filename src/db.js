import 'dotenv/config'
import pg from 'pg'

const { Pool } = pg

if (!process.env.DATABASE_URL) {
  console.error('❌  DATABASE_URL is not set. Copy .env.example → .env and fill it in.')
  process.exit(1)
}

export const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
})

pool.on('error', (err) => console.error('Pool error:', err.message))

export async function q(text, params) {
  const client = await pool.connect()
  try {
    return await client.query(text, params)
  } finally {
    client.release()
  }
}

// Batch upsert helper — splits large arrays into chunks
export async function batchUpsert(sql, rows, chunkSize = 500) {
  let total = 0
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize)
    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      for (const row of chunk) {
        await client.query(sql, row)
      }
      await client.query('COMMIT')
      total += chunk.length
    } catch (err) {
      await client.query('ROLLBACK')
      throw err
    } finally {
      client.release()
    }
  }
  return total
}

export default pool
