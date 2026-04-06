#!/usr/bin/env node
import 'dotenv/config'
import { pool, log, initLog } from '../lib/db.js'

const SCHEMA = `
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ── PROVIDERS ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS providers (
  npi                   VARCHAR(20)  PRIMARY KEY,
  name                  TEXT         NOT NULL,
  type                  VARCHAR(30)  NOT NULL DEFAULT 'hospital',
  owner_system          TEXT,
  address               TEXT,
  city                  TEXT,
  state                 CHAR(2),
  zip                   VARCHAR(10),
  phone                 VARCHAR(20),
  url                   TEXT,
  lat                   NUMERIC(9,6),
  lng                   NUMERIC(9,6),
  data_score            NUMERIC(4,3) DEFAULT 0,
  has_transparency_data BOOLEAN      DEFAULT false,
  service_count         INTEGER      DEFAULT 0,
  last_scraped_at       TIMESTAMPTZ,
  last_scrape_status    VARCHAR(10)  DEFAULT 'pending',
  last_scrape_error     TEXT,
  cms_cert_num          VARCHAR(20),
  source                VARCHAR(30)  DEFAULT 'cms',
  created_at            TIMESTAMPTZ  DEFAULT NOW(),
  updated_at            TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_prov_type      ON providers(type);
CREATE INDEX IF NOT EXISTS idx_prov_state     ON providers(state);
CREATE INDEX IF NOT EXISTS idx_prov_zip       ON providers(zip);
CREATE INDEX IF NOT EXISTS idx_prov_geo       ON providers(lat,lng) WHERE lat IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_prov_score     ON providers(data_score DESC);
CREATE INDEX IF NOT EXISTS idx_prov_scraped   ON providers(last_scraped_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_prov_name_trgm ON providers USING GIN(name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_prov_has_data  ON providers(has_transparency_data) WHERE has_transparency_data;

-- ── PROCEDURE CATALOG ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS procedure_catalog (
  cpt_code         VARCHAR(10) PRIMARY KEY,
  hcpcs_code       VARCHAR(10),
  description      TEXT        NOT NULL,
  category         VARCHAR(60),
  subcategory      VARCHAR(60),
  is_cms_shoppable BOOLEAN     DEFAULT false,
  typical_setting  VARCHAR(20),
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ── SHOPPABLE SERVICES (PRICES) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS shoppable_services (
  id                  UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
  npi                 VARCHAR(20)   NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
  cpt_code            VARCHAR(10),
  hcpcs_code          VARCHAR(10),
  description         TEXT          NOT NULL,
  cash_price          NUMERIC(12,2),
  chargemaster_price  NUMERIC(12,2),
  min_negotiated      NUMERIC(12,2),
  max_negotiated      NUMERIC(12,2),
  avg_negotiated      NUMERIC(12,2),
  payer_name          TEXT,
  plan_name           TEXT,
  billing_class       VARCHAR(20),
  service_code        VARCHAR(20),
  price_type          VARCHAR(20)   DEFAULT 'cash',
  confidence          VARCHAR(10)   DEFAULT 'high',
  effective_date      DATE,
  scraped_at          TIMESTAMPTZ   DEFAULT NOW(),
  source_file_url     TEXT
);

CREATE INDEX IF NOT EXISTS idx_ss_npi         ON shoppable_services(npi);
CREATE INDEX IF NOT EXISTS idx_ss_cpt         ON shoppable_services(cpt_code) WHERE cpt_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ss_hcpcs       ON shoppable_services(hcpcs_code) WHERE hcpcs_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ss_cash        ON shoppable_services(cash_price) WHERE cash_price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ss_negotiated  ON shoppable_services(min_negotiated) WHERE min_negotiated IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ss_payer       ON shoppable_services(payer_name) WHERE payer_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ss_scraped     ON shoppable_services(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_ss_desc_trgm   ON shoppable_services USING GIN(description gin_trgm_ops);

-- Unique constraint: one cash-price row per provider+CPT (for UPSERT)
CREATE UNIQUE INDEX IF NOT EXISTS idx_ss_npi_cpt_cash
  ON shoppable_services(npi, cpt_code)
  WHERE cpt_code IS NOT NULL AND price_type = 'cash';

-- ── PAYER NEGOTIATED RATES ────────────────────────────────────────
-- Separate table for payer-specific negotiated rates (high volume)
CREATE TABLE IF NOT EXISTS negotiated_rates (
  id              UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
  npi             VARCHAR(20)  NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
  cpt_code        VARCHAR(10),
  hcpcs_code      VARCHAR(10),
  description     TEXT,
  negotiated_rate NUMERIC(12,2) NOT NULL,
  payer_name      TEXT         NOT NULL,
  plan_name       TEXT,
  billing_class   VARCHAR(20),
  service_code    VARCHAR(20),
  effective_date  DATE,
  expiration_date DATE,
  source_file_url TEXT,
  ingested_at     TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nr_npi      ON negotiated_rates(npi);
CREATE INDEX IF NOT EXISTS idx_nr_cpt      ON negotiated_rates(cpt_code) WHERE cpt_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nr_payer    ON negotiated_rates(payer_name);
CREATE INDEX IF NOT EXISTS idx_nr_rate     ON negotiated_rates(negotiated_rate);
CREATE INDEX IF NOT EXISTS idx_nr_ingested ON negotiated_rates(ingested_at DESC);

-- Unique: one rate per provider+CPT+payer+plan (UPSERT-safe)
CREATE UNIQUE INDEX IF NOT EXISTS idx_nr_unique
  ON negotiated_rates(npi, cpt_code, payer_name, COALESCE(plan_name,''))
  WHERE cpt_code IS NOT NULL;

-- ── COVERAGE GAPS & LOGS ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS coverage_gaps (
  id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  npi            VARCHAR(20) NOT NULL REFERENCES providers(npi) ON DELETE CASCADE,
  gap_type       VARCHAR(30) NOT NULL,
  error_msg      TEXT,
  file_url       TEXT,
  last_attempted TIMESTAMPTZ DEFAULT NOW(),
  retry_count    INTEGER     DEFAULT 0,
  resolved_at    TIMESTAMPTZ,
  created_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gaps_npi        ON coverage_gaps(npi);
CREATE INDEX IF NOT EXISTS idx_gaps_unresolved ON coverage_gaps(gap_type) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS scrape_log (
  id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  npi             VARCHAR(20) REFERENCES providers(npi) ON DELETE CASCADE,
  job_type        VARCHAR(20) DEFAULT 'hospital_mrf',
  status          VARCHAR(10) NOT NULL,
  rows_parsed     INTEGER     DEFAULT 0,
  rows_inserted   INTEGER     DEFAULT 0,
  file_url        TEXT,
  source_file_url TEXT,
  file_format     VARCHAR(10),
  file_size_bytes BIGINT,
  duration_ms     INTEGER,
  error_msg       TEXT,
  started_at      TIMESTAMPTZ DEFAULT NOW(),
  completed_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_log_npi     ON scrape_log(npi);
CREATE INDEX IF NOT EXISTS idx_log_status  ON scrape_log(status);
CREATE INDEX IF NOT EXISTS idx_log_started ON scrape_log(started_at DESC);
-- Add source_file_url if missing (for existing deployments)
ALTER TABLE scrape_log ADD COLUMN IF NOT EXISTS source_file_url TEXT;

-- ── VIEWS ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW provider_price_summary AS
SELECT
  p.npi, p.name, p.type, p.city, p.state,
  p.data_score, p.service_count, p.last_scraped_at,
  MIN(ss.cash_price)                FILTER (WHERE ss.cash_price IS NOT NULL)  AS cash_low,
  MAX(ss.cash_price)                FILTER (WHERE ss.cash_price IS NOT NULL)  AS cash_high,
  ROUND(AVG(ss.cash_price) FILTER (WHERE ss.cash_price IS NOT NULL)::numeric,2) AS avg_cash,
  COUNT(DISTINCT ss.cpt_code)       FILTER (WHERE ss.cpt_code IS NOT NULL)    AS unique_cpts,
  COUNT(DISTINCT nr.payer_name)     FILTER (WHERE nr.payer_name IS NOT NULL)  AS payer_count
FROM providers p
LEFT JOIN shoppable_services ss ON ss.npi = p.npi
LEFT JOIN negotiated_rates nr   ON nr.npi  = p.npi
GROUP BY p.npi, p.name, p.type, p.city, p.state, p.data_score, p.service_count, p.last_scraped_at;

CREATE OR REPLACE VIEW coverage_stats AS
SELECT
  COUNT(*)                                                    AS total_providers,
  COUNT(*) FILTER (WHERE has_transparency_data)               AS with_data,
  ROUND(100.0 * COUNT(*) FILTER (WHERE has_transparency_data)
        / NULLIF(COUNT(*),0), 1)                              AS coverage_pct,
  (SELECT COUNT(*) FROM shoppable_services)                   AS cash_price_records,
  (SELECT COUNT(*) FROM negotiated_rates)                     AS negotiated_rate_records,
  (SELECT COUNT(DISTINCT payer_name) FROM negotiated_rates)   AS unique_payers,
  (SELECT COUNT(DISTINCT cpt_code) FROM negotiated_rates WHERE cpt_code IS NOT NULL) AS unique_cpts_negotiated,
  ROUND(AVG(data_score)::numeric,3)                           AS avg_data_score
FROM providers;

-- Trigger: keep updated_at fresh
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_providers_updated ON providers;
CREATE TRIGGER trg_providers_updated BEFORE UPDATE ON providers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
`

async function migrate() {
  initLog('01_migrate')
  log.info('Running migration on Neon...')
  const client = await pool.connect()
  try {
    await client.query(SCHEMA)
    const tables = await client.query(
      `SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename`
    )
    log.ok('Migration complete. Tables: ' + tables.rows.map(r => r.tablename).join(', '))
  } catch (e) {
    log.error('Migration failed', { message: e.message })
    process.exit(1)
  } finally {
    client.release()
    await pool.end()
  }
}

migrate()
