# AnaCare Pipeline

## What this is
Healthcare price transparency pipeline. Downloads hospital MRF files,
parses prices, stores in Neon Postgres for the AnaCare frontend.

## Database
Neon Postgres — see .env for DATABASE_URL
Tables: providers, shoppable_services, negotiated_rates, 
        procedure_catalog, scrape_log

## Current state
- 28,619 providers ingested from NPPES
- 257 hospitals have MRF URLs (from TPAFS CSV)
- 1 hospital fully ingested (UPMC Presbyterian via Abbott Northwestern NPI)
  - 9,309 rows in shoppable_services
  - 161,634 rows in negotiated_rates
  - 29 distinct payers

## Taxonomy codes in scope
282N00000X — General Acute Care Hospital
282NC0060X — Critical Access Hospital
282NC2000X — Children's Hospital
261QS0132X — Ambulatory Surgery Center (voluntary MRFs)

## Immediate next tasks
1. Update 03_ingest_providers.js to use only these 4 taxonomy codes
2. Delete standalone providers from DB
3. Run enrich_mrf_urls.mjs to populate providers.url
4. Run LAYER1_LIMIT=10 node scripts/04_layer1_hospital_mrfs.js
5. Build refresh_stale_urls.mjs using Anthropic API web search
6. Build Express API on Railway (4 endpoints — see architecture)
7. Provision DigitalOcean for full pipeline run

## Key pipeline flow
NPPES (4 queries) → providers table
CMS dataset → providers.cms_cert_num (CCN)
TPAFS CSV → providers.url (MRF URLs)
04_layer1 → shoppable_services + negotiated_rates

## Core product query
SELECT p.name, p.city, ss.description, ss.cash_price, nr.negotiated_rate
FROM shoppable_services ss
JOIN providers p ON p.npi = ss.npi
JOIN negotiated_rates nr ON nr.npi = ss.npi AND nr.cpt_code = ss.cpt_code
WHERE ss.description ILIKE '%MRI%brain%'
  AND nr.payer_name ILIKE '%aetna%'
  AND p.state = 'IL'
ORDER BY nr.negotiated_rate ASC;

## MRF format
CMS v2 wide CSV (mandatory since July 2024)
Row 0: metadata headers
Row 1: metadata values  
Row 2: real column headers (description, code|1, standard_charge|gross, etc.)
Row 3+: price data — one row per payer per procedure

## Known issues
- Advocate Health URLs are 404 (stale in TPAFS)
- AHN Emerus URLs are 404
- ~4,000 hospitals have no URL yet — need refresh_stale_urls.mjs
- standalones not yet deleted from DB