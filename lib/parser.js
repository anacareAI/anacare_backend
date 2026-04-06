import { parse as csvParse } from 'csv-parse'
import { createGunzip } from 'zlib'
import { Readable } from 'stream'
import split2 from 'split2'

// ── CMS MRF JSON normalization ────────────────────────────────────
// Handles v1 (pre-2024) and v2 (2024+) JSON schemas
export function normalizeMRFItem(item, npi, sourceUrl) {
  const rows = []
  const description = (item.description || item.charge_description || item.item || '').slice(0, 500)

  // Extract billing codes
  let cptCode = null, hcpcsCode = null
  const codes = item.billing_code_information || item.billing_codes || []
  for (const bc of (Array.isArray(codes) ? codes : [])) {
    if (!bc) continue
    const t = (bc.billing_code_type || bc.type || '').toUpperCase()
    const v = (bc.billing_code || bc.code || '').replace(/[^A-Z0-9]/gi, '').toUpperCase()
    if (!cptCode   && t === 'CPT')   cptCode   = v.slice(0, 10)
    if (!hcpcsCode && t === 'HCPCS') hcpcsCode = v.slice(0, 10)
  }
  // Fallback: top-level billing_code field
  if (!cptCode && !hcpcsCode && item.billing_code) {
    const t = (item.billing_code_type || '').toUpperCase()
    const v = (item.billing_code || '').replace(/[^A-Z0-9]/gi, '').toUpperCase()
    if (t === 'CPT')   cptCode   = v.slice(0, 10)
    if (t === 'HCPCS') hcpcsCode = v.slice(0, 10)
  }

  const charges = item.standard_charges || item.charges || []
  for (const sc of (Array.isArray(charges) ? charges : [charges])) {
    if (!sc) continue
    const setting = (sc.setting || sc.charge_type || sc.type || '').toLowerCase()

    // v2 JSON schema (setting = 'inpatient' | 'outpatient' | 'both')
    if (!setting || ['inpatient','outpatient','both'].some(k => setting === k)) {
      const gross = sanitize(sc.gross_charge)
      const cash  = sanitize(sc.discounted_cash)
      const minN  = sanitize(sc.minimum_negotiated_rate ?? sc.min)
      const maxN  = sanitize(sc.maximum_negotiated_rate ?? sc.max)

      if (gross) rows.push(base(npi, cptCode, hcpcsCode, description, sourceUrl, {
        chargemaster_price: gross, price_type: 'chargemaster', confidence: 'medium'
      }))
      if (cash) rows.push(base(npi, cptCode, hcpcsCode, description, sourceUrl, {
        cash_price: cash, price_type: 'cash', confidence: 'high'
      }))
      if (minN || maxN) rows.push(base(npi, cptCode, hcpcsCode, description, sourceUrl, {
        min_negotiated: minN, max_negotiated: maxN,
        avg_negotiated: avg(minN, maxN), price_type: 'negotiated', confidence: 'low'
      }))
      for (const p of (sc.payers_information || [])) {
        const rate = sanitize(p.negotiated_rate ?? p.estimated_allowed_amount)
        if (!rate) continue
        rows.push(base(npi, cptCode, hcpcsCode, description, sourceUrl, {
          min_negotiated: rate, max_negotiated: rate, avg_negotiated: rate,
          payer_name: (p.payer_name || 'Unknown').slice(0, 100),
          plan_name:  (p.plan_name || '').slice(0, 100) || null,
          price_type: 'negotiated', confidence: 'high',
        }))
      }
      continue
    }

    // v1 JSON schema (setting = 'cash' | 'gross_charge' | 'negotiated' | etc.)
    const val = sanitize(sc.price ?? sc.gross_charge ?? sc.negotiated_rate ?? sc.minimum)
    if (!val) continue

    if (['cash','self-pay','self pay','uninsured','discounted_cash','discounted cash'].some(k => setting.includes(k))) {
      rows.push(base(npi, cptCode, hcpcsCode, description, sourceUrl, {
        cash_price: val, price_type: 'cash', confidence: 'high'
      }))
    } else if (['gross','chargemaster','gross_charge'].some(k => setting.includes(k))) {
      rows.push(base(npi, cptCode, hcpcsCode, description, sourceUrl, {
        chargemaster_price: val, price_type: 'chargemaster', confidence: 'medium'
      }))
    } else if (sc.payers_information || setting.includes('negotiat') || setting.includes('payer')) {
      const payers = sc.payers_information || [{ payer_name: sc.payer_name || 'Unknown', negotiated_rate: val }]
      for (const p of payers) {
        const rate = sanitize(p.negotiated_rate ?? p.rate ?? val)
        if (!rate) continue
        rows.push(base(npi, cptCode, hcpcsCode, description, sourceUrl, {
          min_negotiated: rate, max_negotiated: rate, avg_negotiated: rate,
          payer_name: (p.payer_name || 'Unknown').slice(0, 100),
          plan_name:  (p.plan_name || '').slice(0, 100) || null,
          price_type: 'negotiated', confidence: 'high',
        }))
      }
    }
  }

  return rows.filter(r => r.cash_price || r.chargemaster_price || r.min_negotiated)
}

// ── Payer MRF normalizer ─────────────────────────────────────────
export function normalizePayerItem(item, targetNPIs, targetCPTs) {
  const rows = []
  const billingCode = (item.billing_code || '').toUpperCase().replace(/[^A-Z0-9]/g,'')
  const billingType = (item.billing_code_type || '').toUpperCase()
  const description = (item.description || item.name || '').slice(0, 500)
  const isCPT   = billingType === 'CPT'
  const isHCPCS = billingType === 'HCPCS'
  if (!isCPT && !isHCPCS) return rows
  const cptCode   = isCPT   ? billingCode : null
  const hcpcsCode = isHCPCS ? billingCode : null

  for (const rate of (item.negotiated_rates || [])) {
    const npiArr = Array.isArray(rate.provider_references) ? rate.provider_references : [rate.provider_references]
    for (const np of (rate.negotiated_prices || [])) {
      const val = sanitize(np.negotiated_rate ?? np.rate)
      if (!val) continue
      for (const npiRaw of npiArr) {
        const npi = String(npiRaw || '').trim()
        if (!npi || npi.length < 6) continue
        if (targetNPIs.size > 0 && !targetNPIs.has(npi)) continue
        rows.push({
          npi, cpt_code: cptCode, hcpcs_code: hcpcsCode,
          description: description || 'Unknown',
          cash_price: null, chargemaster_price: null,
          min_negotiated: val, max_negotiated: val, avg_negotiated: val,
          payer_name: (item.payer_name || '').slice(0, 100),
          plan_name: (np.plan_name || item.plan_name || '').slice(0, 100) || null,
          billing_class: (np.billing_class || '').slice(0, 20) || null,
          service_code: (np.service_code || '').slice(0, 20) || null,
          price_type: 'negotiated', confidence: 'high',
          effective_date: np.expiration_date || null,
          source_file_url: null,
        })
      }
    }
  }
  return rows
}

// ── CSV row parser ───────────────────────────────────────────────
// Handles CMS v2 wide format column names exactly as they appear in real files.
// From UPMC Presbyterian real file (2026):
//   description, code|1, code|1|type, code|2, code|2|type, ...
//   billing_class, setting, drug_unit_of_measurement, drug_type_of_measurement
//   standard_charge|gross, standard_charge|discounted_cash
//   standard_charge|min, standard_charge|max
//   [payer_name cols repeat per payer]
export function parseCSVRow(row, headers, npi, sourceUrl) {
  // Find key column values
  const description = (row['description'] || row['item_or_service'] || row['charge description'] || row['charge_description'] || '').slice(0, 500)

  // Billing code: v2 uses 'code|1' with type in 'code|1|type'
  let cptCode = null, hcpcsCode = null
  const code1 = (row['code|1'] || row['billing_code'] || row['cpt_code'] || row['cpt'] || '').replace(/[^A-Z0-9]/gi, '').toUpperCase().slice(0, 10)
  const code1type = (row['code|1|type'] || row['billing_code_type'] || '').toUpperCase()
  if (code1) {
    if (code1type === 'HCPCS') hcpcsCode = code1
    else cptCode = code1
  }
  // Check additional code columns
  for (let i = 2; i <= 4; i++) {
    const c = (row[`code|${i}`] || '').replace(/[^A-Z0-9]/gi, '').toUpperCase().slice(0, 10)
    const t = (row[`code|${i}|type`] || '').toUpperCase()
    if (c && t === 'HCPCS' && !hcpcsCode) hcpcsCode = c
    else if (c && t === 'CPT' && !cptCode) cptCode = c
  }

  // Standard charges
  const gross = sanitize(
    row['standard_charge|gross'] ??
    row['standard_charge | gross'] ??
    row['gross charge'] ?? row['gross_charge'] ?? row['chargemaster']
  )
  const cash = sanitize(
    row['standard_charge|discounted_cash'] ??
    row['standard_charge | discounted_cash'] ??
    row['discounted_cash'] ?? row['cash price'] ?? row['cash_price'] ?? row['self_pay']
  )
  const minN = sanitize(
    row['standard_charge|min'] ??
    row['standard_charge | min'] ??
    row['min_negotiated'] ?? row['deidentified_min_allowed']
  )
  const maxN = sanitize(
    row['standard_charge|max'] ??
    row['standard_charge | max'] ??
    row['max_negotiated'] ?? row['deidentified_max_allowed']
  )

  // Payer-specific negotiated rate (v2 wide format)
  const payerName = (row['payer_name'] || '').trim() || null
  const planName  = (row['plan_name'] || '').trim() || null
  const negotiatedDollar = sanitize(
    row['standard_charge|negotiated_dollar'] ??
    row['standard_charge | negotiated_dollar']
  )

  if (!description && !cptCode && !hcpcsCode) return null
  if (!cash && !gross && !minN && !maxN && !negotiatedDollar) return null

  return base(npi, cptCode, hcpcsCode, description || 'Unknown', sourceUrl, {
    cash_price:         cash,
    chargemaster_price: gross,
    min_negotiated:     negotiatedDollar || minN,
    max_negotiated:     maxN,
    avg_negotiated:     avg(negotiatedDollar || minN, maxN),
    payer_name:         payerName,
    plan_name:          planName,
    price_type:  cash ? 'cash' : negotiatedDollar ? 'negotiated' : gross ? 'chargemaster' : 'negotiated',
    confidence:  cash ? 'high' : negotiatedDollar ? 'high' : gross ? 'medium' : 'low',
  })
}

// ── Helpers ──────────────────────────────────────────────────────
function sanitize(v) {
  if (v == null || v === '' || v === 'N/A' || v === '-' || v === 'Not Available') return null
  const n = parseFloat(String(v).replace(/[$,\s]/g, ''))
  return isFinite(n) && n > 0 && n < 10_000_000 ? Math.round(n * 100) / 100 : null
}

function avg(a, b) {
  if (a && b) return (a + b) / 2
  return a || b || null
}

function base(npi, cptCode, hcpcsCode, description, sourceUrl, extra) {
  return {
    npi,
    cpt_code:           cptCode,
    hcpcs_code:         hcpcsCode,
    description,
    cash_price:         null,
    chargemaster_price: null,
    min_negotiated:     null,
    max_negotiated:     null,
    avg_negotiated:     null,
    payer_name:         null,
    plan_name:          null,
    price_type:         'cash',
    confidence:         'medium',
    effective_date:     null,
    source_file_url:    sourceUrl,
    ...extra,
  }
}

export function scoreProvider(rows) {
  if (!rows.length) return 0
  const withCash = rows.filter(r => r.cash_price > 0).length
  const withCode = rows.filter(r => r.cpt_code || r.hcpcs_code).length
  return Math.min(1, Math.round((
    (withCash / rows.length) * 0.4 +
    (withCode / rows.length) * 0.3 +
    (rows.length >= 300 ? 1 : rows.length / 300) * 0.3
  ) * 1000) / 1000)
}

export function parseCSVRowLegacy(row, headers, npi, sourceUrl) {
  return parseCSVRow(row, headers, npi, sourceUrl)
}
