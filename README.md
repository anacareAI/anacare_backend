# AnaCare Data Pipeline — Complete Run Instructions

## What you'll have when done
- **~10,000–14,000 providers** (hospitals, ASCs, urgent cares, standalones, labs)
- **~2–5M cash price records** from hospital MRF files (Layer 1)
- **~10–25M payer-specific negotiated rate records** from UHC, Aetna, Anthem, Cigna, Humana (Layer 2)
- **80+ procedures** in the catalog covering all CMS shoppable services
- All of it live in your Neon DB, powering the AnaCare app

---

## Step 1 — Spin up the DigitalOcean droplet (5 min)

### Install doctl on your Mac:
```bash
brew install doctl
doctl auth init    # paste your DO API token from cloud.digitalocean.com/account/api/tokens
```

### Create the droplet:
```bash
chmod +x create_droplet.sh
./create_droplet.sh
# Saves IP to .droplet_ip — takes ~2 min
```

### SSH in:
```bash
ssh -i ~/.ssh/anacare_pipeline root@$(cat .droplet_ip)
```

---

## Step 2 — Bootstrap the server (3 min, run ON the droplet)

```bash
# On the droplet:
curl -o bootstrap.sh https://raw.githubusercontent.com/anacareAI/anacare_product/main/bootstrap.sh
bash bootstrap.sh
```

OR just paste the contents of bootstrap.sh directly.

---

## Step 3 — Upload pipeline files (from your local machine)

```bash
# From your local machine, in the anacare-pipeline directory:
scp -i ~/.ssh/anacare_pipeline -r . root@$(cat .droplet_ip):/opt/anacare-pipeline/
```

---

## Step 4 — Install dependencies (on the droplet)

```bash
cd /opt/anacare-pipeline
npm install
```

---

## Step 5 — Run the pipeline inside screen (so it survives SSH disconnect)

```bash
screen -S anacare     # Start a persistent session

# Run everything:
npm run full-pipeline

# OR run step by step (recommended for first run):
npm run migrate           # ~5s   — creates schema on Neon
npm run seed-catalog      # ~2s   — seeds 80+ CPT codes
npm run ingest-providers  # ~20min — loads all US facilities
npm run layer1            # ~6hrs  — hospital MRF cash prices
npm run layer2            # ~12-48hrs — payer negotiated rates

# Detach from screen: Ctrl+A then D
# Reattach later:     screen -r anacare
```

---

## Step 6 — Monitor progress

```bash
# In a separate SSH session, check stats anytime:
cd /opt/anacare-pipeline && npm run stats

# Watch the log:
tail -f logs/04_layer1_*.log
tail -f logs/05_layer2_*.log
```

---

## Step 7 — Point the app at real data

In your frontend `.env`:
```
VITE_USE_MOCK=false
VITE_API_URL=https://your-api.com/api
```

In your API `.env`:
```
DATABASE_URL=postgresql://neondb_owner:npg_iQT5WNZ3ADLE@ep-fragrant-wave-ansbjsi8-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require
```

---

## Step 8 — Destroy the droplet when done (save money)

Layer 1+2 are one-time bulk loads. After that, re-scraping runs weekly from a cron job or n8n — you don't need the $48/mo droplet running 24/7.

```bash
# From your local machine:
DROPLET_ID=$(doctl compute droplet list --format ID,Name --no-header | grep anacare-pipeline | awk '{print $1}')
doctl compute droplet delete $DROPLET_ID --force
echo "Droplet destroyed. Neon DB keeps all the data."
```

---

## Resume after crash

The pipeline is fully resume-safe. If anything crashes, just re-run the same step:

```bash
npm run resume-layer1   # Skips providers already scraped in last 30 days
npm run resume-layer2   # Skips payers that already have 100k+ rows in DB
```

---

## Estimated timeline

| Step | Time | Output |
|------|------|--------|
| migrate + seed | 1 min | Schema + 80 CPTs |
| ingest-providers | 20-40 min | 10,000-14,000 providers |
| layer1 | 4-8 hours | 2-5M cash price records |
| layer2 (UHC only) | 6-12 hours | 5M+ negotiated rates |
| layer2 (all 5 payers) | 24-48 hours | 15-25M negotiated rates |

---

## Cost

| Resource | Cost |
|----------|------|
| DigitalOcean 4vCPU/8GB (s-4vcpu-8gb) | $0.071/hr = ~$3-$7 total for Layer 1+2 |
| Neon Postgres | Free tier handles up to 3GB; upgrade to $19/mo Pro for 25M+ rows |

**Total estimated cost: $5-25** for the full dataset loaded once.

---

## What happens if a payer index URL changes?

Payer MRF index URLs are publicly mandated by CMS but do change occasionally. If a Layer 2 URL fails:
1. Check the payer's website for their current MRF index URL
2. Update the `PAYERS` array in `scripts/05_layer2_payer_mrfs.js`
3. Re-run `npm run layer2`

Current verified URLs are in the script. CMS also maintains a list of all payer MRF index URLs at:
https://www.cms.gov/healthplan-price-transparency/resources
