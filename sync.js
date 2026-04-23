// ============================================================
// sync.js — MFapi.in → Supabase Sync
//
// Tables filled:
//   ✅ mf_schemes     — scheme code, name, fund_house, type, category, ISIN
//   ✅ mf_latest_nav  — latest NAV + date per scheme
//   ✅ mf_nav_history — daily NAV history (for specific scheme codes)
//
// Required env vars:
//   SUPABASE_URL       → https://xxxx.supabase.co
//   SUPABASE_ANON_KEY  → your service_role key (bypasses RLS)
//
// Optional env vars:
//   HISTORY_SCHEME_CODES → comma-separated e.g. "125497,100119"
//   HISTORY_START_DATE   → YYYY-MM-DD (default: 1 year ago)
//   HISTORY_END_DATE     → YYYY-MM-DD (default: today)
//   CONCURRENCY          → parallel requests (default: 5)
// ============================================================

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY;
const MF_BASE      = "https://api.mfapi.in";
const CONCURRENCY  = parseInt(process.env.CONCURRENCY || "5");
const BATCH_SIZE   = 300;
const DELAY_MS     = 100;

const today      = new Date().toISOString().split("T")[0];
const oneYearAgo = new Date(Date.now() - 365 * 86400_000).toISOString().split("T")[0];
const HIST_START = process.env.HISTORY_START_DATE || oneYearAgo;
const HIST_END   = process.env.HISTORY_END_DATE   || today;
const HIST_CODES = process.env.HISTORY_SCHEME_CODES
  ? process.env.HISTORY_SCHEME_CODES.split(",").map(s => s.trim()).filter(Boolean)
  : [];

// ── Utilities ──────────────────────────────────────────────

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function parseDate(ddMmYyyy) {
  if (!ddMmYyyy) return null;
  const [d, m, y] = ddMmYyyy.split("-");
  return `${y}-${m}-${d}`;
}

async function apiFetch(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 500 * (i + 1)));
    }
  }
}

async function supabaseUpsert(table, rows, conflictCol) {
  if (!rows.length) return;
  const url = `${SUPABASE_URL}/rest/v1/${table}?on_conflict=${conflictCol}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type":  "application/json",
      "apikey":        SUPABASE_KEY,
      "Authorization": `Bearer ${SUPABASE_KEY}`,
      "Prefer":        "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Upsert to "${table}" failed [${res.status}]: ${txt}`);
  }
}

async function supabaseUpsertBatched(table, rows, conflictCol) {
  for (const batch of chunk(rows, BATCH_SIZE)) {
    await supabaseUpsert(table, batch, conflictCol);
  }
}

async function runConcurrent(items, concurrency, taskFn) {
  const batches = chunk(items, concurrency);
  let done = 0;
  for (const batch of batches) {
    await Promise.allSettled(batch.map(item => taskFn(item)));
    done += batch.length;
    if (done % 2000 === 0) log(`   ... ${done} / ${items.length} processed`);
    await new Promise(r => setTimeout(r, DELAY_MS));
  }
}

// ── Step 1: Get all scheme codes ───────────────────────────

async function fetchAllSchemeCodes() {
  log("📋 Fetching master scheme list from /mf ...");
  const schemes = await apiFetch(`${MF_BASE}/mf`);
  log(`   → ${schemes.length} schemes found`);
  return schemes;
}

// ── Step 2: Fetch /mf/{code}/latest for every scheme ───────
//    Fills mf_schemes (with full details) + mf_latest_nav

async function syncSchemesAndLatestNAV(allSchemes) {
  log(`🔍 Fetching details for ${allSchemes.length} schemes (concurrency=${CONCURRENCY})...`);
  log(`   ⏳ Estimated time: ~${Math.ceil(allSchemes.length / CONCURRENCY / 10 / 60)} mins`);

  const schemesBuffer  = [];
  const latestNavBuffer = [];
  let processed = 0;
  let failed    = 0;

  async function flush() {
    if (schemesBuffer.length) {
      const rows = schemesBuffer.splice(0, schemesBuffer.length);
      await supabaseUpsertBatched("mf_schemes", rows, "scheme_code");
    }
    if (latestNavBuffer.length) {
      const rows = latestNavBuffer.splice(0, latestNavBuffer.length);
      await supabaseUpsertBatched("mf_latest_nav", rows, "scheme_code");
    }
  }

  await runConcurrent(allSchemes, CONCURRENCY, async (scheme) => {
    try {
      const json = await apiFetch(`${MF_BASE}/mf/${scheme.schemeCode}/latest`);
      const meta = json?.meta;
      const data = json?.data?.[0];

      if (!meta) return;

      // mf_schemes — full details including ISIN
      schemesBuffer.push({
        scheme_code:           meta.scheme_code,
        scheme_name:           meta.scheme_name            ?? null,
        fund_house:            meta.fund_house             ?? null,
        scheme_type:           meta.scheme_type            ?? null,
        scheme_category:       meta.scheme_category        ?? null,
        isin_growth:           meta.isin_growth            ?? null,
        isin_div_reinvestment: meta.isin_div_reinvestment  ?? null,
      });

      // mf_latest_nav
      if (data?.nav && data?.date) {
        latestNavBuffer.push({
          scheme_code: meta.scheme_code,
          nav:         parseFloat(data.nav),
          nav_date:    parseDate(data.date),
        });
      }

      processed++;

      // Flush every BATCH_SIZE to keep memory low + save progress
      if (schemesBuffer.length >= BATCH_SIZE) {
        await flush();
        log(`   💾 Saved ${processed} schemes (${failed} failed)`);
      }
    } catch (err) {
      failed++;
      if (failed <= 10) log(`   ⚠️  Scheme ${scheme.schemeCode} failed: ${err.message}`);
    }
  });

  // Final flush
  await flush();

  log(`✅ mf_schemes     → ${processed} rows synced`);
  log(`✅ mf_latest_nav  → ${processed} rows synced`);
  if (failed > 0) log(`⚠️  ${failed} schemes failed (will retry next run)`);
}

// ── Step 3: NAV history for specific schemes ───────────────

async function syncNavHistory() {
  if (!HIST_CODES.length) {
    log("⏭️  Skipping NAV history (set HISTORY_SCHEME_CODES to enable)");
    return;
  }

  log(`📈 NAV history for ${HIST_CODES.length} scheme(s) [${HIST_START} → ${HIST_END}]`);

  for (const code of HIST_CODES) {
    try {
      const json = await apiFetch(
        `${MF_BASE}/mf/${code}?startDate=${HIST_START}&endDate=${HIST_END}`
      );

      if (!json?.data?.length) {
        log(`   ⚠️  No history for scheme ${code}`);
        continue;
      }

      const rows = json.data
        .filter(d => d.nav && d.date)
        .map(d => ({
          scheme_code: parseInt(code),
          nav:         parseFloat(d.nav),
          nav_date:    parseDate(d.date),
        }));

      await supabaseUpsertBatched("mf_nav_history", rows, "scheme_code,nav_date");
      log(`   ✅ ${code} (${json.meta?.scheme_name ?? "?"}) — ${rows.length} rows`);

      await new Promise(r => setTimeout(r, 200));
    } catch (err) {
      log(`   ❌ History failed for ${code}: ${err.message}`);
    }
  }

  log("✅ mf_nav_history → done");
}

// ── Main ───────────────────────────────────────────────────

async function main() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error("❌ SUPABASE_URL and SUPABASE_ANON_KEY must be set.");
    process.exit(1);
  }

  log("🚀 MFapi → Supabase Sync Starting");
  log(`   Supabase    : ${SUPABASE_URL}`);
  log(`   Concurrency : ${CONCURRENCY}`);

  const start = Date.now();

  try {
    const allSchemes = await fetchAllSchemeCodes();
    await syncSchemesAndLatestNAV(allSchemes);
    await syncNavHistory();

    const mins = ((Date.now() - start) / 60000).toFixed(1);
    log(`🎉 All done in ${mins} minutes!`);
  } catch (err) {
    console.error("❌ Fatal error:", err.message);
    process.exit(1);
  }
}

main();
