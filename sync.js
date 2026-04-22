// ============================================================
// sync.js — MFapi.in → Supabase Sync Script
// Usage: node sync.js
// Env vars required: SUPABASE_URL, SUPABASE_ANON_KEY
// Optional:         HISTORY_SCHEME_CODES (comma-separated)
//                   HISTORY_START_DATE   (YYYY-MM-DD, default: 1 year ago)
//                   HISTORY_END_DATE     (YYYY-MM-DD, default: today)
// ============================================================

const SUPABASE_URL     = process.env.SUPABASE_URL;
const SUPABASE_KEY     = process.env.SUPABASE_ANON_KEY;
const MF_BASE          = "https://api.mfapi.in";

// Schemes to pull full history for (comma-separated scheme codes in env)
// e.g. HISTORY_SCHEME_CODES=125497,100119,120503
const HISTORY_CODES    = process.env.HISTORY_SCHEME_CODES
  ? process.env.HISTORY_SCHEME_CODES.split(",").map(s => s.trim())
  : [];

const today            = new Date().toISOString().split("T")[0];
const oneYearAgo       = new Date(Date.now() - 365 * 86400_000).toISOString().split("T")[0];
const HISTORY_START    = process.env.HISTORY_START_DATE || oneYearAgo;
const HISTORY_END      = process.env.HISTORY_END_DATE   || today;

const BATCH_SIZE       = 500; // rows per Supabase upsert batch

// ── helpers ────────────────────────────────────────────────

async function apiFetch(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`MFapi fetch failed [${res.status}]: ${url}`);
  return res.json();
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
    throw new Error(`Supabase upsert to ${table} failed [${res.status}]: ${txt}`);
  }
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function parseDate(ddMmYyyy) {
  // "29-12-2023" → "2023-12-29"
  const [d, m, y] = ddMmYyyy.split("-");
  return `${y}-${m}-${d}`;
}

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

// ── Step 1: Fetch & store all schemes ──────────────────────

async function syncSchemes() {
  log("📋 Fetching all schemes list...");
  const schemes = await apiFetch(`${MF_BASE}/mf`);
  log(`   → ${schemes.length} schemes found`);

  const rows = schemes.map(s => ({
    scheme_code:  s.schemeCode,
    scheme_name:  s.schemeName,
    fund_house:   s.fundHouse   ?? null,
    scheme_type:  s.schemeType  ?? null,
    scheme_category: s.schemeCategory ?? null,
    isin_growth:  s.isinGrowth  ?? null,
    isin_div_reinvestment: s.isinDivReinvestment ?? null,
  }));

  for (const batch of chunk(rows, BATCH_SIZE)) {
    await supabaseUpsert("mf_schemes", batch, "scheme_code");
  }
  log(`✅ Synced ${rows.length} schemes → mf_schemes`);
  return schemes.map(s => s.schemeCode);
}

// ── Step 2: Fetch & store latest NAV for ALL schemes ───────

async function syncLatestNAV() {
  log("💹 Fetching latest NAV for all schemes...");
  const data = await apiFetch(`${MF_BASE}/mf/latest`);
  log(`   → ${data.length} NAV records found`);

  const rows = data
    .filter(s => s.nav && s.date)
    .map(s => ({
      scheme_code: s.schemeCode,
      nav:         parseFloat(s.nav),
      nav_date:    parseDate(s.date),
    }));

  for (const batch of chunk(rows, BATCH_SIZE)) {
    await supabaseUpsert("mf_latest_nav", batch, "scheme_code");
  }
  log(`✅ Synced ${rows.length} latest NAVs → mf_latest_nav`);
}

// ── Step 3: Fetch & store NAV history for chosen schemes ───

async function syncNavHistory(schemeCodes) {
  if (!schemeCodes.length) {
    log("⏭️  No scheme codes specified for history sync (set HISTORY_SCHEME_CODES)");
    return;
  }

  log(`📈 Fetching NAV history for ${schemeCodes.length} scheme(s) [${HISTORY_START} → ${HISTORY_END}]`);

  for (const code of schemeCodes) {
    try {
      const url = `${MF_BASE}/mf/${code}?startDate=${HISTORY_START}&endDate=${HISTORY_END}`;
      const json = await apiFetch(url);

      if (!json.data || !json.data.length) {
        log(`   ⚠️  No history data for scheme ${code}`);
        continue;
      }

      const rows = json.data
        .filter(d => d.nav && d.date)
        .map(d => ({
          scheme_code: parseInt(code),
          nav:         parseFloat(d.nav),
          nav_date:    parseDate(d.date),
        }));

      for (const batch of chunk(rows, BATCH_SIZE)) {
        await supabaseUpsert("mf_nav_history", batch, "scheme_code,nav_date");
      }

      log(`   ✅ ${code} (${json.meta?.scheme_name ?? "?"}) — ${rows.length} rows`);

      // Polite delay to respect rate limiting
      await new Promise(r => setTimeout(r, 200));
    } catch (err) {
      log(`   ❌ Failed for scheme ${code}: ${err.message}`);
    }
  }

  log("✅ NAV history sync complete → mf_nav_history");
}

// ── Main ───────────────────────────────────────────────────

async function main() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error("❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY env variables.");
    process.exit(1);
  }

  log("🚀 Starting MFapi → Supabase sync");
  log(`   Supabase: ${SUPABASE_URL}`);

  try {
    await syncSchemes();
    await syncLatestNAV();
    await syncNavHistory(HISTORY_CODES);
    log("🎉 All done!");
  } catch (err) {
    console.error("❌ Sync failed:", err.message);
    process.exit(1);
  }
}

main();
