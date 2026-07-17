// ============================================================
//  TikTok DB — dedupe ledger + form/ad → product mapping
//
//  Two tiny tables, same Postgres as orders/leads (zero new
//  infra cost):
//
//  • tiktok_leads — one row per TikTok lead_id, ever. This is
//    the idempotency ledger: the webhook AND the polling backup
//    both funnel through claimLead(); ON CONFLICT DO NOTHING
//    means a retried webhook (TikTok retries 72h until it gets
//    a 200) or an overlapping poll can never double-process.
//    Raw payload is kept for audit/debug (TikTok deletes lead
//    data after 90 days — our copy is the durable one).
//
//  • tiktok_map — resolves WHICH product a lead is buying.
//    TikTok instant forms don't carry a variant, so staff
//    configure a mapping per ad / adgroup / form / campaign
//    (most-specific wins) with a 'default' catch-all.
//
//  Best-effort like the other _*-db modules: no DATABASE_URL →
//  silent no-op; errors are swallowed and logged.
// ============================================================

import pg from 'pg';

let pool = null;
let schemaReady = null;

function getPool() {
  const url = process.env.DATABASE_URL;
  if (!url) return null;
  if (pool) return pool;
  const wantSsl =
    process.env.DATABASE_SSL === 'true' ||
    (!/railway\.internal/.test(url) && /\b(sslmode=require|proxy\.rlwy\.net|\.railway\.app)\b/.test(url));
  pool = new pg.Pool({
    connectionString: url,
    ssl: wantSsl ? { rejectUnauthorized: false } : undefined,
    max: 2,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });
  pool.on('error', err => console.error('[tiktok-db] idle client error:', err.message));
  return pool;
}

async function ensureSchema(p) {
  if (schemaReady) return schemaReady;
  schemaReady = p.query(`
    CREATE TABLE IF NOT EXISTS tiktok_leads (
      tiktok_lead_id TEXT PRIMARY KEY,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      processed_at   TIMESTAMPTZ,
      status         TEXT NOT NULL DEFAULT 'received',
      via            TEXT,
      form_id        TEXT,
      page_id        TEXT,
      campaign_id    TEXT,
      adgroup_id     TEXT,
      ad_id          TEXT,
      order_ref      TEXT,
      fail_reason    TEXT,
      raw            JSONB
    );
    CREATE INDEX IF NOT EXISTS tiktok_leads_created_idx ON tiktok_leads (created_at DESC);

    CREATE TABLE IF NOT EXISTS tiktok_pages (
      page_id     TEXT PRIMARY KEY,     -- TikTok instant-form page id (polling backup)
      label       TEXT,                 -- staff-friendly name ("Ramadan offer form")
      active      BOOLEAN NOT NULL DEFAULT true,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS tiktok_map (
      match_type  TEXT NOT NULL,          -- 'ad' | 'adgroup' | 'form' | 'campaign' | 'default'
      match_id    TEXT NOT NULL,          -- the TikTok id ('*' for default)
      variant_id  BIGINT,                 -- Shopify variant (preferred — real catalog line)
      title       TEXT,                   -- fallback custom line title when no variant
      price_dzd   INTEGER NOT NULL DEFAULT 0,
      quantity    INTEGER NOT NULL DEFAULT 1,
      active      BOOLEAN NOT NULL DEFAULT true,
      note        TEXT,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (match_type, match_id)
    );
  `).catch(err => {
    console.error('[tiktok-db] schema init failed:', err.message);
    schemaReady = null;
    throw err;
  });
  return schemaReady;
}

const s = (v, max = 120) => String(v ?? '').trim().slice(0, max);

/**
 * Claim a TikTok lead for processing. Returns true when THIS caller owns it
 * (first sight), false when it was already claimed (duplicate webhook / poll
 * overlap) or the DB is unavailable — callers must skip processing on false.
 */
export async function claimLead({ tiktokLeadId, via = 'webhook', formId = '', pageId = '', campaignId = '', adgroupId = '', adId = '', raw = null }) {
  const p = getPool();
  const id = s(tiktokLeadId, 80);
  if (!p || !id) return false;
  try {
    await ensureSchema(p);
    const res = await p.query(
      `INSERT INTO tiktok_leads (tiktok_lead_id, via, form_id, page_id, campaign_id, adgroup_id, ad_id, raw)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
       ON CONFLICT (tiktok_lead_id) DO NOTHING`,
      [id, s(via, 20), s(formId, 80), s(pageId, 80), s(campaignId, 80), s(adgroupId, 80), s(adId, 80),
       JSON.stringify(raw ?? {}).slice(0, 50_000)]
    );
    return res.rowCount > 0;
  } catch (err) {
    console.error('[tiktok-db] claimLead failed:', err.message, `(lead ${id})`);
    return false;
  }
}

/** Stamp the outcome of processing on the ledger row. Best-effort. */
export async function markLeadOutcome(tiktokLeadId, { status, orderRef = '', failReason = '' }) {
  const p = getPool();
  if (!p) return;
  try {
    await ensureSchema(p);
    await p.query(
      `UPDATE tiktok_leads SET processed_at = now(), status = $2,
              order_ref = COALESCE(NULLIF($3,''), order_ref),
              fail_reason = COALESCE(NULLIF($4,''), fail_reason)
        WHERE tiktok_lead_id = $1`,
      [s(tiktokLeadId, 80), s(status, 30), s(orderRef, 40), s(failReason, 300)]
    );
  } catch (err) {
    console.error('[tiktok-db] markLeadOutcome failed:', err.message);
  }
}

/**
 * Resolve the product mapping for a lead: most specific active rule wins.
 * ad → adgroup → form → campaign → default. Returns the row or null.
 */
export async function resolveMapping({ adId = '', adgroupId = '', formId = '', campaignId = '' }) {
  const p = getPool();
  if (!p) return null;
  try {
    await ensureSchema(p);
    const { rows } = await p.query(
      `SELECT * FROM tiktok_map
        WHERE active = true AND (
          (match_type = 'ad'       AND match_id = $1) OR
          (match_type = 'adgroup'  AND match_id = $2) OR
          (match_type = 'form'     AND match_id = $3) OR
          (match_type = 'campaign' AND match_id = $4) OR
          (match_type = 'default')
        )
        ORDER BY CASE match_type
          WHEN 'ad' THEN 0 WHEN 'adgroup' THEN 1 WHEN 'form' THEN 2
          WHEN 'campaign' THEN 3 ELSE 4 END
        LIMIT 1`,
      [s(adId, 80), s(adgroupId, 80), s(formId, 80), s(campaignId, 80)]
    );
    return rows[0] || null;
  } catch (err) {
    console.error('[tiktok-db] resolveMapping failed:', err.message);
    return null;
  }
}

/** Upsert one mapping rule (admin). */
export async function upsertMapping({ matchType, matchId, variantId = null, title = '', priceDzd = 0, quantity = 1, active = true, note = '' }) {
  const p = getPool();
  if (!p) return false;
  const type = s(matchType, 20).toLowerCase();
  if (!['ad', 'adgroup', 'form', 'campaign', 'default'].includes(type)) return false;
  const id = type === 'default' ? '*' : s(matchId, 80);
  if (!id) return false;
  try {
    await ensureSchema(p);
    await p.query(
      `INSERT INTO tiktok_map (match_type, match_id, variant_id, title, price_dzd, quantity, active, note, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now())
       ON CONFLICT (match_type, match_id) DO UPDATE SET
         variant_id = EXCLUDED.variant_id, title = EXCLUDED.title,
         price_dzd = EXCLUDED.price_dzd, quantity = EXCLUDED.quantity,
         active = EXCLUDED.active, note = EXCLUDED.note, updated_at = now()`,
      [type, id,
       variantId ? Number(variantId) : null,
       s(title, 200),
       Math.max(0, Math.round(Number(priceDzd) || 0)),
       Math.min(Math.max(Math.round(Number(quantity) || 1), 1), 10),
       active !== false, s(note, 300)]
    );
    return true;
  } catch (err) {
    console.error('[tiktok-db] upsertMapping failed:', err.message);
    return false;
  }
}

/** Delete one mapping rule (admin). */
export async function deleteMapping(matchType, matchId) {
  const p = getPool();
  if (!p) return false;
  try {
    await ensureSchema(p);
    const res = await p.query('DELETE FROM tiktok_map WHERE match_type = $1 AND match_id = $2',
      [s(matchType, 20).toLowerCase(), s(matchId, 80)]);
    return res.rowCount > 0;
  } catch (err) {
    console.error('[tiktok-db] deleteMapping failed:', err.message);
    return false;
  }
}

/** List all mapping rules + polling pages + recent ledger rows (admin dashboard). */
export async function listMappingsAndRecent(limit = 50) {
  const p = getPool();
  if (!p) return { mappings: [], pages: [], recent: [] };
  try {
    await ensureSchema(p);
    const [maps, pages, recent] = await Promise.all([
      p.query('SELECT * FROM tiktok_map ORDER BY match_type, match_id'),
      p.query('SELECT * FROM tiktok_pages ORDER BY updated_at DESC'),
      p.query(`SELECT tiktok_lead_id, created_at, processed_at, status, via, form_id,
                      campaign_id, adgroup_id, ad_id, order_ref, fail_reason
                 FROM tiktok_leads ORDER BY created_at DESC LIMIT $1`,
        [Math.min(Math.max(Number(limit) || 50, 1), 200)]),
    ]);
    return { mappings: maps.rows, pages: pages.rows, recent: recent.rows };
  } catch (err) {
    console.error('[tiktok-db] list failed:', err.message);
    return { mappings: [], pages: [], recent: [] };
  }
}

// ── Polling pages (staff-managed in the admin, not env) ──────────────────────

/** Upsert one polling page (admin). */
export async function upsertPage({ pageId, label = '', active = true }) {
  const p = getPool();
  const id = s(pageId, 80).replace(/\D/g, '');   // TikTok ids are numeric
  if (!p || !id) return false;
  try {
    await ensureSchema(p);
    await p.query(
      `INSERT INTO tiktok_pages (page_id, label, active, updated_at) VALUES ($1,$2,$3, now())
       ON CONFLICT (page_id) DO UPDATE SET label = EXCLUDED.label, active = EXCLUDED.active, updated_at = now()`,
      [id, s(label, 120), active !== false]
    );
    return true;
  } catch (err) {
    console.error('[tiktok-db] upsertPage failed:', err.message);
    return false;
  }
}

/** Delete one polling page (admin). */
export async function deletePage(pageId) {
  const p = getPool();
  if (!p) return false;
  try {
    await ensureSchema(p);
    const res = await p.query('DELETE FROM tiktok_pages WHERE page_id = $1', [s(pageId, 80)]);
    return res.rowCount > 0;
  } catch (err) {
    console.error('[tiktok-db] deletePage failed:', err.message);
    return false;
  }
}

/** Active page ids for the polling cron (DB-managed; env is a legacy extra). */
export async function getActivePageIds() {
  const p = getPool();
  if (!p) return [];
  try {
    await ensureSchema(p);
    const { rows } = await p.query('SELECT page_id FROM tiktok_pages WHERE active = true');
    return rows.map(r => r.page_id);
  } catch (err) {
    console.error('[tiktok-db] getActivePageIds failed:', err.message);
    return [];
  }
}

/**
 * Retention prune — raw payloads are the bulky part; the ledger row itself is
 * tiny and MUST outlive TikTok's retry window and any poll overlap, so we keep
 * rows 180 days and only null the raw JSON after 30.
 */
export async function pruneTiktokLeads() {
  const p = getPool();
  if (!p) return 0;
  try {
    await ensureSchema(p);
    await p.query(`UPDATE tiktok_leads SET raw = NULL WHERE raw IS NOT NULL AND created_at < now() - interval '30 days'`);
    const res = await p.query(`DELETE FROM tiktok_leads WHERE created_at < now() - interval '180 days'`);
    return res.rowCount || 0;
  } catch (err) {
    console.error('[tiktok-db] prune failed:', err.message);
    return 0;
  }
}
