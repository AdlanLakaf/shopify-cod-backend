// ============================================================
//  Leads DB — Postgres writer for the lead / funnel system
//
//  A "lead" is an order-in-the-making: captured the moment a
//  customer enters a valid name + phone, then progressively
//  enriched (wilaya, variant, quantity…) as they fill the form.
//  When the order is finally created, create-order.js flips the
//  matching lead to `converted` and links the order ref. Leads
//  that never submit become the abandoned-callback queue.
//
//  Design guarantees:
//   • ONE row per lead, ever. Identity is the client-minted
//     `leadId` (sessionStorage, stable across field edits) with a
//     cross-session fallback: an open lead with the same phone in
//     the last 30 days is reused instead of duplicated.
//   • Full audit trail. Every change to a tracked field appends a
//     human-readable entry to `history` ("user updated phone
//     number from X to Y") and flags the row `was_updated`.
//   • Best-effort, never on the order critical path. With no
//     DATABASE_URL every export is a silent no-op; every error is
//     swallowed (callers run inside Promise.allSettled / .catch).
// ============================================================

import pg from 'pg';

let pool = null;
let schemaReady = null;

function getPool() {
  const url = process.env.DATABASE_URL;
  if (!url) return null;
  if (pool) return pool;

  // Mirror _orders-db.js SSL logic: Railway internal = plain TCP, public = SSL.
  const wantSsl =
    process.env.DATABASE_SSL === 'true' ||
    (!/railway\.internal/.test(url) && /\b(sslmode=require|proxy\.rlwy\.net|\.railway\.app)\b/.test(url));

  pool = new pg.Pool({
    connectionString: url,
    ssl: wantSsl ? { rejectUnauthorized: false } : undefined,
    max: 4,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });
  pool.on('error', err => console.error('[leads-db] idle client error:', err.message));
  return pool;
}

async function ensureSchema(p) {
  if (schemaReady) return schemaReady;
  schemaReady = p.query(`
    CREATE TABLE IF NOT EXISTS leads (
      lead_id           TEXT PRIMARY KEY,
      created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
      first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      identified_at     TIMESTAMPTZ,
      submit_clicked_at TIMESTAMPTZ,
      converted_at      TIMESTAMPTZ,
      status            TEXT NOT NULL DEFAULT 'new',
      was_updated       BOOLEAN NOT NULL DEFAULT false,
      name              TEXT,
      phone             TEXT,
      wilaya            TEXT,
      baladiya          TEXT,
      office            TEXT,
      delivery_type     TEXT,
      variant_id        TEXT,
      variant_title     TEXT,
      quantity          INTEGER,
      merch_total_dzd   INTEGER,
      shipping_cost     INTEGER,
      total_dzd         INTEGER,
      source            TEXT,
      origin            TEXT,
      origin_url        TEXT,
      entry_url         TEXT,
      referrer          TEXT,
      user_agent        TEXT,
      order_ref         TEXT,
      shopify_order_id  BIGINT,
      order_count       INTEGER NOT NULL DEFAULT 0,
      last_source       TEXT,
      last_origin       TEXT,
      fail_reason       TEXT,
      history           JSONB NOT NULL DEFAULT '[]'::jsonb
    );
    CREATE INDEX IF NOT EXISTS leads_status_idx     ON leads (status);
    CREATE INDEX IF NOT EXISTS leads_phone_idx      ON leads (phone);
    CREATE INDEX IF NOT EXISTS leads_created_at_idx ON leads (created_at DESC);
    -- Additive columns for already-created tables (multi-order + last-touch attribution).
    ALTER TABLE leads ADD COLUMN IF NOT EXISTS order_count INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_source TEXT;
    ALTER TABLE leads ADD COLUMN IF NOT EXISTS last_origin TEXT;
    ALTER TABLE leads ADD COLUMN IF NOT EXISTS image_url TEXT;
  `).catch(err => {
    console.error('[leads-db] schema init failed:', err.message);
    schemaReady = null;          // allow a retry on the next call
    throw err;
  });
  return schemaReady;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const VALID_STAGES = new Set(['new', 'enriched', 'submitting', 'abandoned', 'failed', 'converted', 'recovered']);
const PHONE_RE = /^(05|06|07)\d{8}$/;

const clean = (s, max = 300) => String(s ?? '').replace(/<[^>]*>/g, '').trim().slice(0, max);
const normPhone = p => {
  const v = String(p ?? '').replace(/\s/g, '');
  return PHONE_RE.test(v) ? v : '';
};
const intOrNull = v => {
  const n = Math.round(Number(v));
  return Number.isFinite(n) ? n : null;
};

// Tracked fields that get a human-readable history entry when they change.
// [inputKey, columnName, label]
const NARRATED = [
  ['name',         'name',          'name'],
  ['phone',        'phone',         'phone number'],
  ['wilaya',       'wilaya',        'wilaya'],
  ['baladiya',     'baladiya',      'baladiya'],
  ['office',       'office',        'office'],
  ['deliveryType', 'delivery_type', 'delivery type'],
  ['variantTitle', 'variant_title', 'variant'],
  ['quantity',     'quantity',      'quantity'],
];

// Status precedence — predictable, never silently downgrades, but lets an
// abandoned lead re-activate if the customer comes back and progresses.
function resolveStatus(cur, incoming) {
  if (cur === 'converted' || cur === 'recovered') return cur;     // terminal
  if (incoming === 'converted')  return 'converted';
  if (incoming === 'failed')     return 'failed';
  if (incoming === 'submitting') return 'submitting';
  if (incoming === 'enriched')   return (cur === 'new' || cur === 'abandoned') ? 'enriched' : cur;
  if (incoming === 'abandoned')  return (cur === 'new' || cur === 'enriched')  ? 'abandoned' : cur;
  return cur === 'new' ? 'new' : cur;                              // incoming === 'new'
}

function notify(client, type, leadId) {
  client.query(`SELECT pg_notify('hn_lead_events', $1)`, [JSON.stringify({ type, leadId })])
    .catch(err => console.error('[leads-db] pg_notify failed:', err.message));
}

/**
 * Upsert one lead beacon. Idempotent on leadId (with phone-based cross-session
 * merge). Computes a field-level diff and appends narrated history entries.
 * Returns true on write, false on no-op/failure. Best-effort.
 */
export async function upsertLead(input = {}) {
  const p = getPool();
  if (!p) return false;

  const leadId = clean(input.leadId, 80);
  if (!leadId) return false;
  const stage = VALID_STAGES.has(input.stage) ? input.stage : 'new';
  const phone = normPhone(input.phone);

  // Provided values, normalised. A field counts as "provided" only when the
  // incoming value is non-empty — so an enrichment beacon never wipes earlier data.
  const incoming = {
    name:         clean(input.name, 120),
    phone,                                            // '' when missing/invalid
    wilaya:       clean(input.wilaya, 120),
    baladiya:     clean(input.baladiya, 120),
    office:       clean(input.office, 200),
    delivery_type: clean(input.deliveryType, 60),
    variant_id:   clean(input.variantId, 60),
    variant_title: clean(input.variantTitle, 200),
    image_url:    clean(input.imageUrl, 1000),
    quantity:     intOrNull(input.quantity),
    merch_total_dzd: intOrNull(input.merchTotalDzd),
    shipping_cost:   intOrNull(input.shippingCost),
    total_dzd:    intOrNull(input.totalDzd),
    source:       clean(input.source, 60),
    origin:       clean(input.origin, 200),
    origin_url:   clean(input.originUrl, 1000),
    entry_url:    clean(input.entryUrl, 1000),
    referrer:     clean(input.referrer, 1000),
    user_agent:   clean(input.userAgent, 300),
    fail_reason:  clean(input.failReason, 200),
  };

  let client;
  try {
    await ensureSchema(p);
    client = await p.connect();
    await client.query('BEGIN');

    // 1) Resolve identity: lead_id first, then an open same-phone lead.
    let { rows } = await client.query('SELECT * FROM leads WHERE lead_id = $1 FOR UPDATE', [leadId]);
    let row = rows[0];
    let merged = false;
    if (!row && phone) {
      const m = await client.query(
        `SELECT * FROM leads
           WHERE phone = $1 AND status NOT IN ('converted','recovered')
             AND created_at > now() - interval '30 days'
           ORDER BY created_at DESC LIMIT 1 FOR UPDATE`,
        [phone]
      );
      row = m.rows[0];
      merged = !!row;
    }

    // ── INSERT (new lead) — requires BOTH a real name and a valid phone ──
    // A lead is only a lead once the customer is identifiable: name + phone
    // both present and the phone passing the DZ mobile format. Anything less
    // stays an anonymous funnel session (see _funnel-db.js), not a lead.
    if (!row) {
      if (!phone || incoming.name.length < 2) { await client.query('ROLLBACK'); return false; }
      // Fresh lead: the stage maps straight to its natural status ('new' stays
      // 'new'; an enrich/submit/fail first-beacon takes that status directly).
      const status = resolveStatus('new', stage);
      const history = [{ ts: new Date().toISOString(), field: '_', action: 'create', note: 'lead captured' }];
      await client.query(
        `INSERT INTO leads
           (lead_id, status, identified_at, submit_clicked_at, converted_at,
            name, phone, wilaya, baladiya, office, delivery_type,
            variant_id, variant_title, quantity, merch_total_dzd, shipping_cost, total_dzd,
            source, origin, last_source, last_origin, origin_url, entry_url, referrer, user_agent, fail_reason, history, image_url)
         VALUES ($1,$2, now(), $3, $4,
            $5,$6,$7,$8,$9,$10,
            $11,$12,$13,$14,$15,$16,
            $17,$18,$17,$18,$19,$20,$21,$22,$23,$24::jsonb,$25)`,
        [
          leadId, status,
          stage === 'submitting' ? new Date() : null,
          stage === 'converted'  ? new Date() : null,
          incoming.name, incoming.phone, incoming.wilaya, incoming.baladiya, incoming.office, incoming.delivery_type,
          incoming.variant_id, incoming.variant_title, incoming.quantity, incoming.merch_total_dzd, incoming.shipping_cost, incoming.total_dzd,
          incoming.source, incoming.origin, incoming.origin_url, incoming.entry_url, incoming.referrer, incoming.user_agent, incoming.fail_reason,
          JSON.stringify(history), incoming.image_url,
        ]
      );
      await client.query('COMMIT');
      notify(client, 'created', leadId);
      return true;
    }

    // ── UPDATE (existing lead) — diff, narrate, transition ──
    const nowIso = new Date().toISOString();
    const entries = [];
    let wasUpdated = row.was_updated;
    const merge = {}; // column → final value

    for (const [key, col, label] of NARRATED) {
      const next = incoming[key];
      const provided = (next !== '' && next !== null && next !== undefined);
      if (!provided) { merge[col] = row[col]; continue; }
      const old = row[col];
      const oldStr = (old === null || old === undefined) ? '' : String(old);
      const newStr = String(next);
      if (oldStr === newStr) { merge[col] = old; continue; }
      if (oldStr === '') {
        entries.push({ ts: nowIso, field: col, action: 'set', to: newStr, note: `set ${label} to "${newStr}"` });
      } else {
        entries.push({ ts: nowIso, field: col, action: 'update', from: oldStr, to: newStr,
                       note: `user updated ${label} from "${oldStr}" to "${newStr}"` });
        wasUpdated = true;
      }
      merge[col] = next;
    }

    // Silently-merged columns (no narration): keep old when not provided.
    const keepOrSet = (col, val) => (val !== '' && val !== null && val !== undefined) ? val : row[col];
    merge.merch_total_dzd = keepOrSet('merch_total_dzd', incoming.merch_total_dzd);
    merge.shipping_cost   = keepOrSet('shipping_cost',   incoming.shipping_cost);
    merge.total_dzd       = keepOrSet('total_dzd',       incoming.total_dzd);
    merge.variant_id      = keepOrSet('variant_id',      incoming.variant_id);
    merge.image_url       = keepOrSet('image_url',       incoming.image_url);
    merge.source          = keepOrSet('source',          incoming.source);
    merge.origin          = keepOrSet('origin',          incoming.origin);
    merge.origin_url      = keepOrSet('origin_url',      incoming.origin_url);
    merge.entry_url       = keepOrSet('entry_url',       incoming.entry_url);
    merge.referrer        = keepOrSet('referrer',        incoming.referrer);
    merge.user_agent      = keepOrSet('user_agent',      incoming.user_agent);
    merge.fail_reason     = incoming.fail_reason || row.fail_reason;

    // Last-touch attribution: first-touch source/origin are preserved above
    // (keepOrSet keeps the old value); these record the MOST RECENT touch so we
    // can see when a buyer returns via a different — or the same — ad platform.
    merge.last_source = incoming.source || row.last_source || row.source;
    merge.last_origin = incoming.origin || row.last_origin || row.origin;
    const firstSrc = row.source || '';
    if (incoming.source && firstSrc && incoming.source !== firstSrc && incoming.source !== (row.last_source || '')) {
      entries.push({ ts: nowIso, field: 'last_source', action: 'retouch',
                     note: `returning from "${incoming.source}" (first touch was "${firstSrc}")` });
    }

    if (merged) {
      entries.push({ ts: nowIso, field: '_', action: 'merge', note: 'returning visit merged into this lead' });
    }

    const status = resolveStatus(row.status, stage);
    const history = [...(Array.isArray(row.history) ? row.history : []), ...entries].slice(-50);

    await client.query(
      `UPDATE leads SET
         status = $2, was_updated = $3, updated_at = now(),
         identified_at     = COALESCE(identified_at, now()),
         submit_clicked_at = CASE WHEN $4 = 'submitting' AND submit_clicked_at IS NULL THEN now() ELSE submit_clicked_at END,
         converted_at      = CASE WHEN $2 = 'converted'  AND converted_at      IS NULL THEN now() ELSE converted_at END,
         name = $5, phone = $6, wilaya = $7, baladiya = $8, office = $9, delivery_type = $10,
         variant_id = $11, variant_title = $12, quantity = $13,
         merch_total_dzd = $14, shipping_cost = $15, total_dzd = $16,
         source = $17, origin = $18, origin_url = $19, entry_url = $20, referrer = $21, user_agent = $22,
         fail_reason = $23, history = $24::jsonb, last_source = $25, last_origin = $26, image_url = $27
       WHERE lead_id = $1`,
      [
        row.lead_id, status, wasUpdated, stage,
        merge.name, merge.phone, merge.wilaya, merge.baladiya, merge.office, merge.delivery_type,
        merge.variant_id, merge.variant_title, merge.quantity,
        merge.merch_total_dzd, merge.shipping_cost, merge.total_dzd,
        merge.source, merge.origin, merge.origin_url, merge.entry_url, merge.referrer, merge.user_agent,
        merge.fail_reason, JSON.stringify(history), merge.last_source, merge.last_origin, merge.image_url,
      ]
    );
    await client.query('COMMIT');
    notify(client, 'updated', row.lead_id);
    return true;
  } catch (err) {
    try { await client?.query('ROLLBACK'); } catch { /* ignore */ }
    console.error('[leads-db] upsertLead failed:', err.message, `(lead ${leadId})`);
    return false;
  } finally {
    client?.release();
  }
}

/**
 * Mark a lead as converted once its order is created. Links by leadId, else by
 * the most recent open lead for the phone. If NEITHER exists — e.g. the
 * pre-conversion beacons never reached us (blocked / network) — we INSERT a
 * fresh converted lead from the order data, so the leads table is always a
 * superset of orders (never "empty" after a real sale). Best-effort.
 */
/** Fetch one lead row by leadId — used by the admin fire-event endpoint. */
export async function getLeadById(leadId) {
  const p = getPool();
  if (!p || !leadId) return null;
  try {
    await ensureSchema(p);
    const { rows } = await p.query('SELECT * FROM leads WHERE lead_id = $1', [String(leadId).slice(0, 80)]);
    return rows[0] || null;
  } catch (err) {
    console.error('[leads-db] getLeadById failed:', err.message, `(leadId ${leadId})`);
    return null;
  }
}

export async function markLeadConverted({
  leadId = '', orderRef = '', shopifyOrderId = null, phone = '',
  name = '', wilaya = '', baladiya = '', deliveryType = '',
  merchTotalDzd = null, shippingCost = null, totalDzd = null,
  source = '', origin = '', originUrl = '', imageUrl = '',
} = {}) {
  const p = getPool();
  if (!p) return false;
  const id = clean(leadId, 80);
  const ph = normPhone(phone);
  if (!id && !ph) return false;
  try {
    await ensureSchema(p);
    const entry = JSON.stringify([{ ts: new Date().toISOString(), field: '_', action: 'convert', note: `order placed (${clean(orderRef, 40)})` }]);
    // order_count + the history note only advance for a GENUINELY NEW order
    // (a new order_ref, or the first conversion) — so an idempotent retry /
    // fallback re-call of the SAME order never double-counts, while a real
    // repeat purchase on the same lead bumps the count and logs a line.
    const isNewOrder = `(($2 <> '' AND order_ref IS DISTINCT FROM $2) OR ($2 = '' AND order_count = 0))`;
    const sets = `status = 'converted',
                  converted_at = COALESCE(converted_at, now()),
                  updated_at   = now(),
                  order_count  = order_count + CASE WHEN ${isNewOrder} THEN 1 ELSE 0 END,
                  order_ref    = COALESCE(NULLIF($2,''), order_ref),
                  shopify_order_id = COALESCE($3, shopify_order_id),
                  image_url    = COALESCE(NULLIF($5,''), image_url),
                  history = CASE WHEN ${isNewOrder}
                                 THEN COALESCE(history,'[]'::jsonb) || $4::jsonb
                                 ELSE COALESCE(history,'[]'::jsonb) END`;
    const img = clean(imageUrl, 1000);
    let res;
    if (id) {
      res = await p.query(
        `UPDATE leads SET ${sets} WHERE lead_id = $1 RETURNING lead_id`,
        [id, clean(orderRef, 40), shopifyOrderId ? Number(shopifyOrderId) : null, entry, img]
      );
    }
    // Fall back to phone when no leadId match.
    if ((!res || res.rowCount === 0) && ph) {
      res = await p.query(
        `UPDATE leads SET ${sets}
           WHERE lead_id = (
             SELECT lead_id FROM leads
               WHERE phone = $1 AND status NOT IN ('converted','recovered')
               ORDER BY created_at DESC LIMIT 1
           ) RETURNING lead_id`,
        [ph, clean(orderRef, 40), shopifyOrderId ? Number(shopifyOrderId) : null, entry, img]
      );
    }
    if (res && res.rowCount > 0) return true;

    // ── Backfill: no prior lead existed → create the converted lead now. ──
    if (!ph) return false;                              // need a valid phone to be a real lead
    const newId = id || `conv-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const history = JSON.stringify([
      { ts: new Date().toISOString(), field: '_', action: 'create', note: 'lead created at order (no prior beacon)' },
      { ts: new Date().toISOString(), field: '_', action: 'convert', note: `order created (${clean(orderRef, 40)})` },
    ]);
    const ins = await p.query(
      `INSERT INTO leads
         (lead_id, status, identified_at, converted_at, name, phone, wilaya, baladiya,
          delivery_type, merch_total_dzd, shipping_cost, total_dzd, source, origin, last_source, last_origin, origin_url,
          order_ref, shopify_order_id, order_count, history, image_url)
       VALUES ($1,'converted', now(), now(), $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$10,$11,$12,$13,$14,1,$15::jsonb,$16)
       ON CONFLICT (lead_id) DO UPDATE SET
         status='converted', converted_at = COALESCE(leads.converted_at, now()), updated_at = now(),
         order_ref = COALESCE(NULLIF(EXCLUDED.order_ref,''), leads.order_ref),
         image_url = COALESCE(NULLIF(EXCLUDED.image_url,''), leads.image_url)
       RETURNING lead_id`,
      [
        newId, clean(name, 120), ph, clean(wilaya, 120), clean(baladiya, 120),
        clean(deliveryType, 60), intOrNull(merchTotalDzd), intOrNull(shippingCost), intOrNull(totalDzd),
        clean(source, 60), clean(origin, 200), clean(originUrl, 1000),
        clean(orderRef, 40), shopifyOrderId ? Number(shopifyOrderId) : null, history, img,
      ]
    );
    return !!(ins && ins.rowCount > 0);
  } catch (err) {
    console.error('[leads-db] markLeadConverted failed:', err.message, `(lead ${id || ph})`);
    return false;
  }
}

/**
 * Retention prune — keep the table small and the callback queue fresh.
 * Deletes every lead older than `days` (default 30). Converted leads are kept
 * as long as the window because the dashboard funnel needs them; the order
 * itself lives independently in the `orders` table, so nothing is lost.
 * Returns the number of rows removed.
 */
export async function pruneLeads(days = 30) {
  const p = getPool();
  if (!p) return 0;
  try {
    await ensureSchema(p);
    const d = Math.max(1, Math.floor(Number(days) || 30));
    const res = await p.query(`DELETE FROM leads WHERE created_at < now() - ($1 || ' days')::interval`, [String(d)]);
    return res.rowCount || 0;
  } catch (err) {
    console.error('[leads-db] pruneLeads failed:', err.message);
    return 0;
  }
}
