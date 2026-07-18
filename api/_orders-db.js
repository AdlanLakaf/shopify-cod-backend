// ============================================================
//  Orders DB — Postgres writer for the new custom order system
//
//  The COD backend stays the single source of truth for order
//  CREATION: every completed order is mirrored into Postgres so
//  the landing admin (/admin/orders) can manage its lifecycle
//  independently of Shopify. This is the migration bridge — while
//  Shopify orders are still created, each one also lands here.
//
//  Best-effort by design: a DB hiccup must NEVER break the order
//  path (see CLAUDE.md — a lost order is worse than a missing
//  mirror row). Every export swallows its own errors and the
//  caller runs it inside Promise.allSettled. With no DATABASE_URL
//  set, every function is a silent no-op.
// ============================================================

import { getPool } from './_pg.js';

let schemaReady = null;

async function ensureSchema(p) {
  if (schemaReady) return schemaReady;
  schemaReady = p.query(`
    CREATE TABLE IF NOT EXISTS orders (
      ref              TEXT PRIMARY KEY,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
      status           TEXT NOT NULL DEFAULT 'pending',
      name             TEXT NOT NULL,
      phone            TEXT NOT NULL,
      wilaya           TEXT,
      baladiya         TEXT,
      address          TEXT,
      delivery_type    TEXT,
      shipping_cost    INTEGER NOT NULL DEFAULT 0,
      items            JSONB NOT NULL DEFAULT '[]'::jsonb,
      merch_total_dzd  INTEGER NOT NULL DEFAULT 0,
      total_dzd        INTEGER NOT NULL DEFAULT 0,
      note             TEXT,
      source           TEXT,
      origin           TEXT,
      origin_url       TEXT,
      entry_url        TEXT,
      shopify_order_id BIGINT
    );
    CREATE INDEX IF NOT EXISTS orders_status_idx     ON orders (status);
    CREATE INDEX IF NOT EXISTS orders_created_at_idx ON orders (created_at DESC);
    CREATE INDEX IF NOT EXISTS orders_phone_idx      ON orders (phone);
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS origin_url TEXT;
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS entry_url  TEXT;
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS ad_type    TEXT NOT NULL DEFAULT '';
  `).catch(err => {
    console.error('[orders-db] schema init failed:', err.message);
    schemaReady = null;          // allow a retry on the next call
    throw err;
  });
  return schemaReady;
}

/**
 * Mirror one completed order into Postgres. Idempotent on `ref`
 * (ON CONFLICT DO NOTHING) so an idempotent order replay never
 * double-inserts. Returns true on write, false on no-op/failure.
 */
export async function insertOrder(o) {
  const p = getPool();
  if (!p) return false;
  try {
    await ensureSchema(p);
    const result = await p.query(
      `INSERT INTO orders
         (ref, status, name, phone, wilaya, baladiya, address, delivery_type,
          shipping_cost, items, merch_total_dzd, total_dzd, note, source, origin,
          origin_url, entry_url, shopify_order_id, ad_type)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,$17,$18,$19)
       ON CONFLICT (ref) DO NOTHING`,
      [
        o.ref,
        o.status || 'pending',
        o.name || '',
        o.phone || '',
        o.wilaya || '',
        o.baladiya || '',
        o.address || '',
        o.deliveryType || '',
        Math.round(Number(o.shippingCost) || 0),
        JSON.stringify(Array.isArray(o.items) ? o.items : []),
        Math.round(Number(o.merchTotalDzd) || 0),
        Math.round(Number(o.totalDzd) || 0),
        o.note || '',
        o.source || '',
        o.origin || '',
        (o.originUrl || '').slice(0, 1000),
        (o.entryUrl  || '').slice(0, 1000),
        o.shopifyOrderId ? Number(o.shopifyOrderId) : null,
        (o.adType || '').slice(0, 30),
      ]
    );

    // Notify the landing app's SSE clients about the new order.
    // rowCount === 0 means ON CONFLICT DO NOTHING fired (idempotent replay) — no notify.
    // Fire-and-forget: never awaited, never allowed to throw into the order path.
    if (result.rowCount > 0) {
      p.query(
        `SELECT pg_notify('hn_order_events', $1)`,
        [JSON.stringify({ type: 'created', ref: o.ref })]
      ).catch(err => console.error('[orders-db] pg_notify failed:', err.message));
    }

    return true;
  } catch (err) {
    console.error('[orders-db] insertOrder failed:', err.message, `(ref ${o?.ref})`);
    return false;
  }
}

/**
 * Most recent order for a phone within `hours` (optionally same source).
 * Used by the TikTok lead pipeline: a customer double-submitting the form
 * gets linked to their existing order instead of a duplicate. Best-effort.
 */
export async function findRecentOrderByPhone(phone, { source = '', hours = 24 } = {}) {
  const p = getPool();
  if (!p || !phone) return null;
  try {
    await ensureSchema(p);
    const h = Math.min(Math.max(Number(hours) || 24, 1), 720);
    const { rows } = await p.query(
      `SELECT ref, created_at, status, source FROM orders
        WHERE phone = $1 AND ($2 = '' OR source = $2)
          AND created_at > now() - ($3 || ' hours')::interval
        ORDER BY created_at DESC LIMIT 1`,
      [String(phone).slice(0, 20), String(source).slice(0, 60), String(h)]
    );
    return rows[0] || null;
  } catch (err) {
    console.error('[orders-db] findRecentOrderByPhone failed:', err.message);
    return null;
  }
}

/** Fetch one order row by ref — used by the admin fire-event endpoint. */
export async function getOrderByRef(ref) {
  const p = getPool();
  if (!p || !ref) return null;
  try {
    await ensureSchema(p);
    const { rows } = await p.query('SELECT * FROM orders WHERE ref = $1', [String(ref).slice(0, 60)]);
    return rows[0] || null;
  } catch (err) {
    console.error('[orders-db] getOrderByRef failed:', err.message, `(ref ${ref})`);
    return null;
  }
}

/**
 * Record the outcome of the background Shopify push for an order that was
 * already saved to our DB (fast path). Either stamps the Shopify order id, or
 * flags the row so staff see the parcel never reached Shopify. Best-effort.
 */
export async function updateOrderShopify(ref, { shopifyOrderId = null, syncFailed = false } = {}) {
  const p = getPool();
  if (!p) return false;
  try {
    await ensureSchema(p);
    if (syncFailed) {
      // Append a one-time flag to the note so it surfaces in /admin/orders.
      await p.query(
        `UPDATE orders
            SET note = CASE WHEN COALESCE(note,'') = '' THEN $2 ELSE note || ' | ' || $2 END,
                updated_at = now()
          WHERE ref = $1 AND COALESCE(note,'') NOT LIKE '%SHOPIFY-SYNC-FAILED%'`,
        [ref, '⚠ SHOPIFY-SYNC-FAILED']
      );
    } else if (shopifyOrderId) {
      await p.query(
        'UPDATE orders SET shopify_order_id = $2, updated_at = now() WHERE ref = $1',
        [ref, Number(shopifyOrderId)]
      );
    }
    return true;
  } catch (err) {
    console.error('[orders-db] updateOrderShopify failed:', err.message, `(ref ${ref})`);
    return false;
  }
}
