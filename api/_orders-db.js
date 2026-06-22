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

import pg from 'pg';

let pool = null;
let schemaReady = null;

function getPool() {
  const url = process.env.DATABASE_URL;
  if (!url) return null;
  if (pool) return pool;

  // Railway's internal network (*.railway.internal) speaks plain TCP; the
  // public proxy host needs SSL. Allow an explicit override either way.
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
  pool.on('error', err => console.error('[orders-db] idle client error:', err.message));
  return pool;
}

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
      shopify_order_id BIGINT
    );
    CREATE INDEX IF NOT EXISTS orders_status_idx     ON orders (status);
    CREATE INDEX IF NOT EXISTS orders_created_at_idx ON orders (created_at DESC);
    CREATE INDEX IF NOT EXISTS orders_phone_idx      ON orders (phone);
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
    await p.query(
      `INSERT INTO orders
         (ref, status, name, phone, wilaya, baladiya, address, delivery_type,
          shipping_cost, items, merch_total_dzd, total_dzd, note, source, origin, shopify_order_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16)
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
        o.shopifyOrderId ? Number(o.shopifyOrderId) : null,
      ]
    );
    return true;
  } catch (err) {
    console.error('[orders-db] insertOrder failed:', err.message, `(ref ${o?.ref})`);
    return false;
  }
}
