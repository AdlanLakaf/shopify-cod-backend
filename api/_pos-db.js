// ============================================================
//  POS sync DB — multi-shop / multi-brand bridge between the
//  local HandsNose ERP installs and the online CRM.
//
//  Model:
//    pos_brands   — a brand (owner + profit-share %, logo)
//    pos_shops    — a physical shop running one local ERP host.
//                   Each shop has its own sync token, its own
//                   configurable sync interval, and a routing
//                   priority (lower = asked for stock first).
//    pos_products — per-shop mirror of the local salable stock
//                   (keyed shop_id + local uuid). Pushed up by
//                   the ERP on every tick; the cloud NEVER writes
//                   quantities except through this push.
//    pos_variant_map — Shopify variant → (shop, local product)
//                   binding. One variant can map to a different
//                   local product in each shop.
//    pos_sales    — per-shop mirror of local `sales` rows (items
//                   inlined as JSONB) for the unified analytics.
//
//  Orders table gains: variant_id (routing key), assigned_shop_id,
//  pos_status ('' → assigned → received → confirmed | cancelled).
//  Reservation: an order in an open pos_status counts against the
//  mapped product's available stock, so two web orders between two
//  ticks can't both grab the last bottle.
//
//  Cost rules (Railway): ONE shared pool (_pg.js), ONE request per
//  shop per interval (the tick carries everything both ways), no
//  per-row round-trips (multi-VALUES batches), and analytics reads
//  hit indexed aggregates only when staff open the page.
//
//  Best-effort contract: same as _orders-db.js — never throw into
//  the order path; no DATABASE_URL → every function no-ops.
// ============================================================

import crypto from 'crypto';
import { getPool } from './_pg.js';

let schemaReady = null;

async function ensureSchema(p) {
  if (schemaReady) return schemaReady;
  schemaReady = p.query(`
    CREATE TABLE IF NOT EXISTS pos_brands (
      id             SERIAL PRIMARY KEY,
      name           TEXT NOT NULL UNIQUE,
      owner_name     TEXT NOT NULL DEFAULT '',
      our_share_pct  DOUBLE PRECISION NOT NULL DEFAULT 100,
      logo_url       TEXT NOT NULL DEFAULT '',
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS pos_shops (
      id                SERIAL PRIMARY KEY,
      brand_id          INTEGER NOT NULL REFERENCES pos_brands(id),
      name              TEXT NOT NULL,
      logo_url          TEXT NOT NULL DEFAULT '',
      sync_token        TEXT NOT NULL UNIQUE,
      sync_interval_sec INTEGER NOT NULL DEFAULT 300,
      routing_priority  INTEGER NOT NULL DEFAULT 100,
      active            BOOLEAN NOT NULL DEFAULT true,
      last_sync_at      TIMESTAMPTZ,
      created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS pos_products (
      shop_id        INTEGER NOT NULL REFERENCES pos_shops(id) ON DELETE CASCADE,
      uuid           TEXT NOT NULL,
      product_type   TEXT NOT NULL DEFAULT '',
      name           TEXT NOT NULL DEFAULT '',
      brand          TEXT NOT NULL DEFAULT '',
      category       TEXT NOT NULL DEFAULT '',
      sell_price_dzd DOUBLE PRECISION NOT NULL DEFAULT 0,
      stock_qty      DOUBLE PRECISION NOT NULL DEFAULT 0,
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (shop_id, uuid)
    );
    CREATE TABLE IF NOT EXISTS pos_variant_map (
      shopify_variant_id BIGINT NOT NULL,
      shop_id            INTEGER NOT NULL REFERENCES pos_shops(id) ON DELETE CASCADE,
      product_uuid       TEXT NOT NULL,
      qty_per_unit       DOUBLE PRECISION NOT NULL DEFAULT 1,
      PRIMARY KEY (shopify_variant_id, shop_id)
    );
    CREATE TABLE IF NOT EXISTS pos_sales (
      shop_id        INTEGER NOT NULL REFERENCES pos_shops(id) ON DELETE CASCADE,
      local_sale_id  INTEGER NOT NULL,
      status         TEXT NOT NULL DEFAULT '',
      channel        TEXT NOT NULL DEFAULT 'in_store',
      payment_method TEXT NOT NULL DEFAULT '',
      total_dzd      DOUBLE PRECISION NOT NULL DEFAULT 0,
      discount_dzd   DOUBLE PRECISION NOT NULL DEFAULT 0,
      items          JSONB NOT NULL DEFAULT '[]'::jsonb,
      sold_at        TIMESTAMPTZ,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (shop_id, local_sale_id)
    );
    CREATE INDEX IF NOT EXISTS pos_sales_sold_idx    ON pos_sales (shop_id, sold_at DESC);
    CREATE INDEX IF NOT EXISTS pos_products_type_idx ON pos_products (shop_id, product_type);
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS variant_id       BIGINT;
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS variant_qty      INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS variant_lines    JSONB NOT NULL DEFAULT '[]'::jsonb;
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_shop_id INTEGER;
    ALTER TABLE orders ADD COLUMN IF NOT EXISTS pos_status       TEXT NOT NULL DEFAULT '';
    CREATE INDEX IF NOT EXISTS orders_pos_open_idx ON orders (assigned_shop_id, pos_status)
      WHERE pos_status IN ('assigned','received');
  `).catch(err => {
    console.error('[pos-db] schema init failed:', err.message);
    schemaReady = null;
    throw err;
  });
  return schemaReady;
}

async function db() {
  const p = getPool();
  if (!p) return null;
  await ensureSchema(p);
  return p;
}

const num  = (v, d = 0) => (Number.isFinite(Number(v)) ? Number(v) : d);
const text = (v, max = 200) => String(v ?? '').trim().slice(0, max);

// Open pos_status values that hold a stock reservation.
const OPEN_POS = ['assigned', 'received'];

// ── Shop auth ────────────────────────────────────────────────────────────────

/** Resolve a sync token to its active shop row, or null. */
export async function authShopByToken(token) {
  try {
    const p = await db();
    if (!p || !token) return null;
    const { rows } = await p.query(
      `SELECT s.*, b.name AS brand_name FROM pos_shops s
        JOIN pos_brands b ON b.id = s.brand_id
       WHERE s.sync_token = $1 AND s.active`,
      [text(token, 100)]
    );
    return rows[0] || null;
  } catch (err) {
    console.error('[pos-db] authShopByToken failed:', err.message);
    return null;
  }
}

// ── Brands / shops CRUD (admin) ──────────────────────────────────────────────

export async function upsertBrand({ id = null, name, ownerName = '', ourSharePct = 100, logoUrl = '' }) {
  const p = await db();
  if (!p) return null;
  if (id) {
    const { rows } = await p.query(
      `UPDATE pos_brands SET name=$2, owner_name=$3, our_share_pct=$4, logo_url=$5 WHERE id=$1 RETURNING *`,
      [num(id), text(name), text(ownerName), Math.min(Math.max(num(ourSharePct, 100), 0), 100), text(logoUrl, 1000)]
    );
    return rows[0] || null;
  }
  const { rows } = await p.query(
    `INSERT INTO pos_brands (name, owner_name, our_share_pct, logo_url)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (name) DO UPDATE SET owner_name=EXCLUDED.owner_name,
       our_share_pct=EXCLUDED.our_share_pct, logo_url=EXCLUDED.logo_url
     RETURNING *`,
    [text(name), text(ownerName), Math.min(Math.max(num(ourSharePct, 100), 0), 100), text(logoUrl, 1000)]
  );
  return rows[0] || null;
}

export async function upsertShop({ id = null, brandId, name, logoUrl = '', syncIntervalSec = 300, routingPriority = 100, active = true }) {
  const p = await db();
  if (!p) return null;
  const interval = Math.min(Math.max(num(syncIntervalSec, 300), 60), 3600);
  if (id) {
    const { rows } = await p.query(
      `UPDATE pos_shops SET brand_id=$2, name=$3, logo_url=$4, sync_interval_sec=$5,
              routing_priority=$6, active=$7 WHERE id=$1 RETURNING *`,
      [num(id), num(brandId), text(name), text(logoUrl, 1000), interval, num(routingPriority, 100), !!active]
    );
    return rows[0] || null;
  }
  // New shop → mint its sync token. Returned ONCE here; store it in the
  // local ERP's cloud-sync settings.
  const tokenValue = crypto.randomBytes(24).toString('base64url');
  const { rows } = await p.query(
    `INSERT INTO pos_shops (brand_id, name, logo_url, sync_interval_sec, routing_priority, active, sync_token)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
    [num(brandId), text(name), text(logoUrl, 1000), interval, num(routingPriority, 100), !!active, tokenValue]
  );
  return rows[0] || null;
}

export async function rotateShopToken(shopId) {
  const p = await db();
  if (!p) return null;
  const tokenValue = crypto.randomBytes(24).toString('base64url');
  const { rows } = await p.query(
    `UPDATE pos_shops SET sync_token=$2 WHERE id=$1 RETURNING *`,
    [num(shopId), tokenValue]
  );
  return rows[0] || null;
}

/** Brands + shops with sync freshness + mirror row counts (admin overview). */
export async function listBrandsAndShops() {
  const p = await db();
  if (!p) return { brands: [], shops: [] };
  const [brands, shops] = await Promise.all([
    p.query(`SELECT * FROM pos_brands ORDER BY id`),
    p.query(
      `SELECT s.id, s.brand_id, s.name, s.logo_url, s.sync_interval_sec, s.routing_priority,
              s.active, s.last_sync_at, s.created_at,
              (SELECT COUNT(*)::int FROM pos_products pp WHERE pp.shop_id = s.id)  AS product_count,
              (SELECT COUNT(*)::int FROM pos_variant_map m WHERE m.shop_id = s.id) AS mapping_count,
              (SELECT COUNT(*)::int FROM orders o
                WHERE o.assigned_shop_id = s.id AND o.pos_status = ANY($1)) AS open_orders
         FROM pos_shops s ORDER BY s.routing_priority, s.id`,
      [OPEN_POS]
    ),
  ]);
  // sync_token intentionally not selected — it is shown once at creation only.
  return { brands: brands.rows, shops: shops.rows };
}

// ── Variant mapping ──────────────────────────────────────────────────────────

export async function upsertVariantMap({ shopifyVariantId, shopId, productUuid, qtyPerUnit = 1 }) {
  const p = await db();
  if (!p) return false;
  await p.query(
    `INSERT INTO pos_variant_map (shopify_variant_id, shop_id, product_uuid, qty_per_unit)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (shopify_variant_id, shop_id)
       DO UPDATE SET product_uuid=EXCLUDED.product_uuid, qty_per_unit=EXCLUDED.qty_per_unit`,
    [num(shopifyVariantId), num(shopId), text(productUuid, 80), Math.max(num(qtyPerUnit, 1), 0.01)]
  );
  return true;
}

export async function deleteVariantMap(shopifyVariantId, shopId) {
  const p = await db();
  if (!p) return false;
  const r = await p.query(
    `DELETE FROM pos_variant_map WHERE shopify_variant_id=$1 AND shop_id=$2`,
    [num(shopifyVariantId), num(shopId)]
  );
  return r.rowCount > 0;
}

/** All mappings joined with the mirrored product (name/stock) for the admin UI. */
export async function listVariantMaps({ shopId = null } = {}) {
  const p = await db();
  if (!p) return [];
  const { rows } = await p.query(
    `SELECT m.shopify_variant_id, m.shop_id, m.product_uuid, m.qty_per_unit,
            s.name AS shop_name, pp.name AS product_name, pp.product_type,
            pp.stock_qty, pp.sell_price_dzd
       FROM pos_variant_map m
       JOIN pos_shops s ON s.id = m.shop_id
       LEFT JOIN pos_products pp ON pp.shop_id = m.shop_id AND pp.uuid = m.product_uuid
      WHERE ($1::int IS NULL OR m.shop_id = $1)
      ORDER BY m.shopify_variant_id, m.shop_id`,
    [shopId ? num(shopId) : null]
  );
  return rows;
}

// ── Tick payload appliers ────────────────────────────────────────────────────

/** Upsert a batch of stock rows for one shop (multi-VALUES, one query). */
export async function applyStockBatch(shopId, rows) {
  const p = await db();
  if (!p || !Array.isArray(rows) || !rows.length) return 0;
  const batch = rows.slice(0, 500);
  const vals = [];
  const params = [num(shopId)];
  for (const r of batch) {
    const u = text(r?.uuid, 80);
    if (!u) continue;
    const base = params.length;
    params.push(u, text(r?.type, 40), text(r?.name, 300), text(r?.brand, 120),
      text(r?.category, 120), num(r?.sellPriceDzd), num(r?.qty));
    vals.push(`($1, $${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, now())`);
  }
  if (!vals.length) return 0;
  const r = await p.query(
    `INSERT INTO pos_products (shop_id, uuid, product_type, name, brand, category, sell_price_dzd, stock_qty, updated_at)
     VALUES ${vals.join(',')}
     ON CONFLICT (shop_id, uuid) DO UPDATE SET
       product_type=EXCLUDED.product_type, name=EXCLUDED.name, brand=EXCLUDED.brand,
       category=EXCLUDED.category, sell_price_dzd=EXCLUDED.sell_price_dzd,
       stock_qty=EXCLUDED.stock_qty, updated_at=now()`,
    params
  );
  return r.rowCount;
}

/** Remove mirror rows the ERP says no longer exist locally (deleted products). */
export async function pruneStock(shopId, uuids) {
  const p = await db();
  if (!p || !Array.isArray(uuids) || !uuids.length) return 0;
  const r = await p.query(
    `DELETE FROM pos_products WHERE shop_id=$1 AND uuid = ANY($2)`,
    [num(shopId), uuids.slice(0, 500).map(u => text(u, 80))]
  );
  return r.rowCount;
}

/** Upsert a batch of local sales (idempotent on shop_id + local_sale_id). */
export async function applySalesBatch(shopId, rows) {
  const p = await db();
  if (!p || !Array.isArray(rows) || !rows.length) return 0;
  const batch = rows.slice(0, 200);
  const vals = [];
  const params = [num(shopId)];
  for (const r of batch) {
    const saleId = num(r?.id, 0);
    if (!saleId) continue;
    const base = params.length;
    const soldAt = r?.soldAt ? new Date(r.soldAt) : null;
    params.push(saleId, text(r?.status, 40), text(r?.channel, 40), text(r?.paymentMethod, 40),
      num(r?.totalDzd), num(r?.discountDzd),
      JSON.stringify(Array.isArray(r?.items) ? r.items.slice(0, 100) : []),
      soldAt && !isNaN(soldAt) ? soldAt.toISOString() : null);
    vals.push(`($1, $${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}::jsonb, $${base + 8})`);
  }
  if (!vals.length) return 0;
  const r = await p.query(
    `INSERT INTO pos_sales (shop_id, local_sale_id, status, channel, payment_method, total_dzd, discount_dzd, items, sold_at)
     VALUES ${vals.join(',')}
     ON CONFLICT (shop_id, local_sale_id) DO UPDATE SET
       status=EXCLUDED.status, channel=EXCLUDED.channel, payment_method=EXCLUDED.payment_method,
       total_dzd=EXCLUDED.total_dzd, discount_dzd=EXCLUDED.discount_dzd,
       items=EXCLUDED.items, sold_at=EXCLUDED.sold_at`,
    params
  );
  return r.rowCount;
}

/** Highest local sale id the cloud already has — lets a fresh ERP install resume. */
export async function maxSaleId(shopId) {
  const p = await db();
  if (!p) return 0;
  const { rows } = await p.query(
    `SELECT COALESCE(MAX(local_sale_id), 0)::int AS m FROM pos_sales WHERE shop_id=$1`, [num(shopId)]
  );
  return rows[0]?.m || 0;
}

export async function touchShopSync(shopId) {
  const p = await db();
  if (!p) return;
  await p.query(`UPDATE pos_shops SET last_sync_at=now() WHERE id=$1`, [num(shopId)]);
}

// ── Order routing (web order → which shop fulfils it) ────────────────────────

/**
 * Assign one web order to a shop. `lines` are the fulfillment lines
 * ([{ variantId, quantity }]) — the same validated lineItems the order path
 * sends to Shopify. Walks shops by routing_priority; the first shop that maps
 * EVERY line's variant and has enough AVAILABLE stock for every line (mirror
 * qty minus reservations held by other open web orders' variant_lines) wins.
 * If nothing verifies, falls back to the first active shop so the order is
 * never stranded — flagged so staff see stock wasn't confirmed.
 * Best-effort: called fire-and-forget from the order path.
 */
export async function assignOrderToShop({ ref, lines = [], variantId = null, quantity = 1 }) {
  try {
    const p = await db();
    if (!p || !ref) return null;

    // Skip if already assigned (idempotent replays).
    const cur = await p.query(`SELECT pos_status FROM orders WHERE ref=$1`, [text(ref, 60)]);
    if (!cur.rows.length || cur.rows[0].pos_status !== '') return null;

    // Normalize lines; aggregate duplicates by variant. Falls back to the
    // single main variant for callers that predate per-line routing.
    const byVariant = new Map();
    const rawLines = Array.isArray(lines) && lines.length
      ? lines
      : (variantId ? [{ variantId, quantity }] : []);
    for (const l of rawLines.slice(0, 10)) {
      const v = num(l?.variantId ?? l?.variant_id);
      if (!(v > 0)) continue;
      const q = Math.max(num(l?.quantity, 1), 1);
      byVariant.set(v, (byVariant.get(v) || 0) + q);
    }
    const needed = [...byVariant.entries()].map(([v, q]) => ({ variantId: v, qty: q }));
    const variantIds = needed.map(l => l.variantId);

    let chosen = null;
    let verified = false;

    if (variantIds.length) {
      // One query: every (shop, variant) pair that is mapped, with mirror
      // stock and the quantity reserved by other open orders' variant_lines.
      const { rows } = await p.query(
        `SELECT s.id AS shop_id, s.routing_priority, m.shopify_variant_id,
                m.qty_per_unit, pp.stock_qty, COALESCE(r.reserved, 0) AS reserved
           FROM pos_shops s
           JOIN pos_variant_map m ON m.shop_id = s.id AND m.shopify_variant_id = ANY($1)
           JOIN pos_products pp   ON pp.shop_id = s.id AND pp.uuid = m.product_uuid
           LEFT JOIN LATERAL (
             SELECT SUM((vl->>'quantity')::float * m.qty_per_unit) AS reserved
               FROM orders o, jsonb_array_elements(o.variant_lines) vl
              WHERE o.assigned_shop_id = s.id AND o.pos_status = ANY($2)
                AND (vl->>'variantId')::bigint = m.shopify_variant_id
           ) r ON true
          WHERE s.active
          ORDER BY s.routing_priority, s.id`,
        [variantIds, OPEN_POS]
      );

      // Group by shop, preserving routing order.
      const shopOrder = [];
      const perShop = new Map();
      for (const r of rows) {
        if (!perShop.has(r.shop_id)) { perShop.set(r.shop_id, new Map()); shopOrder.push(r.shop_id); }
        perShop.get(r.shop_id).set(Number(r.shopify_variant_id), {
          available: Number(r.stock_qty) - Number(r.reserved),
          qtyPer: Number(r.qty_per_unit),
        });
      }
      for (const shopId of shopOrder) {
        const varMap = perShop.get(shopId);
        const coversAll = needed.every(l => {
          const e = varMap.get(l.variantId);
          return e && e.available >= l.qty * e.qtyPer;
        });
        if (coversAll) { chosen = shopId; verified = true; break; }
      }
    }

    if (!chosen) {
      const { rows } = await p.query(
        `SELECT id FROM pos_shops WHERE active ORDER BY routing_priority, id LIMIT 1`
      );
      chosen = rows[0]?.id || null;
    }
    if (!chosen) return null;

    const mainLine = needed[0] || { variantId: null, qty: 1 };
    await p.query(
      `UPDATE orders SET assigned_shop_id=$2, variant_id=$3, variant_qty=$5,
              variant_lines=$6::jsonb, pos_status='assigned',
              note = CASE WHEN $4 OR COALESCE(note,'') LIKE '%STOCK-UNVERIFIED%' THEN note
                          ELSE CASE WHEN COALESCE(note,'')='' THEN 'STOCK-UNVERIFIED'
                                    ELSE note || ' | STOCK-UNVERIFIED' END END,
              updated_at=now()
        WHERE ref=$1 AND pos_status=''`,
      [text(ref, 60), chosen, mainLine.variantId, verified, mainLine.qty, JSON.stringify(needed.map(l => ({ variantId: l.variantId, quantity: l.qty })))]
    );
    return { shopId: chosen, verified };
  } catch (err) {
    console.error('[pos-db] assignOrderToShop failed:', err.message, `(ref ${ref})`);
    return null;
  }
}

/**
 * Sweep: assign orders the creation-time hook missed. DELIBERATELY narrow —
 * only orders from the last 2 hours, i.e. ones created while the pos system
 * was already live. Anything older (including everything that predates the
 * first shop) is NEVER auto-routed: it surfaces in /admin/pos as "unrouted"
 * and staff explicitly choose assign / ignore / cancel (bulkRoutePending).
 */
export async function assignPendingOrders(limit = 20) {
  try {
    const p = await db();
    if (!p) return 0;
    const { rows } = await p.query(
      `SELECT ref, variant_id, variant_lines, items FROM orders
        WHERE pos_status='' AND status='pending'
          AND created_at > now() - interval '2 hours'
        ORDER BY created_at LIMIT $1`,
      [Math.min(num(limit, 20), 100)]
    );
    let n = 0;
    for (const o of rows) {
      const qty = Array.isArray(o.items)
        ? o.items.reduce((s, i) => s + (num(i?.quantity, 1) || 1), 0) || 1
        : 1;
      const lines = Array.isArray(o.variant_lines) ? o.variant_lines : [];
      const r = await assignOrderToShop({ ref: o.ref, lines, variantId: o.variant_id, quantity: qty });
      if (r) n++;
    }
    return n;
  } catch (err) {
    console.error('[pos-db] assignPendingOrders failed:', err.message);
    return 0;
  }
}

/**
 * Open web orders for a shop's tick. Everything not yet confirmed/cancelled
 * is re-sent every tick (the ERP upserts by ref, so re-delivery is safe);
 * the ERP acks with orderUpdates status 'received' to slim the payload.
 * Each variant line is enriched with the shop's local product (via
 * pos_variant_map + pos_products) — the POS holds no mapping of its own,
 * so this resolution is what lets staff confirm the order into a local sale.
 */
export async function getOpenOrdersForShop(shopId) {
  const p = await db();
  if (!p) return [];
  const { rows } = await p.query(
    `SELECT ref, created_at, status, pos_status, name, phone, wilaya, baladiya, address,
            delivery_type, shipping_cost, items, merch_total_dzd, total_dzd, note,
            variant_id, variant_lines
       FROM orders
      WHERE assigned_shop_id=$1 AND pos_status='assigned'
      ORDER BY created_at LIMIT 50`,
    [num(shopId)]
  );
  if (!rows.length) return rows;

  const variantIds = [...new Set(rows.flatMap(o =>
    (Array.isArray(o.variant_lines) ? o.variant_lines : []).map(l => num(l?.variantId)).filter(v => v > 0)
  ))];
  if (variantIds.length) {
    const res = await p.query(
      `SELECT m.shopify_variant_id, m.product_uuid, m.qty_per_unit,
              pp.name, pp.product_type, pp.sell_price_dzd
         FROM pos_variant_map m
         LEFT JOIN pos_products pp ON pp.shop_id = m.shop_id AND pp.uuid = m.product_uuid
        WHERE m.shop_id = $1 AND m.shopify_variant_id = ANY($2)`,
      [num(shopId), variantIds]
    );
    const byVariant = new Map(res.rows.map(r => [Number(r.shopify_variant_id), r]));
    for (const o of rows) {
      o.variant_lines = (Array.isArray(o.variant_lines) ? o.variant_lines : []).map(l => {
        const m = byVariant.get(num(l?.variantId));
        return {
          variantId: num(l?.variantId), quantity: Math.max(num(l?.quantity, 1), 1),
          productUuid: m?.product_uuid || null,
          productType: m?.product_type || null,
          productName: m?.name || null,
          qtyPerUnit:  m ? Number(m.qty_per_unit) : 1,
          sellPriceDzd: m ? Number(m.sell_price_dzd) : 0,
        };
      });
    }
  }
  return rows;
}

/** Apply order lifecycle acks pushed up by the ERP. */
export async function applyOrderUpdates(shopId, updates) {
  const p = await db();
  if (!p || !Array.isArray(updates) || !updates.length) return 0;
  const allowed = new Set(['received', 'confirmed', 'cancelled']);
  let n = 0;
  for (const u of updates.slice(0, 100)) {
    const ref = text(u?.ref, 60);
    const st  = text(u?.status, 20);
    if (!ref || !allowed.has(st)) continue;
    const r = await p.query(
      `UPDATE orders SET pos_status=$3, updated_at=now()
        WHERE ref=$1 AND assigned_shop_id=$2 AND pos_status <> $3`,
      [ref, num(shopId), st]
    );
    n += r.rowCount;
  }
  return n;
}

/**
 * Legacy/unrouted pending orders — anything still status='pending' that no
 * shop was assigned to and the 2h auto-window skipped. Staff decide their
 * fate explicitly in /admin/pos; nothing automatic ever touches them.
 */
export async function listUnrouted() {
  const p = await db();
  if (!p) return { count: 0, oldest: null, sample: [] };
  const { rows } = await p.query(
    `SELECT ref, created_at, name, phone, total_dzd FROM orders
      WHERE pos_status='' AND status='pending'
        AND created_at <= now() - interval '2 hours'
      ORDER BY created_at DESC LIMIT 500`
  );
  return {
    count: rows.length,
    oldest: rows.length ? rows[rows.length - 1].created_at : null,
    sample: rows.slice(0, 10),
  };
}

/**
 * One explicit bulk decision for the unrouted backlog:
 *   'assign' → route them all to the chosen shop (POS queue; lines without
 *              variant data show as display items and can't auto-confirm —
 *              staff handle or cancel them there)
 *   'ignore' → pos_status='ignored': never routed, never reserved; the order
 *              stays fully manageable in /admin/orders as before
 *   'cancel' → orders.status='cancelled' + pos_status='ignored'
 * Only touches rows matching the exact listUnrouted() window. Returns count.
 */
export async function bulkRoutePending(action, shopId = null) {
  const p = await db();
  if (!p) return 0;
  const where = `pos_status='' AND status='pending' AND created_at <= now() - interval '2 hours'`;
  let r;
  if (action === 'assign' && num(shopId) > 0) {
    r = await p.query(
      `UPDATE orders SET assigned_shop_id=$1, pos_status='assigned', updated_at=now() WHERE ${where}`,
      [num(shopId)]
    );
  } else if (action === 'ignore') {
    r = await p.query(`UPDATE orders SET pos_status='ignored', updated_at=now() WHERE ${where}`);
  } else if (action === 'cancel') {
    r = await p.query(
      `UPDATE orders SET status='cancelled', pos_status='ignored', updated_at=now() WHERE ${where}`
    );
  } else {
    return 0;
  }
  console.log(`[pos-db] bulkRoutePending ${action}${shopId ? ` shop ${shopId}` : ''}: ${r.rowCount} order(s)`);
  return r.rowCount;
}

/** Manual reassign from the admin (e.g. shop is out of stock after all). */
export async function reassignOrder(ref, shopId) {
  const p = await db();
  if (!p) return false;
  const r = await p.query(
    `UPDATE orders SET assigned_shop_id=$2, pos_status='assigned', updated_at=now()
      WHERE ref=$1 AND pos_status IN ('', 'assigned', 'received', 'cancelled')`,
    [text(ref, 60), num(shopId)]
  );
  return r.rowCount > 0;
}

// ── Daily reconciliation sweep (called from the server cron) ─────────────────

/**
 * Keeps the reservation model honest and surfaces drift:
 *  1. Frees reservations held by orders staff cancelled/returned in the
 *     orders admin (their pos_status would otherwise reserve stock forever).
 *  2. Logs shops whose last sync is >24h old (POS offline / token broken).
 *  3. Logs orders stuck unconfirmed >48h (shop is not working its queue).
 *  4. Logs mappings pointing at products missing from the pushed stock
 *     (deleted locally after being mapped).
 * Log-only beyond (1) — staff act from /admin/pos; nothing destructive.
 */
export async function posDailySweep() {
  try {
    const p = await db();
    if (!p) return null;
    const released = await p.query(
      `UPDATE orders SET pos_status='cancelled', updated_at=now()
        WHERE pos_status = ANY($1) AND status IN ('cancelled','returned')`,
      [OPEN_POS]
    );
    const [staleShops, stuckOrders, brokenMaps] = await Promise.all([
      p.query(`SELECT name, last_sync_at FROM pos_shops
                WHERE active AND (last_sync_at IS NULL OR last_sync_at < now() - interval '24 hours')`),
      p.query(`SELECT ref, assigned_shop_id, created_at FROM orders
                WHERE pos_status = ANY($1) AND created_at < now() - interval '48 hours'
                ORDER BY created_at LIMIT 50`, [OPEN_POS]),
      p.query(`SELECT m.shopify_variant_id, m.shop_id FROM pos_variant_map m
                LEFT JOIN pos_products pp ON pp.shop_id = m.shop_id AND pp.uuid = m.product_uuid
                WHERE pp.uuid IS NULL`),
    ]);
    if (released.rowCount) console.log(`[pos-sweep] released ${released.rowCount} reservation(s) from cancelled orders`);
    for (const s of staleShops.rows) console.warn(`[pos-sweep] shop "${s.name}" has not synced since ${s.last_sync_at || 'ever'}`);
    for (const o of stuckOrders.rows) console.warn(`[pos-sweep] order ${o.ref} unconfirmed >48h (shop ${o.assigned_shop_id})`);
    for (const m of brokenMaps.rows) console.warn(`[pos-sweep] mapping variant ${m.shopify_variant_id} (shop ${m.shop_id}) points at a missing product`);
    return {
      released: released.rowCount,
      staleShops: staleShops.rows.length,
      stuckOrders: stuckOrders.rows.length,
      brokenMaps: brokenMaps.rows.length,
    };
  } catch (err) {
    console.error('[pos-db] posDailySweep failed:', err.message);
    return null;
  }
}

// ── Stock + analytics reads (admin) ──────────────────────────────────────────

export async function listStock({ shopId = null, type = '', q = '', limit = 300 } = {}) {
  const p = await db();
  if (!p) return [];
  const { rows } = await p.query(
    `SELECT pp.*, s.name AS shop_name, s.brand_id
       FROM pos_products pp JOIN pos_shops s ON s.id = pp.shop_id
      WHERE ($1::int IS NULL OR pp.shop_id=$1)
        AND ($2 = '' OR pp.product_type = $2)
        AND ($3 = '' OR pp.name ILIKE '%' || $3 || '%' OR pp.brand ILIKE '%' || $3 || '%')
      ORDER BY pp.name LIMIT $4`,
    [shopId ? num(shopId) : null, text(type, 40), text(q, 100), Math.min(num(limit, 300), 1000)]
  );
  return rows;
}

/**
 * Unified daily analytics: in-store POS revenue + web COD revenue side by
 * side, filterable to one shop, one brand, or everything. Two indexed
 * aggregate queries per page view — no raw row streaming.
 */
export async function getAnalytics({ scope = 'all', days = 30 } = {}) {
  const p = await db();
  if (!p) return { daily: [], totals: null };
  const d = Math.min(Math.max(num(days, 30), 1), 365);
  let shopFilter = null, brandFilter = null;
  const m = String(scope).match(/^(shop|brand):(\d+)$/);
  if (m) (m[1] === 'shop' ? (shopFilter = Number(m[2])) : (brandFilter = Number(m[2])));

  const [pos, web] = await Promise.all([
    p.query(
      `SELECT date_trunc('day', COALESCE(ps.sold_at, ps.created_at))::date AS day,
              COUNT(*)::int AS sales, SUM(ps.total_dzd) AS revenue
         FROM pos_sales ps JOIN pos_shops s ON s.id = ps.shop_id
        WHERE COALESCE(ps.sold_at, ps.created_at) > now() - ($1 || ' days')::interval
          AND ps.status NOT IN ('cancelled','refunded')
          AND ($2::int IS NULL OR ps.shop_id=$2)
          AND ($3::int IS NULL OR s.brand_id=$3)
        GROUP BY 1 ORDER BY 1`,
      [String(d), shopFilter, brandFilter]
    ),
    p.query(
      `SELECT date_trunc('day', o.created_at)::date AS day,
              COUNT(*)::int AS orders, SUM(o.total_dzd) AS revenue
         FROM orders o
         LEFT JOIN pos_shops s ON s.id = o.assigned_shop_id
        WHERE o.created_at > now() - ($1 || ' days')::interval
          AND o.status NOT IN ('cancelled','returned')
          AND ($2::int IS NULL OR o.assigned_shop_id=$2)
          AND ($3::int IS NULL OR s.brand_id=$3)
        GROUP BY 1 ORDER BY 1`,
      [String(d), shopFilter, brandFilter]
    ),
  ]);

  const byDay = new Map();
  for (const r of pos.rows) byDay.set(String(r.day), { day: String(r.day), posSales: r.sales, posRevenue: Number(r.revenue) || 0, webOrders: 0, webRevenue: 0 });
  for (const r of web.rows) {
    const k = String(r.day);
    const row = byDay.get(k) || { day: k, posSales: 0, posRevenue: 0, webOrders: 0, webRevenue: 0 };
    row.webOrders = r.orders;
    row.webRevenue = Number(r.revenue) || 0;
    byDay.set(k, row);
  }
  const daily = [...byDay.values()].sort((a, b) => a.day.localeCompare(b.day));
  const totals = daily.reduce((t, r) => ({
    posSales: t.posSales + r.posSales, posRevenue: t.posRevenue + r.posRevenue,
    webOrders: t.webOrders + r.webOrders, webRevenue: t.webRevenue + r.webRevenue,
  }), { posSales: 0, posRevenue: 0, webOrders: 0, webRevenue: 0 });
  return { daily, totals };
}
