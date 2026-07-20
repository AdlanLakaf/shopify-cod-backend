// ============================================================
//  Online catalog — the brand's web-facing product list.
//
//  This is where the two prices live side by side. A local stock
//  row has ONE local price (what the shop sells it for) and the
//  web has its own (delivery, ads, different margin). Nothing in
//  this file ever writes a local price; the overlay
//  (pos_online_products) is a separate layer keyed by SKU.
//
//  Expansion: local stock is not the same shape as the web's.
//    original            → 1 sellable unit  (SKU uuid)
//    extrait / shopMade  → one per matrix volume of its tier
//    decant              → one per the brand's decant volume list
//  …because only originals are discrete bottles; the rest are bulk
//  pools measured in millilitres.
//
//  Local price per volume, by type:
//    original   → the row's own sell price
//    extrait /  → the matrix cell (tier × volume). No cell = the
//    shopMade     grid is sparse: reported, never guessed.
//    decant     → sell_price is per 10ml, so /10 × volume
//
//  Stock is summed across the brand's shops (the web sells the
//  brand, not one counter); per-shop routing stays where it was.
// ============================================================

import { getPool } from './_pg.js';
import { makeSku } from './_shopify-write.js';

const num = (v, d = 0) => (Number.isFinite(Number(v)) ? Number(v) : d);

/** Bulk types are pools; only `original` is a discrete unit. */
const isBulk = type => type === 'extrait' || type === 'shopMade' || type === 'decant';

/**
 * Local price for one sellable unit. Returns { price, issue } — `issue`
 * carries WHY a price is unknown so the admin can show the row greyed with
 * a reason instead of silently pricing it at zero.
 */
export function localPriceFor(row, volumeMl, matrixByCell) {
  if (row.product_type === 'original') return { price: num(row.sell_price_dzd), issue: '' };

  if (row.product_type === 'decant') {
    // Stored per 10ml.
    const perMl = num(row.sell_price_dzd) / 10;
    if (!perMl) return { price: 0, issue: 'no local decant price' };
    return { price: Math.round(perMl * volumeMl), issue: '' };
  }

  // extrait / shopMade → the tier × volume matrix cell.
  if (!row.price_category_id) return { price: 0, issue: 'no quality tier set' };
  const cell = matrixByCell.get(`${row.price_category_id}|${volumeMl}`);
  if (cell == null) return { price: 0, issue: `no matrix cell for ${volumeMl}ml in tier ${row.price_category_id}` };
  return { price: Math.round(cell), issue: '' };
}

/**
 * The volumes one stock row can be sold in online.
 * Bulk rows get the candidate list; originals get their own single volume.
 */
function volumesFor(row, matrixVolumesByTier, decantVolumes) {
  if (row.product_type === 'original') return [row.volume_ml == null ? null : num(row.volume_ml)];
  if (row.product_type === 'decant') return decantVolumes;
  return matrixVolumesByTier.get(row.price_category_id) || [];
}

/**
 * Full online catalog for a brand: every sellable unit, with BOTH prices,
 * the overlay's name/status, and its Shopify link if published.
 *
 * filters: { type, q, status, published: 'yes'|'no'|'', gender, limit }
 */
export async function listOnlineCatalog(brandId, filters = {}) {
  const p = getPool();
  if (!p) return { rows: [], issues: [] };
  const bid = num(brandId);
  if (!bid) return { rows: [], issues: [] };

  const limit = Math.min(Math.max(num(filters.limit, 500), 1), 2000);

  const [stockRes, catRes, matrixRes, overlayRes, brandRes] = await Promise.all([
    // One row per uuid for the whole brand: stock summed across its shops,
    // descriptive fields taken from any shop (they come from the same
    // perfume record, so they agree).
    p.query(
      `SELECT pp.uuid, MAX(pp.product_type) AS product_type, MAX(pp.name) AS name,
              MAX(pp.brand) AS brand, MAX(pp.category) AS category, MAX(pp.gender) AS gender,
              MAX(pp.volume_ml) AS volume_ml, MAX(pp.price_category_id) AS price_category_id,
              MAX(pp.perfume_id) AS perfume_id,
              MAX(pp.sell_price_dzd) AS sell_price_dzd,
              SUM(pp.stock_qty) AS stock_qty,
              COUNT(*)::int AS shop_count
         FROM pos_products pp
         JOIN pos_shops s ON s.id = pp.shop_id
        WHERE s.brand_id = $1
        GROUP BY pp.uuid`,
      [bid]
    ),
    p.query(`SELECT local_id, name, product_type FROM pos_price_categories WHERE brand_id=$1`, [bid]),
    p.query(`SELECT category_id, volume_ml, sell_price FROM pos_price_matrix WHERE brand_id=$1`, [bid]),
    p.query(`SELECT * FROM pos_online_products WHERE brand_id=$1`, [bid]),
    p.query(`SELECT decant_volumes FROM pos_brands WHERE id=$1`, [bid]),
  ]);

  const tierName = new Map(catRes.rows.map(c => [c.local_id, c.name]));
  const matrixByCell = new Map();
  const matrixVolumesByTier = new Map();
  for (const c of matrixRes.rows) {
    matrixByCell.set(`${c.category_id}|${num(c.volume_ml)}`, num(c.sell_price));
    if (!matrixVolumesByTier.has(c.category_id)) matrixVolumesByTier.set(c.category_id, []);
    matrixVolumesByTier.get(c.category_id).push(num(c.volume_ml));
  }
  for (const list of matrixVolumesByTier.values()) list.sort((a, b) => a - b);

  const decantVolumes = String(brandRes.rows[0]?.decant_volumes || '')
    .split(',').map(v => num(v)).filter(v => v > 0).sort((a, b) => a - b);

  const overlay = new Map(overlayRes.rows.map(o => [o.sku, o]));

  const rows = [];
  const issues = [];
  for (const r of stockRes.rows) {
    for (const vol of volumesFor(r, matrixVolumesByTier, decantVolumes)) {
      const sku = makeSku(r.uuid, isBulk(r.product_type) ? vol : null);
      if (!sku) continue;
      const { price: localPrice, issue } = localPriceFor(r, num(vol), matrixByCell);
      if (issue) issues.push({ sku, name: r.name, issue });

      const o = overlay.get(sku) || null;
      rows.push({
        sku,
        uuid: r.uuid,
        type: r.product_type,
        volumeMl: vol == null ? null : num(vol),
        localName: r.name,
        brand: r.brand,
        category: r.category,
        gender: r.gender || '',
        tierId: r.price_category_id || null,
        tierName: tierName.get(r.price_category_id) || '',
        stockQty: num(r.stock_qty),
        shopCount: r.shop_count,
        // The two prices, always both present.
        localPriceDzd: localPrice,
        onlinePriceDzd: o?.online_price_dzd == null ? null : num(o.online_price_dzd),
        prevPriceDzd: o?.prev_price_dzd == null ? null : num(o.prev_price_dzd),
        onlineName: o?.online_name || '',
        status: o?.status || 'draft',
        priceOverridden: !!o?.price_overridden,
        nameOverridden: !!o?.name_overridden,
        shopifyProductId: o?.shopify_product_id ? String(o.shopify_product_id) : null,
        shopifyVariantId: o?.shopify_variant_id ? String(o.shopify_variant_id) : null,
        published: !!o?.shopify_variant_id,
        priceIssue: issue,
      });
    }
  }

  // Filters applied after expansion — they act on sellable units, not stock rows.
  const q = String(filters.q || '').trim().toLowerCase();
  const filtered = rows.filter(r => {
    if (filters.type && r.type !== filters.type) return false;
    if (filters.gender && r.gender !== filters.gender) return false;
    if (filters.status && r.status !== filters.status) return false;
    if (filters.published === 'yes' && !r.published) return false;
    if (filters.published === 'no' && r.published) return false;
    if (q && !(`${r.localName} ${r.onlineName} ${r.brand} ${r.sku}`.toLowerCase().includes(q))) return false;
    return true;
  });

  return {
    rows: filtered.slice(0, limit),
    total: filtered.length,
    issues: issues.slice(0, 200),
    decantVolumes,
  };
}

/**
 * Edit one online row by hand. Setting a name or price marks it OVERRIDDEN,
 * which is what makes later bulk rules leave it alone — a human decision
 * outranks a rule until someone explicitly says otherwise.
 */
export async function updateOnlineProduct(brandId, sku, fields = {}) {
  const p = getPool();
  if (!p) return { ok: false, error: 'no database' };
  const bid = num(brandId);
  const s = String(sku || '').trim();
  if (!bid || !s) return { ok: false, error: 'brandId and sku are required' };

  const sets = [], params = [bid, s];
  const add = (col, val) => { params.push(val); sets.push(`${col}=$${params.length}`); };

  if (fields.onlineName !== undefined) {
    add('online_name', String(fields.onlineName).slice(0, 300));
    add('name_overridden', String(fields.onlineName).trim() !== '');
  }
  if (fields.onlinePriceDzd !== undefined) {
    const price = fields.onlinePriceDzd === null ? null : num(fields.onlinePriceDzd);
    sets.push(`prev_price_dzd = pos_online_products.online_price_dzd`);
    add('online_price_dzd', price);
    add('price_overridden', price !== null);
  }
  if (fields.status !== undefined) add('status', String(fields.status).slice(0, 20));
  if (fields.priceOverridden !== undefined) add('price_overridden', !!fields.priceOverridden);
  if (fields.nameOverridden !== undefined) add('name_overridden', !!fields.nameOverridden);
  if (!sets.length) return { ok: false, error: 'nothing to update' };

  const { uuid, volumeMl } = splitSku(s);
  try {
    await p.query(
      `INSERT INTO pos_online_products (brand_id, sku, product_uuid, volume_ml, updated_at)
       VALUES ($1,$2,$${params.length + 1},$${params.length + 2}, now())
       ON CONFLICT (brand_id, sku) DO NOTHING`,
      [...params, uuid, volumeMl]
    );
    await p.query(
      `UPDATE pos_online_products SET ${sets.join(', ')}, updated_at=now()
        WHERE brand_id=$1 AND sku=$2`,
      params
    );
    return { ok: true };
  } catch (err) {
    console.error('[online-catalog] updateOnlineProduct failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/** Local split so this module doesn't depend on the Shopify writer for reads. */
function splitSku(sku) {
  const at = String(sku).lastIndexOf('@');
  if (at < 1) return { uuid: sku, volumeMl: null };
  const v = Number(sku.slice(at + 1));
  return v > 0 ? { uuid: sku.slice(0, at), volumeMl: v } : { uuid: sku, volumeMl: null };
}

/** Flip status for many rows at once (publish/hide selections). */
export async function setOnlineStatus(brandId, skus = [], status = 'active') {
  const p = getPool();
  if (!p) return { ok: false, error: 'no database' };
  if (!Array.isArray(skus) || !skus.length) return { ok: false, error: 'no skus given' };
  const bid = num(brandId);
  const st = String(status).slice(0, 20);
  try {
    // Rows with no overlay yet must be created, so status can be set on a
    // product that has never been touched.
    const values = skus.slice(0, 2000).map((sku, i) => {
      const { uuid, volumeMl } = splitSku(String(sku));
      return { sku: String(sku), uuid, volumeMl, i };
    });
    const tuples = values.map((v, i) =>
      `($1, $${i * 3 + 3}, $${i * 3 + 4}, $${i * 3 + 5}, $2, now())`).join(',');
    const params = [bid, st, ...values.flatMap(v => [v.sku, v.uuid, v.volumeMl])];
    const r = await p.query(
      `INSERT INTO pos_online_products (brand_id, sku, product_uuid, volume_ml, status, updated_at)
       VALUES ${tuples}
       ON CONFLICT (brand_id, sku) DO UPDATE SET status=EXCLUDED.status, updated_at=now()`,
      params
    );
    return { ok: true, updated: r.rowCount };
  } catch (err) {
    console.error('[online-catalog] setOnlineStatus failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/** The brand's publishable decant volumes (they have no quality tier). */
export async function setDecantVolumes(brandId, csv) {
  const p = getPool();
  if (!p) return { ok: false, error: 'no database' };
  const clean = String(csv || '').split(',').map(v => num(v)).filter(v => v > 0)
    .sort((a, b) => a - b).join(',');
  try {
    await p.query(`UPDATE pos_brands SET decant_volumes=$2 WHERE id=$1`, [num(brandId), clean]);
    return { ok: true, decantVolumes: clean };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

/** Recent pricing runs, for the audit panel. */
export async function listPriceAudit(brandId, limit = 50) {
  const p = getPool();
  if (!p) return [];
  try {
    const { rows } = await p.query(
      `SELECT id, rule, actor, rows_count, created_at FROM pos_price_audit
        WHERE brand_id=$1 ORDER BY id DESC LIMIT $2`,
      [num(brandId), Math.min(num(limit, 50), 200)]
    );
    return rows;
  } catch { return []; }
}
