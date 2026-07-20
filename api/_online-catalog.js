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
