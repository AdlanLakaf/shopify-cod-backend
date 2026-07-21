// ============================================================
//  Online catalog — the brand's web-facing product list.
//
//  ONE ROW PER LOCAL PRODUCT (uuid), mirroring the ERP stock page
//  1:1 — originals, extrait, decants, shop-made, exactly as they
//  exist locally. NO volume expansion: a decant is one row (its
//  10ml base), not three; volume-level pricing is derived later at
//  sale / publish time, not stored as separate rows. This is what
//  keeps the list correct (extrait/shopMade are no longer hostage
//  to a synced matrix) and cheap (few rows, one query).
//
//  Two prices side by side. The local price is the shop's, mirrored
//  read-only; the online price is the web overlay (pos_online_products,
//  keyed by uuid). Nothing here writes a local price.
//
//  Core data comes from pos_perfumes (the parent perfume record) via
//  perfume_id — gender, description, accords, notes, image, etc.
//
//  Stock is summed across the brand's shops; the representative local
//  price is the catalog-source shop's, falling back to the max.
// ============================================================

import { getPool } from './_pg.js';

const num = (v, d = 0) => (Number.isFinite(Number(v)) ? Number(v) : d);
const parseJson = (v, d) => {
  if (v == null) return d;
  if (typeof v === 'object') return v;
  try { return JSON.parse(v); } catch { return d; }
};

/**
 * The brand catalog: one row per local product, both prices, the online
 * overlay, and the joined perfume core data.
 *
 * filters: { type, q, status, published: 'yes'|'no'|'', gender, limit }
 */
export async function listOnlineCatalog(brandId, filters = {}) {
  const p = getPool();
  if (!p) return { rows: [], total: 0 };
  const bid = num(brandId);
  if (!bid) return { rows: [], total: 0 };

  const limit = Math.min(Math.max(num(filters.limit, 800), 1), 3000);

  // One grouped query does the whole list: stock summed across the brand's
  // shops, the catalog-source shop's price kept as canonical, and the perfume
  // core data + Shopify thumbnail joined in. No per-row follow-up reads.
  const { rows: raw } = await p.query(
    `SELECT pp.uuid,
            MAX(pp.product_type)                                       AS product_type,
            MAX(pp.name)                                              AS name,
            MAX(pp.perfume_id)                                        AS perfume_id,
            SUM(pp.stock_qty)                                         AS stock_qty,
            COUNT(*)::int                                             AS shop_count,
            MAX(pp.sell_price_dzd)                                    AS any_price,
            MAX(pp.sell_price_dzd) FILTER (WHERE s.is_catalog_source) AS source_price,
            COALESCE(MAX(pf.name), MAX(pp.name))                      AS perfume_name,
            MAX(pf.brand)       AS brand,      MAX(pf.category) AS category,
            MAX(pf.gender)      AS gender,     MAX(pf.image_url) AS image_url,
            MAX(pf.description) AS description,
            o.online_name, o.online_price_dzd, o.prev_price_dzd, o.status,
            o.price_overridden, o.name_overridden, o.shopify_product_id, o.shopify_variant_id
       FROM pos_products pp
       JOIN pos_shops s        ON s.id = pp.shop_id
       LEFT JOIN pos_perfumes pf       ON pf.brand_id = s.brand_id AND pf.local_id = pp.perfume_id
       LEFT JOIN pos_online_products o ON o.brand_id  = s.brand_id AND o.sku       = pp.uuid
      WHERE s.brand_id = $1
      GROUP BY pp.uuid, o.online_name, o.online_price_dzd, o.prev_price_dzd, o.status,
               o.price_overridden, o.name_overridden, o.shopify_product_id, o.shopify_variant_id`,
    [bid]
  );

  const rows = raw.map(r => ({
    sku: r.uuid,                       // product-level key = the local uuid
    uuid: r.uuid,
    type: r.product_type,
    localName: r.name,
    perfumeName: r.perfume_name || r.name,
    brand: r.brand || '',
    category: r.category || '',
    gender: r.gender || '',
    imageUrl: r.image_url || '',
    description: r.description || '',
    perfumeId: r.perfume_id || null,
    stockQty: num(r.stock_qty),
    shopCount: r.shop_count,
    // Local price: the designated source shop's, else any shop's.
    localPriceDzd: r.source_price == null ? num(r.any_price) : num(r.source_price),
    // Online overlay.
    onlinePriceDzd: r.online_price_dzd == null ? null : num(r.online_price_dzd),
    prevPriceDzd: r.prev_price_dzd == null ? null : num(r.prev_price_dzd),
    onlineName: r.online_name || '',
    status: r.status || 'draft',
    priceOverridden: !!r.price_overridden,
    nameOverridden: !!r.name_overridden,
    shopifyProductId: r.shopify_product_id ? String(r.shopify_product_id) : null,
    shopifyVariantId: r.shopify_variant_id ? String(r.shopify_variant_id) : null,
    published: !!r.shopify_product_id,
  }));

  const q = String(filters.q || '').trim().toLowerCase();
  const filtered = rows.filter(r => {
    if (filters.type && r.type !== filters.type) return false;
    if (filters.gender && r.gender !== filters.gender) return false;
    if (filters.status && r.status !== filters.status) return false;
    if (filters.published === 'yes' && !r.published) return false;
    if (filters.published === 'no' && r.published) return false;
    if (q && !(`${r.localName} ${r.onlineName} ${r.perfumeName} ${r.brand}`.toLowerCase().includes(q))) return false;
    return true;
  });

  filtered.sort((a, b) => a.localName.localeCompare(b.localName));
  return { rows: filtered.slice(0, limit), total: filtered.length };
}

/**
 * Everything about one product for the detail view: the full perfume record
 * (accords/notes/seasons/weather/photos…), per-shop stock and price, and the
 * price matrix for its tier so staff can see volume pricing without it being
 * a separate row. One product = a handful of small reads, only on open.
 */
export async function getProductDetail(brandId, uuid) {
  const p = getPool();
  if (!p) return null;
  const bid = num(brandId);
  const u = String(uuid || '').trim();
  if (!bid || !u) return null;

  try {
    const [shopsRes, overlayRes] = await Promise.all([
      p.query(
        `SELECT pp.shop_id, s.name AS shop_name, s.is_catalog_source,
                pp.product_type, pp.name, pp.sell_price_dzd, pp.stock_qty,
                pp.volume_ml, pp.price_category_id, pp.perfume_id, pp.gender
           FROM pos_products pp JOIN pos_shops s ON s.id = pp.shop_id
          WHERE s.brand_id = $1 AND pp.uuid = $2
          ORDER BY s.is_catalog_source DESC, s.name`,
        [bid, u]),
      p.query(`SELECT * FROM pos_online_products WHERE brand_id=$1 AND sku=$2`, [bid, u]),
    ]);
    if (!shopsRes.rows.length) return null;

    const first = shopsRes.rows[0];
    let perfume = null;
    if (first.perfume_id) {
      const pfRes = await p.query(
        `SELECT * FROM pos_perfumes WHERE brand_id=$1 AND local_id=$2`, [bid, first.perfume_id]);
      const pf = pfRes.rows[0];
      if (pf) perfume = {
        localId: pf.local_id, name: pf.name, brand: pf.brand, category: pf.category,
        originalBottle: pf.original_bottle, gender: pf.gender, description: pf.description,
        rating: pf.rating, imageUrl: pf.image_url,
        accords: parseJson(pf.accords, []), notes: parseJson(pf.notes, {}),
        seasons: parseJson(pf.seasons, {}), weather: parseJson(pf.weather, {}),
        rememberMe: parseJson(pf.remember_me, []), photos: parseJson(pf.photos, []),
      };
    }

    // Matrix rows for this product's tier (extrait/shopMade), for the volume
    // pricing panel — derived, never stored as rows.
    let matrix = [];
    const tierId = first.price_category_id;
    if (tierId) {
      const mRes = await p.query(
        `SELECT volume_ml, sell_price FROM pos_price_matrix
          WHERE brand_id=$1 AND category_id=$2 ORDER BY volume_ml`, [bid, tierId]);
      matrix = mRes.rows.map(m => ({ volumeMl: num(m.volume_ml), price: num(m.sell_price) }));
    }

    return {
      uuid: u,
      type: first.product_type,
      localName: first.name,
      perfume,
      shops: shopsRes.rows.map(s => ({
        shopId: s.shop_id, shopName: s.shop_name, isSource: s.is_catalog_source,
        localPriceDzd: num(s.sell_price_dzd), stockQty: num(s.stock_qty),
        volumeMl: s.volume_ml == null ? null : num(s.volume_ml),
      })),
      overlay: overlayRes.rows[0] || null,
      matrix,
    };
  } catch (err) {
    console.error('[online-catalog] getProductDetail failed:', err.message);
    return null;
  }
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
