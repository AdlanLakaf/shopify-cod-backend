// ============================================================
//  /api/pos/tick — the ONE sync call a local ERP host makes per
//  interval. Bearer = the shop's sync_token (minted by the admin
//  when the shop is registered; each shop has its own).
//
//  Request body (all sections optional):
//    stock:        [{ uuid, type, name, brand, category, sellPriceDzd, qty,
//                     volumeMl, gender, categoryId, perfumeId }]
//    catalog:      { perfumes[], priceCategories[], matrixCells[] }
//                  — brand-level static data; only honoured from the shop
//                    flagged is_catalog_source for that brand. Photo BYTES do
//                    NOT ride here — they go to /api/pos/image (too large).
//    prunedUuids:  [uuid]                    — products deleted locally
//    sales:        [{ id, status, channel, paymentMethod, totalDzd,
//                     discountDzd, soldAt, items:[…] }]
//    orderUpdates: [{ ref, status }]         — 'received' | 'confirmed' | 'cancelled'
//
//  Response:
//    { ok, intervalSec,          — shop's cadence, set from the online admin
//      orders: [...],            — open web orders assigned to this shop
//      maxSaleId,                — resume watermark for the sales push
//      counts: {...} }
//
//  Cost design: everything both directions rides this single
//  request; the ERP never opens a second connection, the cloud
//  never calls the shop. Batches are capped (500 stock / 200
//  sales / 100 acks per tick) — the ERP just carries leftovers to
//  the next tick.
// ============================================================

import {
  authShopByToken, applyStockBatch, pruneStock, applySalesBatch,
  applyOrderUpdates, getOpenOrdersForShop, assignPendingOrders,
  maxSaleId, touchShopSync, applyCatalogBatch,
} from './_pos-db.js';

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  const shop = await authShopByToken(token);
  if (!shop) return res.status(401).json({ error: 'Unknown sync token — re-copy it from /admin/pos (rotate if lost)' });
  if (!shop.active) {
    return res.status(403).json({ error: `Shop "${shop.name}" is deactivated in the admin — tick the Active box in /admin/pos to resume sync` });
  }

  const b = req.body || {};
  const counts = {};
  try {
    // Push-up sections. Order matters: stock first so freshly created
    // products exist before sales/orders reference them.
    // Static catalog first — perfumes/tiers/matrix are what stock rows
    // reference. Ignored unless this shop is its brand's catalog source.
    counts.catalog = await applyCatalogBatch(shop, b.catalog);
    counts.stock  = await applyStockBatch(shop.id, b.stock);
    counts.pruned = await pruneStock(shop.id, b.prunedUuids);
    counts.sales  = await applySalesBatch(shop.id, b.sales);
    counts.acks   = await applyOrderUpdates(shop.id, b.orderUpdates);

    // Catch any web order the creation-time hook failed to assign, so it
    // reaches a POS at the latest on the next tick of any shop.
    counts.assigned = await assignPendingOrders(20);

    const [orders, saleWatermark] = await Promise.all([
      getOpenOrdersForShop(shop.id),
      maxSaleId(shop.id),
    ]);
    await touchShopSync(shop.id);

    return res.json({
      ok: true,
      intervalSec: shop.sync_interval_sec || 300,
      orders,
      maxSaleId: saleWatermark,
      counts,
    });
  } catch (err) {
    console.error('[pos-sync] tick failed:', err.message, `(shop ${shop.id})`);
    return res.status(500).json({ error: 'Sync failed' });
  }
}
