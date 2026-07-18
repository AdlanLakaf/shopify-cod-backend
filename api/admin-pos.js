// ============================================================
//  /api/admin/pos — POS multi-shop administration, staff-managed
//  ADMIN_SECRET bearer (called by the landing admin proxy).
//
//  GET  ?view=overview                      → { brands, shops }
//  GET  ?view=stock&shopId=&type=&q=        → rows
//  GET  ?view=mappings&shopId=              → rows
//  GET  ?view=analytics&scope=all|shop:1|brand:2&days=30
//
//  POST one op per call:
//    { op:'upsertBrand', id?, name, ownerName?, ourSharePct?, logoUrl? }
//    { op:'upsertShop',  id?, brandId, name, logoUrl?, syncIntervalSec?,
//                        routingPriority?, active? }
//        · creating (no id) returns { shop } INCLUDING sync_token —
//          shown once, paste it into the local ERP's cloud-sync page.
//    { op:'rotateShopToken', shopId }        → { shop } with new token
//    { op:'upsertMapping', shopifyVariantId, shopId, productUuid, qtyPerUnit? }
//    { op:'deleteMapping', shopifyVariantId, shopId }
//    { op:'reassignOrder', ref, shopId }
// ============================================================

import {
  listBrandsAndShops, upsertBrand, upsertShop, rotateShopToken,
  upsertVariantMap, deleteVariantMap, listVariantMaps,
  listStock, getAnalytics, reassignOrder, listUnrouted, bulkRoutePending,
} from './_pos-db.js';

export default async function handler(req, res) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret || req.headers.authorization !== `Bearer ${secret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    if (req.method === 'GET') {
      const q = req.query || {};
      switch (String(q.view || 'overview')) {
        case 'overview':
          return res.json(await listBrandsAndShops());
        case 'stock':
          return res.json({
            rows: await listStock({
              shopId: q.shopId || null, type: q.type || '', q: q.q || '',
              limit: q.limit || 300,
            }),
          });
        case 'mappings':
          return res.json({ rows: await listVariantMaps({ shopId: q.shopId || null }) });
        case 'unrouted':
          return res.json(await listUnrouted());
        case 'analytics':
          return res.json(await getAnalytics({ scope: q.scope || 'all', days: q.days || 30 }));
        default:
          return res.status(400).json({ error: `Unknown view "${q.view}"` });
      }
    }

    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
    const b = req.body || {};

    switch (String(b.op || '')) {
      case 'upsertBrand': {
        if (!b.name) return res.status(400).json({ error: 'name required' });
        const brand = await upsertBrand(b);
        return res.status(brand ? 200 : 500).json({ ok: !!brand, brand });
      }
      case 'upsertShop': {
        if (!b.name || (!b.id && !b.brandId)) {
          return res.status(400).json({ error: 'name + brandId required' });
        }
        const shop = await upsertShop(b);
        if (!shop) return res.status(500).json({ ok: false });
        // Only expose the token on CREATE (staff must copy it into the ERP).
        if (b.id) delete shop.sync_token;
        return res.json({ ok: true, shop });
      }
      case 'rotateShopToken': {
        if (!b.shopId) return res.status(400).json({ error: 'shopId required' });
        const shop = await rotateShopToken(b.shopId);
        return res.status(shop ? 200 : 404).json({ ok: !!shop, shop });
      }
      case 'upsertMapping': {
        if (!b.shopifyVariantId || !b.shopId || !b.productUuid) {
          return res.status(400).json({ error: 'shopifyVariantId + shopId + productUuid required' });
        }
        const ok = await upsertVariantMap(b);
        return res.status(ok ? 200 : 500).json({ ok });
      }
      case 'deleteMapping': {
        if (!b.shopifyVariantId || !b.shopId) {
          return res.status(400).json({ error: 'shopifyVariantId + shopId required' });
        }
        const ok = await deleteVariantMap(b.shopifyVariantId, b.shopId);
        return res.status(ok ? 200 : 404).json({ ok });
      }
      case 'bulkRoute': {
        const action = String(b.action || '');
        if (!['assign', 'ignore', 'cancel'].includes(action)) {
          return res.status(400).json({ error: "action must be 'assign' | 'ignore' | 'cancel'" });
        }
        if (action === 'assign' && !b.shopId) return res.status(400).json({ error: 'shopId required for assign' });
        const n = await bulkRoutePending(action, b.shopId || null);
        return res.json({ ok: true, updated: n });
      }
      case 'reassignOrder': {
        if (!b.ref || !b.shopId) return res.status(400).json({ error: 'ref + shopId required' });
        const ok = await reassignOrder(b.ref, b.shopId);
        return res.status(ok ? 200 : 404).json({ ok });
      }
      default:
        return res.status(400).json({ error: `Unknown op "${b.op}"` });
    }
  } catch (err) {
    console.error('[admin-pos] failed:', err.message);
    return res.status(500).json({ error: 'Server error' });
  }
}
