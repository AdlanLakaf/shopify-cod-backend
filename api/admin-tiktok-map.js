// ============================================================
//  /api/admin/tiktok-map — TikTok lead settings, staff-managed
//  ADMIN_SECRET bearer (called by the landing admin proxy).
//
//  GET  → { mappings, pages, recent }
//  POST → one op per call:
//    { op:'upsertMapping', matchType, matchId?, variantId?, title?,
//      priceDzd?, quantity?, active?, note? }
//      · variantId given w/o title/price → auto-filled from Shopify
//    { op:'deleteMapping', matchType, matchId }
//    { op:'upsertPage', pageId, label?, active? }
//    { op:'deletePage', pageId }
//
//  matchType: 'ad' | 'adgroup' | 'form' | 'campaign' | 'default'
//  (default needs no matchId). Most specific active rule wins.
// ============================================================

import { upsertMapping, deleteMapping, listMappingsAndRecent, upsertPage, deletePage } from './_tiktok-db.js';
import { getVariantInfo } from './_shopify-catalog.js';

export default async function handler(req, res) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret || req.headers.authorization !== `Bearer ${secret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (req.method === 'GET') {
    return res.json(await listMappingsAndRecent(50));
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  const b = req.body || {};
  const op = String(b.op || (b.delete === true ? 'deleteMapping' : 'upsertMapping'));

  switch (op) {
    case 'upsertMapping': {
      if (!b.matchType || (b.matchType !== 'default' && !b.matchId)) {
        return res.status(400).json({ error: 'matchType (+ matchId unless default) required' });
      }
      let { variantId = null, title = '', priceDzd = 0 } = b;
      // Staff picked a variant → pull authoritative title/price from Shopify so
      // the CRM order line and the Shopify line always agree.
      if (variantId && (!title || !(Number(priceDzd) > 0))) {
        const info = await getVariantInfo(variantId);
        if (!info) return res.status(400).json({ error: `Variant ${variantId} not found in Shopify` });
        title    = title || info.title || 'Shopify variant';
        priceDzd = Number(priceDzd) > 0 ? priceDzd : info.priceDzd;
      }
      if (!variantId && !(title && Number(priceDzd) > 0)) {
        return res.status(400).json({ error: 'need variantId, or title + priceDzd' });
      }
      const ok = await upsertMapping({ ...b, variantId, title, priceDzd });
      return res.status(ok ? 200 : 500).json({ ok, applied: { variantId, title, priceDzd } });
    }
    case 'deleteMapping': {
      const ok = await deleteMapping(b.matchType, b.matchType === 'default' ? '*' : b.matchId, b.answerMatch || '');
      return res.status(ok ? 200 : 404).json({ ok });
    }
    case 'upsertPage': {
      if (!b.pageId) return res.status(400).json({ error: 'pageId required' });
      const ok = await upsertPage(b);
      return res.status(ok ? 200 : 500).json({ ok });
    }
    case 'deletePage': {
      const ok = await deletePage(b.pageId);
      return res.status(ok ? 200 : 404).json({ ok });
    }
    default:
      return res.status(400).json({ error: `Unknown op "${op}"` });
  }
}
