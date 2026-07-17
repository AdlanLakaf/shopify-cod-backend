// ============================================================
//  /api/admin/tiktok-map — manage TikTok lead → product mapping
//  ADMIN_SECRET bearer (same pattern as sync-page-data).
//
//  GET  → { mappings, recent }  (rules + last 50 ledger rows)
//  POST → upsert rule  { matchType, matchId, variantId?, title?,
//                        priceDzd, quantity?, active?, note? }
//  POST → delete rule  { delete: true, matchType, matchId }
//
//  matchType: 'ad' | 'adgroup' | 'form' | 'campaign' | 'default'
//  (default needs no matchId). Most specific active rule wins.
// ============================================================

import { upsertMapping, deleteMapping, listMappingsAndRecent } from './_tiktok-db.js';

export default async function handler(req, res) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret || req.headers.authorization !== `Bearer ${secret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (req.method === 'GET') {
    return res.json(await listMappingsAndRecent(50));
  }

  if (req.method === 'POST') {
    const b = req.body || {};
    if (b.delete === true) {
      const ok = await deleteMapping(b.matchType, b.matchType === 'default' ? '*' : b.matchId);
      return res.status(ok ? 200 : 404).json({ ok });
    }
    if (!b.matchType || (b.matchType !== 'default' && !b.matchId)) {
      return res.status(400).json({ error: 'matchType (+ matchId unless default) required' });
    }
    if (!b.variantId && !(b.title && Number(b.priceDzd) > 0)) {
      return res.status(400).json({ error: 'need variantId, or title + priceDzd' });
    }
    const ok = await upsertMapping(b);
    return res.status(ok ? 200 : 500).json({ ok });
  }

  return res.status(405).json({ error: 'Method not allowed' });
}
