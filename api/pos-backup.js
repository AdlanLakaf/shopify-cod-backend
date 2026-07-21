// ============================================================
//  /api/pos/backup — receive a shop's daily DB dump, chunked.
//
//  Auth: the shop's sync token (any active shop — each backs up
//  its OWN local database; not restricted to the catalog source).
//
//  Body (one chunk): { day:'YYYY-MM-DD', part:N, dataB64, done? }
//    part 0 starts the file; send in order; done finalizes + runs
//    retention. A small dump can be one call: { part:0, done:true }.
//
//  Reply: { ok, bytes, evicted?, totalBytes? } or { ok:false }
// ============================================================

import { authShopByToken } from './_pos-db.js';
import { appendBackupChunk } from './_backup-store.js';

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  const shop = await authShopByToken(token);
  if (!shop) return res.status(401).json({ error: 'Unknown sync token' });
  if (!shop.active) return res.status(403).json({ error: `Shop "${shop.name}" is deactivated` });

  const b = req.body || {};
  const result = appendBackupChunk({
    shopId: shop.id, day: b.day, part: b.part, dataB64: b.dataB64, done: b.done,
  });
  return res.status(result.ok ? 200 : 400).json(result);
}
