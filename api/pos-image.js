// ============================================================
//  /api/pos/image — upload ONE perfume photo, off the JSON tick.
//
//  Image bytes are large; carrying 20 base64 photos inside the
//  tick blew its body limit (413). Images now drip one per request
//  on their own channel with a generous limit, while the tick stays
//  small and fast for orders / stock / analytics.
//
//  Auth: the shop's sync token, same as the tick. Only the brand's
//  catalog-source shop may push images.
//
//  Body: { file, dataB64, mime? }   (one image)
//  Reply: { ok, file, url } or { ok:false, error }
// ============================================================

import { authShopByToken, applyOneImage } from './_pos-db.js';

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  const shop = await authShopByToken(token);
  if (!shop) return res.status(401).json({ error: 'Unknown sync token' });
  if (!shop.active) return res.status(403).json({ error: `Shop "${shop.name}" is deactivated` });

  const result = await applyOneImage(shop, req.body || {});
  return res.status(result.ok ? 200 : 400).json(result);
}
