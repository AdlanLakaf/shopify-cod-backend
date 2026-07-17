// ============================================================
//  GET /api/admin/shopify-variants?q=<search> — variant picker
//  ADMIN_SECRET bearer. Flattened active-product variants,
//  filtered by q against product/variant title (10-min cache
//  behind listVariants, so browsing is free).
// ============================================================

import { listVariants } from './_shopify-catalog.js';

export default async function handler(req, res) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret || req.headers.authorization !== `Bearer ${secret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const q = String(req.query?.q || '').trim().toLowerCase();
  let variants = await listVariants();
  if (q) {
    variants = variants.filter(v =>
      v.productTitle.toLowerCase().includes(q) ||
      (v.variantTitle || '').toLowerCase().includes(q) ||
      String(v.variantId).includes(q));
  }
  res.json({ variants: variants.slice(0, 100) });
}
