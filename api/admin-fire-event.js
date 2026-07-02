// ============================================================
//  Admin — manually fire ad conversion events
//  POST /api/admin/fire-event      (Bearer ADMIN_SECRET)
//
//  Lets the landing admin re-send (or send for the first time) a
//  Purchase conversion for an order in OUR orders DB to Meta /
//  TikTok / GA4 — e.g. after phone confirmation, or when the
//  original fire was gated off by attribution (organic hn_src).
//
//  Body: {
//    ref:        'H&N-ORD-XXXXXX',            // required — order in Postgres
//    platforms:  ['meta','tiktok','ga4'],     // required — at least one
//    eventId:    'optional-override',         // default: the order ref, so a
//                                             // recent duplicate de-dupes
//    value:      12345,                       // optional DZD override
//    testCode:   'TEST123'                    // optional Meta/TikTok test code
//  }
//
//  The identity payload is rebuilt from the stored order (hashed
//  phone / name / geo). Click-time ids (fbp/fbc/ttclid) are not
//  persisted on orders, so match quality relies on the hashed
//  identifiers — still solid for phone-verified COD customers.
// ============================================================

import { getOrderByRef } from './_orders-db.js';
import { trackPurchase } from './_tracking.js';

const VALID_PLATFORMS = new Set(['meta', 'tiktok', 'ga4']);

export default async function handler(req, res) {
  // Same fail-closed auth as /api/admin/sync-page-data.
  const secret = process.env.ADMIN_SECRET;
  if (!secret || req.headers.authorization !== `Bearer ${secret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const body = req.body || {};
  const ref = String(body.ref || '').trim().slice(0, 60);
  const platforms = (Array.isArray(body.platforms) ? body.platforms : [])
    .map(p => String(p).toLowerCase())
    .filter(p => VALID_PLATFORMS.has(p));

  if (!ref)              return res.status(400).json({ error: 'ref required' });
  if (!platforms.length) return res.status(400).json({ error: 'platforms required (meta|tiktok|ga4)' });

  const order = await getOrderByRef(ref);
  if (!order) return res.status(404).json({ error: `Order ${ref} not found` });

  const items = Array.isArray(order.items) ? order.items : [];
  const totalQty = items.reduce((n, it) => n + (Number(it.quantity) || 1), 0) || 1;
  const valueOverride = Number(body.value);
  const total = Number.isFinite(valueOverride) && valueOverride > 0
    ? valueOverride
    : Number(order.total_dzd) || 0;
  const merch = Number(order.merch_total_dzd) || total;
  const testCode = typeof body.testCode === 'string' ? body.testCode.slice(0, 60) : null;

  console.log(`[admin-fire-event] ref:${ref} platforms:[${platforms.join(',')}] value:${total} by admin`);

  const results = await trackPurchase({
    ref,
    total:      String(total),
    unitPrice:  String((merch / totalQty).toFixed(2)),
    variantId:  0,
    quantity:   totalQty,
    phone:      order.phone || '',
    name:       order.name  || '',
    city:       order.baladiya || '',
    state:      order.wilaya   || '',
    // Default eventId = ref: matches what create-order sends as fallback, so a
    // re-fire of a recently-tracked order de-dupes instead of double-counting.
    eventId:    String(body.eventId || ref).slice(0, 80),
    productTitle: items[0]?.title || order.origin || '',
    sourceUrl:  order.origin_url || '',
    skipGA4:    !platforms.includes('ga4'),
    skipMeta:   !platforms.includes('meta'),
    skipTikTok: !platforms.includes('tiktok'),
    metaTestCode:   platforms.includes('meta')   ? (testCode || null) : null,
    tiktokTestCode: platforms.includes('tiktok') ? (testCode || null) : null,
    // Admin-fired: no customer IP/UA available — hashed identifiers carry the match.
    ip: '', userAgent: 'HandsNose-Admin/1.0',
  });

  const ok = results.every(r => r.ok);
  return res.status(ok ? 200 : 502).json({ ok, ref, results });
}
