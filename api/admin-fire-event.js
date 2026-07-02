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
import { getLeadById }   from './_leads-db.js';
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
  const leadId = String(body.leadId || '').trim().slice(0, 80);
  const platforms = (Array.isArray(body.platforms) ? body.platforms : [])
    .map(p => String(p).toLowerCase())
    .filter(p => VALID_PLATFORMS.has(p));

  if (!ref && !leadId)   return res.status(400).json({ error: 'ref or leadId required' });
  if (!platforms.length) return res.status(400).json({ error: 'platforms required (meta|tiktok|ga4)' });

  // ── Resolve the sale: an order row (by ref, or via the lead's order_ref),
  //    else the lead row itself (staff-recovered sale with no order record).
  let sale = null;      // normalised: { key, total, merch, qty, phone, name, city, state, variantId, title, sourceUrl }
  const lead = !ref && leadId ? await getLeadById(leadId) : null;
  const orderRef = ref || (lead?.order_ref ? String(lead.order_ref) : '');
  const order = orderRef ? await getOrderByRef(orderRef) : null;

  if (order) {
    const items = Array.isArray(order.items) ? order.items : [];
    const total = Number(order.total_dzd) || 0;
    sale = {
      key:       order.ref,
      total,
      merch:     Number(order.merch_total_dzd) || total,
      qty:       items.reduce((n, it) => n + (Number(it.quantity) || 1), 0) || 1,
      phone:     order.phone || '',
      name:      order.name || '',
      city:      order.baladiya || '',
      state:     order.wilaya || '',
      variantId: 0,
      title:     items[0]?.title || order.origin || '',
      sourceUrl: order.origin_url || '',
    };
  } else if (lead) {
    const merch = Number(lead.merch_total_dzd) || 0;
    const total = Number(lead.total_dzd) || (merch + (Number(lead.shipping_cost) || 0));
    if (total <= 0) return res.status(400).json({ error: `Lead ${leadId} has no total value — cannot fire a purchase` });
    sale = {
      key:       `lead-${leadId}`,
      total,
      merch:     merch || total,
      qty:       Math.max(1, Number(lead.quantity) || 1),
      phone:     lead.phone || '',
      name:      lead.name || '',
      city:      lead.baladiya || '',
      state:     lead.wilaya || '',
      variantId: Number(lead.variant_id) || 0,
      title:     lead.variant_title || lead.origin || '',
      sourceUrl: lead.origin_url || '',
    };
  } else {
    return res.status(404).json({ error: ref ? `Order ${ref} not found` : `Lead ${leadId} not found` });
  }

  const valueOverride = Number(body.value);
  const total = Number.isFinite(valueOverride) && valueOverride > 0 ? valueOverride : sale.total;
  const testCode = typeof body.testCode === 'string' ? body.testCode.slice(0, 60) : null;

  console.log(`[admin-fire-event] ${sale.key} platforms:[${platforms.join(',')}] value:${total} by admin`);

  const results = await trackPurchase({
    ref:        sale.key,
    total:      String(total),
    unitPrice:  String((sale.merch / sale.qty).toFixed(2)),
    variantId:  sale.variantId,
    quantity:   sale.qty,
    phone:      sale.phone,
    name:       sale.name,
    city:       sale.city,
    state:      sale.state,
    // Default eventId = the sale key (order ref / lead id): matches what
    // create-order sends as fallback, so a re-fire of a recently-tracked
    // order de-dupes instead of double-counting.
    eventId:    String(body.eventId || sale.key).slice(0, 80),
    productTitle: sale.title,
    sourceUrl:  sale.sourceUrl,
    skipGA4:    !platforms.includes('ga4'),
    skipMeta:   !platforms.includes('meta'),
    skipTikTok: !platforms.includes('tiktok'),
    metaTestCode:   platforms.includes('meta')   ? (testCode || null) : null,
    tiktokTestCode: platforms.includes('tiktok') ? (testCode || null) : null,
    // Admin-fired: no customer IP/UA available — hashed identifiers carry the match.
    ip: '', userAgent: 'HandsNose-Admin/1.0',
  });

  const ok = results.every(r => r.ok);
  return res.status(ok ? 200 : 502).json({ ok, ref: sale.key, results });
}
