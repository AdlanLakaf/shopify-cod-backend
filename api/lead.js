// ============================================================
//  Lead beacon  —  POST /api/lead
//
//  Receives fire-and-forget `navigator.sendBeacon` pings from the
//  order form as the customer progresses through it (valid phone →
//  enrich → submit → fail / abandon). Each ping upserts the SAME
//  lead row (idempotent on leadId, see _leads-db.js) so there is
//  never a duplicate — a phone correction just updates in place
//  with an audit entry. Conversion is NOT sent from here; it is
//  stamped server-side by create-order.js when the order is made.
//
//  Like log-error.js this is best-effort and intentionally lax on
//  security (no HMAC, any origin, generous beacon rate bucket) —
//  it must never add friction to the order path, and it carries no
//  more PII than the order itself already does.
// ============================================================

import { runSecurityChecks } from './_security.js';
import { upsertLead } from './_leads-db.js';

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, {
    skipHmac: true, anyOrigin: true, rateBucket: 'beacon', rateMax: 1200,
  });
  if (blocked) return;

  if (req.method !== 'POST') return res.status(405).end();

  const body = req.body || {};

  // Derive context the client can't be trusted to send (and shouldn't have to).
  const referrer  = body.referrer  || req.headers['referer'] || req.headers['referrer'] || '';
  const userAgent = req.headers['user-agent'] || '';

  // Fire-and-forget: ack immediately, do the DB work without blocking the
  // beacon. A failure here is logged inside _leads-db and never surfaced.
  upsertLead({
    leadId:        body.leadId,
    stage:         body.stage,
    name:          body.name,
    phone:         body.phone,
    wilaya:        body.wilaya,
    baladiya:      body.baladiya,
    office:        body.office,
    deliveryType:  body.deliveryType,
    variantId:     body.variantId,
    variantTitle:  body.variantTitle,
    quantity:      body.quantity,
    merchTotalDzd: body.merchTotalDzd,
    shippingCost:  body.shippingCost,
    totalDzd:      body.totalDzd,
    source:        body.source || body.trafficSource,
    origin:        body.origin,
    originUrl:     body.originUrl || body.sourceUrl,
    entryUrl:      body.entryUrl,
    referrer,
    userAgent,
    failReason:    body.failReason || body.reason,
  }).catch(err => console.error('[lead] upsert error:', err?.message));

  return res.status(202).json({ ok: true });
}
