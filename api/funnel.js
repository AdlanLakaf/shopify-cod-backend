// ============================================================
//  Funnel beacon  —  POST /api/funnel
//
//  Fire-and-forget engagement pings from the landing page (and any
//  storefront) recording the on-page micro-funnel: page_view →
//  product_view → price_view → variant_select → form_start →
//  info_filled → submit_click → converted, plus side engagements
//  (whatsapp_click, story_view, vocals_*, feedback_*).
//
//  Anonymous: keyed on a session id minted at page load, no phone
//  required. Same lax posture as log-error / lead (no HMAC, any
//  origin, generous beacon bucket). Never on the order path.
// ============================================================

import { runSecurityChecks } from './_security.js';
import { trackSession } from './_funnel-db.js';

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, {
    skipHmac: true, anyOrigin: true, rateBucket: 'beacon', rateMax: 2000,
  });
  if (blocked) return;

  if (req.method !== 'POST') return res.status(405).end();

  const body = req.body || {};

  // Arrival log — confirms the client funnel beacon reached us (Railway logs).
  console.log('[funnel] beacon', JSON.stringify({
    step: body.step || (Array.isArray(body.steps) ? body.steps.join(',') : ''),
    sid: (body.sessionId || '').slice(0, 24), origin: body.origin || '',
  }));

  const referrer = body.referrer || req.headers['referer'] || req.headers['referrer'] || '';

  // Accept a single step or a small batch (the client may coalesce a few).
  const steps = Array.isArray(body.steps) ? body.steps.slice(0, 20) : [body.step];
  const common = {
    sessionId: body.sessionId,
    source:    body.source || body.trafficSource,
    origin:    body.origin,
    url:       body.url || body.sourceUrl,
    entryUrl:  body.entryUrl,
    referrer,
    device:    body.device,
    leadId:    body.leadId,
    phone:     body.phone,
  };

  for (const step of steps) {
    trackSession({ ...common, step }).catch(err => console.error('[funnel] track error:', err?.message));
  }

  return res.status(202).json({ ok: true });
}
