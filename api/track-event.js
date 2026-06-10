// ============================================================
//  Server-side funnel event relay
//  POST /api/track-event
//  Mirrors mid-funnel browser pixels (ViewContent,
//  InitiateCheckout, AddPaymentInfo) to Meta CAPI + TikTok
//  Events API for ad-block / iOS resilience.
//
//  De-dupe: the browser fires the same event with the same
//  eventID — Meta & TikTok collapse the pair by event_id.
//  GA4 is intentionally NOT called here (no mid-funnel dedup).
// ============================================================

import { runSecurityChecks, log } from './_security.js';
import { trackEvent } from './_tracking.js';
import { getTestMode } from './_test-mode.js';
import { resolveAdPlatform } from './_attribution.js';

// Only these events may be relayed
const ALLOWED_EVENTS = new Set([
  'ViewContent', 'InitiateCheckout', 'AddPaymentInfo', 'Search', 'FindLocation', 'CustomizeProduct'
]);

// Conversion / mid-funnel events that are gated by the hn_src attribution cookie.
// Top-of-funnel events (ViewContent, Search) are NOT gated — they always fire to both
// platforms because they're audience-building signals, not conversion claims.
const GATED_EVENTS = new Set([
  'InitiateCheckout', 'AddPaymentInfo', 'FindLocation', 'CustomizeProduct'
]);

export default async function handler(req, res) {
  // anyOrigin: the Custom Pixel fires from a sandboxed iframe on a
  // different origin — this is a cookie-less beacon, so '*' is safe.
  const blocked = runSecurityChecks(req, res, { skipHmac: true, anyOrigin: true, rateBucket: 'beacon', rateMax: 600 });
  if (blocked) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const {
    event,
    eventId         = '',
    variantId       = '',
    quantity        = 1,
    value           = 0,
    productTitle    = '',
    contentCategory = '',
    brand           = '',
    description     = '',
    searchString    = '',
    phone           = '',
    name            = '',
    city            = '',
    state           = '',
    fbp             = '',
    fbc             = '',
    ttp             = '',
    ttclid          = '',
    externalId      = '',
    sourceUrl       = '',
    trafficSource   = '',
    metaTestCode    = '',
    tiktokTestCode  = ''
  } = req.body || {};

  // ── Inbound log so every invocation is identifiable in Vercel logs ──
  log('[track-event] in:', event, '| id:', eventId, '| variant:', variantId, '| value:', value);

  if (!ALLOWED_EVENTS.has(event)) {
    console.warn('[track-event] REJECTED — unsupported event:', JSON.stringify(event));
    return res.status(400).json({ error: 'Unsupported event' });
  }

  // ── Apply test mode overrides ──
  const testMode   = getTestMode(req.body || {});
  const adPlatform = resolveAdPlatform(trafficSource);
  const shouldGate = GATED_EVENTS.has(event);

  // Attribution logic:
  //  - Test mode: honour explicit skip/test flags, ignore attribution.
  //  - Gated events (conversion signals): only fire to the attributed platform.
  //  - Ungated events (ViewContent, Search): always fire to both — they are
  //    audience-building signals and must mirror the browser pixel for de-dupe / EMQ.
  let skipMeta, skipTikTok;
  if (testMode) {
    skipMeta   = testMode.metaMode   === 'skip';
    skipTikTok = testMode.tiktokMode === 'skip';
  } else if (shouldGate) {
    skipMeta   = adPlatform !== 'meta';
    skipTikTok = adPlatform !== 'tiktok';
  } else {
    skipMeta   = false;
    skipTikTok = false;
  }

  // ── Awaited so Vercel doesn't cut the request off before the beacons send ──
  await trackEvent({
    eventName:       event,
    eventId:         eventId || undefined,
    value,
    variantId,
    quantity,
    productTitle,
    contentCategory,
    brand,
    description,
    searchString,
    phone,
    name,
    city,
    state,
    fbp,
    fbc,
    ttp,
    ttclid,
    externalId,
    sourceUrl,
    skipMeta,
    skipTikTok,
    metaTestCode:   testMode?.metaMode   === 'test' ? (metaTestCode   || null) : null,
    tiktokTestCode: testMode?.tiktokMode === 'test' ? (tiktokTestCode || null) : null,
    ip:        req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || '',
    userAgent: req.headers['user-agent'] || ''
  }).catch(err => console.error('[track-event] error:', err.message));

  return res.status(200).json({ ok: true });
}
