// ============================================================
//  Server-Side Conversion Tracking
//  Fires Purchase to GA4, Meta CAPI, TikTok Events API
//  Follows the full pixelData schema with hashed PII
// ============================================================

import crypto from 'crypto';

// ── Utilities ─────────────────────────────────────────────────

const sha256 = str =>
  crypto.createHash('sha256').update(String(str || '').toLowerCase().trim()).digest('hex');

// Converts Algerian phone to E164 digits (no +, no spaces)
// 0770123456 → 213770123456
function toE164DZ(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.startsWith('213') && digits.length >= 12) return digits;
  if (digits.startsWith('0') && digits.length === 10)  return '213' + digits.slice(1);
  if (digits.length === 9)                              return '213' + digits;
  return digits;
}

// ── GA4 Measurement Protocol ───────────────────────────────────
async function trackGA4({ ref, total, variantId, quantity, productTitle, gaClientId, sessionId }) {
  const MEASUREMENT_ID = process.env.GA4_MEASUREMENT_ID;
  const API_SECRET     = process.env.GA4_API_SECRET;

  if (!MEASUREMENT_ID || !API_SECRET) {
    console.warn('[tracking:ga4] credentials not set — skipping');
    return;
  }

  const url = `https://www.google-analytics.com/mp/collect?measurement_id=${MEASUREMENT_ID}&api_secret=${API_SECRET}`;

  const params = {
    transaction_id: ref,
    value:          parseFloat(total) || 0,
    currency:       'DZD',
    items: [{
      item_id:   String(variantId),
      item_name: productTitle || '',
      quantity:  parseInt(quantity) || 1,
      price:     parseFloat(total) || 0
    }]
  };

  if (sessionId) params.session_id = sessionId;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: gaClientId || ('server.' + Date.now()),
      events: [{ name: 'purchase', params }]
    })
  });

  if (!res.ok) throw new Error(`GA4 MP ${res.status}: ${await res.text().catch(() => '')}`);
  console.log('[tracking:ga4] purchase fired — ref:', ref);
}

// ── Meta Conversions API ────────────────────────────────────────
async function trackMeta({ ref, total, variantId, quantity, productTitle, contentCategory, phone, name, city, state, fbp, fbc, gclid, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.META_PIXEL_ID;
  const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:meta] META_PIXEL_ID or META_ACCESS_TOKEN not set — skipping');
    return;
  }

  const url = `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`;

  // ── Hash all PII ──
  const userData = {
    client_ip_address: ip        || '',
    client_user_agent: userAgent || '',
    country:           [sha256('dz')]
  };

  const e164 = toE164DZ(phone);
  if (e164)  userData.ph = [sha256(e164)];

  if (name) {
    const parts = name.trim().split(/\s+/);
    userData.fn = [sha256(parts[0])];
    if (parts.length > 1) userData.ln = [sha256(parts.slice(1).join(' '))];
  }

  if (city)  userData.ct = [sha256(city)];
  if (state) userData.st = [sha256(state)];
  if (fbp)   userData.fbp = fbp;
  if (fbc)   userData.fbc = fbc;

  const customData = {
    value:        parseFloat(total) || 0,
    currency:     'DZD',
    order_id:     ref,
    content_ids:  [String(variantId)],
    content_type: 'product',
    num_items:    parseInt(quantity) || 1
  };

  if (productTitle)    customData.content_name     = productTitle;
  if (contentCategory) customData.content_category = contentCategory;

  const eventPayload = {
    event_name:       'Purchase',
    event_time:       Math.floor(Date.now() / 1000),
    event_id:         eventId || ref,
    event_source_url: sourceUrl || '',
    action_source:    'website',
    user_data:        userData,
    custom_data:      customData
  };

  // Enhanced Conversions — gclid for Google Ads cross-attribution
  if (gclid) eventPayload.referrer_url = sourceUrl || '';

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ data: [eventPayload] })
  });

  if (!res.ok) throw new Error(`Meta CAPI ${res.status}: ${await res.text().catch(() => '')}`);
  const json = await res.json().catch(() => ({}));
  console.log('[tracking:meta] purchase fired — events_received:', json.events_received, '— ref:', ref);
}

// ── TikTok Events API ───────────────────────────────────────────
async function trackTikTok({ ref, total, variantId, quantity, productTitle, phone, ttp, ttclid, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.TIKTOK_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:tiktok] TIKTOK_PIXEL_ID or TIKTOK_ACCESS_TOKEN not set — skipping');
    return;
  }

  const e164 = toE164DZ(phone);

  const user = {
    ip:         ip        || '',
    user_agent: userAgent || '',
  };

  if (e164)   user.phone  = sha256(e164);
  if (ttp)    user.ttp    = ttp;
  if (ttclid) user.ttclid = ttclid;

  const price = parseFloat(total) || 0;

  const res = await fetch('https://business-api.tiktok.com/open_api/v1.3/event/track/', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Access-Token': ACCESS_TOKEN
    },
    body: JSON.stringify({
      pixel_code:      PIXEL_ID,
      event_source:    'web',
      event_source_id: PIXEL_ID,
      data: [{
        event:      'PlaceAnOrder',
        event_time: Math.floor(Date.now() / 1000),
        event_id:   eventId || ref,
        user,
        page: { url: sourceUrl || '' },
        properties: {
          currency: 'DZD',
          value:    price,
          order_id: ref,
          contents: [{
            content_id:   String(variantId),
            content_type: 'product',
            content_name: productTitle || '',
            quantity:     parseInt(quantity) || 1,
            price
          }]
        }
      }]
    })
  });

  if (!res.ok) throw new Error(`TikTok API ${res.status}: ${await res.text().catch(() => '')}`);
  const json = await res.json().catch(() => ({}));
  console.log('[tracking:tiktok] purchase fired — code:', json.code, '— ref:', ref);
}

// ── Main export ─────────────────────────────────────────────────
export async function trackPurchase(data) {
  const results = await Promise.allSettled([
    trackGA4(data),
    trackMeta(data),
    trackTikTok(data),
  ]);

  results.forEach((r, i) => {
    const platform = ['GA4', 'Meta CAPI', 'TikTok'][i];
    if (r.status === 'rejected') {
      console.error(`[tracking:${platform.toLowerCase()}] failed:`, r.reason?.message || r.reason);
    }
  });
}
