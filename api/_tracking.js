// ============================================================
//  Server-Side Conversion Tracking
//  Purchase  → GA4 (MP) + Meta CAPI + TikTok Events API
//  Funnel    → Meta CAPI + TikTok Events API (ViewContent,
//              InitiateCheckout, AddPaymentInfo)
//  Follows the full pixelData schema with hashed PII.
//
//  NOTE: mid-funnel events are NOT sent to GA4 server-side —
//  GA4 only de-dupes purchases (by transaction_id), so a
//  server copy of view_item/begin_checkout would double-count.
//  Those stay client-side. Meta & TikTok de-dupe every event
//  by event_id, so we send them both browser + server.
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

// ── Shared identity builders ───────────────────────────────────
// PII is optional — mid-funnel events may not have a phone/name yet.

function buildMetaUserData({ phone, name, city, state, fbp, fbc, ip, userAgent }) {
  const userData = {
    client_ip_address: ip        || '',
    client_user_agent: userAgent || '',
    country:           [sha256('dz')]
  };

  const e164 = toE164DZ(phone);
  if (e164) {
    userData.ph          = [sha256(e164)];
    userData.external_id = [sha256(e164)]; // stable customer key — lifts Event Match Quality
  }

  if (name) {
    const parts = name.trim().split(/\s+/);
    userData.fn = [sha256(parts[0])];
    if (parts.length > 1) userData.ln = [sha256(parts.slice(1).join(' '))];
  }

  if (city)  userData.ct  = [sha256(city)];
  if (state) userData.st  = [sha256(state)];
  if (fbp)   userData.fbp = fbp;
  if (fbc)   userData.fbc = fbc;

  return userData;
}

function buildTikTokUser({ phone, ttp, ttclid, ip, userAgent }) {
  const user = {
    ip:         ip        || '',
    user_agent: userAgent || ''
  };

  const e164 = toE164DZ(phone);
  if (e164)   user.phone  = sha256(e164);
  if (ttp)    user.ttp    = ttp;
  if (ttclid) user.ttclid = ttclid;

  return user;
}

// ── GA4 Measurement Protocol (purchase only) ───────────────────
async function trackGA4({ ref, total, unitPrice, variantId, quantity, productTitle, gaClientId, sessionId }) {
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
    // ── Required by MP so the purchase shows in Realtime/engagement reports ──
    engagement_time_msec: 100,
    items: [{
      item_id:   String(variantId),
      item_name: productTitle || '',
      quantity:  parseInt(quantity) || 1,
      price:     parseFloat(unitPrice) || 0
    }]
  };

  // ── session_id ties the server event to the user's GA4 session (attribution) ──
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

// ── Meta Conversions API — Purchase ─────────────────────────────
async function trackMeta({ ref, total, variantId, quantity, productTitle, contentCategory, phone, name, city, state, fbp, fbc, gclid, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.META_PIXEL_ID;
  const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:meta] META_PIXEL_ID or META_ACCESS_TOKEN not set — skipping');
    return;
  }

  const url = `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`;

  const userData = buildMetaUserData({ phone, name, city, state, fbp, fbc, ip, userAgent });

  const qty       = parseInt(quantity) || 1;
  const unitValue = (parseFloat(total) || 0) / qty;

  const customData = {
    value:        parseFloat(total) || 0,
    currency:     'DZD',
    order_id:     ref,
    content_ids:  [String(variantId)],
    content_type: 'product',
    num_items:    qty,
    // ── contents array — per-item detail improves catalog matching ──
    contents: [{
      id:          String(variantId),
      quantity:    qty,
      item_price:  Number(unitValue.toFixed(2))
    }]
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

// ── TikTok Events API — Purchase ────────────────────────────────
async function trackTikTok({ ref, total, variantId, quantity, productTitle, phone, ttp, ttclid, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.TIKTOK_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:tiktok] TIKTOK_PIXEL_ID or TIKTOK_ACCESS_TOKEN not set — skipping');
    return;
  }

  const user  = buildTikTokUser({ phone, ttp, ttclid, ip, userAgent });
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
        event:      'Purchase', // TikTok's current purchase event (PlaceAnOrder soft-deprecated, sunset 2027)
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

// ── Meta Conversions API — generic funnel event ─────────────────
async function trackMetaEvent({ eventName, value, variantId, quantity, productTitle, contentCategory, searchString, phone, name, city, state, fbp, fbc, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.META_PIXEL_ID;
  const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:meta] credentials not set — skipping', eventName);
    return;
  }

  const url = `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`;

  const userData = buildMetaUserData({ phone, name, city, state, fbp, fbc, ip, userAgent });

  const qty = parseInt(quantity) || 1;
  const val = parseFloat(value) || 0;

  const customData = {
    value:        val,
    currency:     'DZD',
    content_ids:  variantId ? [String(variantId)] : [],
    content_type: 'product',
    num_items:    qty
  };

  if (variantId) {
    customData.contents = [{
      id:         String(variantId),
      quantity:   qty,
      item_price: qty ? Number((val / qty).toFixed(2)) : val
    }];
  }
  if (productTitle)    customData.content_name     = productTitle;
  if (contentCategory) customData.content_category = contentCategory;
  if (eventName === 'Search' && searchString) customData.search_string = searchString;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      data: [{
        event_name:       eventName,
        event_time:       Math.floor(Date.now() / 1000),
        event_id:         eventId || undefined, // matches the browser pixel's eventID → de-dupe
        event_source_url: sourceUrl || '',
        action_source:    'website',
        user_data:        userData,
        custom_data:      customData
      }]
    })
  });

  if (!res.ok) throw new Error(`Meta CAPI ${res.status}: ${await res.text().catch(() => '')}`);
  const json = await res.json().catch(() => ({}));
  console.log(`[tracking:meta] ${eventName} fired — events_received:`, json.events_received);
}

// ── TikTok Events API — generic funnel event ────────────────────
async function trackTikTokEvent({ eventName, value, variantId, quantity, productTitle, searchString, phone, ttp, ttclid, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.TIKTOK_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:tiktok] credentials not set — skipping', eventName);
    return;
  }

  const user = buildTikTokUser({ phone, ttp, ttclid, ip, userAgent });
  const val  = parseFloat(value) || 0;

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
        event:      eventName,
        event_time: Math.floor(Date.now() / 1000),
        event_id:   eventId || undefined, // matches the browser pixel's event_id → de-dupe
        user,
        page: { url: sourceUrl || '' },
        properties: Object.assign({
          currency: 'DZD',
          value:    val,
          contents: variantId ? [{
            content_id:   String(variantId),
            content_type: 'product',
            content_name: productTitle || '',
            quantity:     parseInt(quantity) || 1,
            price:        (parseInt(quantity) || 1) ? Number((val / (parseInt(quantity) || 1)).toFixed(2)) : val
          }] : []
        }, (eventName === 'Search' && searchString) ? { query: searchString } : {})
      }]
    })
  });

  if (!res.ok) throw new Error(`TikTok API ${res.status}: ${await res.text().catch(() => '')}`);
  const json = await res.json().catch(() => ({}));
  console.log(`[tracking:tiktok] ${eventName} fired — code:`, json.code);
}

// ── Public exports ──────────────────────────────────────────────

// Purchase — fired from create-order.js (GA4 + Meta + TikTok)
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

// Events TikTok has no standard equivalent for — send to Meta only
const META_ONLY = new Set(['FindLocation']);

// Funnel event — fired from track-event.js (Meta + TikTok)
export async function trackEvent(data) {
  const tasks     = [trackMetaEvent(data)];
  const platforms = ['Meta CAPI'];
  if (!META_ONLY.has(data.eventName)) {
    tasks.push(trackTikTokEvent(data));
    platforms.push('TikTok');
  }

  const results = await Promise.allSettled(tasks);
  results.forEach((r, i) => {
    if (r.status === 'rejected') {
      console.error(`[tracking:${platforms[i].toLowerCase()}] ${data.eventName} failed:`, r.reason?.message || r.reason);
    }
  });
}
