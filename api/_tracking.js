// ============================================================
//  Server-Side Conversion Tracking
//  Fires Purchase to GA4, Meta CAPI, and TikTok Events API
//  Called non-blocking from create-order.js after order completes
// ============================================================

import crypto from 'crypto';

const sha256 = str =>
  crypto.createHash('sha256').update(String(str || '').toLowerCase().trim()).digest('hex');

// ── GA4 Measurement Protocol ─────────────────────────────────
async function trackGA4({ ref, total, variantId, quantity, gaClientId }) {
  const MEASUREMENT_ID = process.env.GA4_MEASUREMENT_ID;
  const API_SECRET     = process.env.GA4_API_SECRET;

  if (!MEASUREMENT_ID || !API_SECRET) {
    console.warn('[tracking:ga4] GA4_MEASUREMENT_ID or GA4_API_SECRET not set — skipping');
    return;
  }

  const url = `https://www.google-analytics.com/mp/collect?measurement_id=${MEASUREMENT_ID}&api_secret=${API_SECRET}`;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: gaClientId || ('server.' + Date.now()),
      events: [{
        name: 'purchase',
        params: {
          transaction_id: ref,
          value:          parseFloat(total) || 0,
          currency:       'DZD',
          items: [{
            item_id:  String(variantId),
            quantity: parseInt(quantity) || 1
          }]
        }
      }]
    })
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`GA4 MP ${res.status}: ${body}`);
  }

  console.log('[tracking:ga4] purchase fired — ref:', ref);
}

// ── Meta Conversions API ──────────────────────────────────────
async function trackMeta({ ref, total, variantId, quantity, phone, name, fbp, fbc, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.FB_PIXEL_ID;
  const ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:meta] FB_PIXEL_ID or FB_ACCESS_TOKEN not set — skipping');
    return;
  }

  const url = `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`;

  // ── User data — hash all PII ──
  const userData = {
    client_ip_address: ip        || '',
    client_user_agent: userAgent || '',
  };

  if (phone) userData.ph = [sha256(phone.replace(/\D/g, ''))];

  if (name) {
    const parts = name.trim().split(/\s+/);
    userData.fn = [sha256(parts[0])];
    if (parts.length > 1) userData.ln = [sha256(parts.slice(1).join(' '))];
  }

  if (fbp) userData.fbp = fbp;
  if (fbc) userData.fbc = fbc;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      data: [{
        event_name:       'Purchase',
        event_time:       Math.floor(Date.now() / 1000),
        event_id:         eventId || ref,
        event_source_url: sourceUrl || '',
        action_source:    'website',
        user_data:        userData,
        custom_data: {
          value:        parseFloat(total) || 0,
          currency:     'DZD',
          order_id:     ref,
          content_ids:  [String(variantId)],
          content_type: 'product',
          num_items:    parseInt(quantity) || 1
        }
      }]
    })
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Meta CAPI ${res.status}: ${body}`);
  }

  const json = await res.json().catch(() => ({}));
  console.log('[tracking:meta] purchase fired — events_received:', json.events_received, '— ref:', ref);
}

// ── TikTok Events API ─────────────────────────────────────────
async function trackTikTok({ ref, total, variantId, quantity, phone, ttp, ttclid, eventId, sourceUrl, ip, userAgent }) {
  const PIXEL_ID     = process.env.TT_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TT_ACCESS_TOKEN;

  if (!PIXEL_ID || !ACCESS_TOKEN) {
    console.warn('[tracking:tiktok] TT_PIXEL_ID or TT_ACCESS_TOKEN not set — skipping');
    return;
  }

  const user = {
    ip:         ip        || '',
    user_agent: userAgent || '',
  };

  if (phone) user.phone = sha256(phone.replace(/\D/g, ''));
  if (ttp)    user.ttp   = ttp;
  if (ttclid) user.ttclid = ttclid;

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
          value:    parseFloat(total) || 0,
          order_id: ref,
          contents: [{
            content_id:   String(variantId),
            content_type: 'product',
            quantity:     parseInt(quantity) || 1,
            price:        parseFloat(total) || 0
          }]
        }
      }]
    })
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`TikTok Events API ${res.status}: ${body}`);
  }

  const json = await res.json().catch(() => ({}));
  console.log('[tracking:tiktok] purchase fired — code:', json.code, '— ref:', ref);
}

// ── Main export — fire all platforms in parallel ──────────────
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