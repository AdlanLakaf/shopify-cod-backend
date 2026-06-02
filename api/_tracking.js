// ============================================================
//  Server-Side Conversion Tracking
//  Purchase  → GA4 (MP) + Meta CAPI + TikTok Events API
//  Funnel    → Meta CAPI + TikTok Events API
// ============================================================

import crypto from 'crypto';

const META_TEST_EVENT_CODE   = process.env.META_TEST_EVENT_CODE   || '';
const TIKTOK_TEST_EVENT_CODE = process.env.TIKTOK_TEST_EVENT_CODE || '';

// ── Utilities ─────────────────────────────────────────────────────────────────

const sha256 = str =>
  crypto.createHash('sha256').update(String(str || '').toLowerCase().trim()).digest('hex');

function toE164DZ(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.startsWith('213') && digits.length >= 12) return digits;
  if (digits.startsWith('0') && digits.length === 10)  return '213' + digits.slice(1);
  if (digits.length === 9)                              return '213' + digits;
  return digits;
}

// ── Identity builders ─────────────────────────────────────────────────────────

function buildMetaUserData({ phone, name, city, state, fbp, fbc, externalId, ip, userAgent }) {
  const userData = {
    client_ip_address: ip        || '',
    client_user_agent: userAgent || '',
    country:           [sha256('dz')]
  };
  if (externalId) userData.external_id = [String(externalId)];
  const e164 = toE164DZ(phone);
  if (e164) userData.ph = [sha256(e164)];
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
  const user = { ip: ip || '', user_agent: userAgent || '' };
  const e164 = toE164DZ(phone);
  if (e164)   user.phone  = sha256(e164);
  if (ttp)    user.ttp    = ttp;
  if (ttclid) user.ttclid = ttclid;
  return user;
}

// ── GA4 Measurement Protocol ──────────────────────────────────────────────────

async function trackGA4({ ref, total, unitPrice, variantId, quantity, productTitle, gaClientId, sessionId }) {
  const tag = '[GA4][purchase]';
  const MEASUREMENT_ID = process.env.GA4_MEASUREMENT_ID;
  const API_SECRET     = process.env.GA4_API_SECRET;

  if (!MEASUREMENT_ID && !API_SECRET) {
    console.warn(`${tag} SKIP — GA4_MEASUREMENT_ID and GA4_API_SECRET are both missing`);
    return;
  }
  if (!MEASUREMENT_ID) {
    console.warn(`${tag} SKIP — GA4_MEASUREMENT_ID is missing`);
    return;
  }
  if (!API_SECRET) {
    console.warn(`${tag} SKIP — GA4_API_SECRET is missing`);
    return;
  }

  const params = {
    transaction_id:       ref,
    value:                parseFloat(total) || 0,
    currency:             'DZD',
    engagement_time_msec: 100,
    items: [{
      item_id:   String(variantId),
      item_name: productTitle || '',
      quantity:  parseInt(quantity) || 1,
      price:     parseFloat(unitPrice) || 0
    }]
  };
  if (sessionId) params.session_id = sessionId;

  const clientId = gaClientId || ('server.' + Date.now());
  console.log(`${tag} firing — ref:${ref} value:${params.value} DZD client_id:${clientId}`);

  const res = await fetch(
    `https://www.google-analytics.com/mp/collect?measurement_id=${MEASUREMENT_ID}&api_secret=${API_SECRET}`,
    {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ client_id: clientId, events: [{ name: 'purchase', params }] })
    }
  );

  // GA4 MP always returns 204 on success (no body)
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`GA4 MP HTTP ${res.status}: ${body}`);
  }
  console.log(`${tag} OK — HTTP ${res.status}`);
}

// ── Meta CAPI — Purchase ──────────────────────────────────────────────────────

async function trackMeta({ ref, total, variantId, quantity, productTitle, contentCategory, phone, name, city, state, fbp, fbc, externalId, gclid, eventId, sourceUrl, ip, userAgent }) {
  const tag = '[Meta CAPI][Purchase]';
  const PIXEL_ID     = process.env.META_PIXEL_ID;
  const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — META_PIXEL_ID and META_ACCESS_TOKEN are both missing`);
    return;
  }
  if (!PIXEL_ID) {
    console.warn(`${tag} SKIP — META_PIXEL_ID is missing`);
    return;
  }
  if (!ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — META_ACCESS_TOKEN is missing`);
    return;
  }

  const qty       = parseInt(quantity) || 1;
  const unitValue = (parseFloat(total) || 0) / qty;
  const userData  = buildMetaUserData({ phone, name, city, state, fbp, fbc, externalId, ip, userAgent });

  const customData = {
    value:        parseFloat(total) || 0,
    currency:     'DZD',
    order_id:     ref,
    content_ids:  [String(variantId)],
    content_type: 'product',
    num_items:    qty,
    contents:     [{ id: String(variantId), quantity: qty, item_price: Number(unitValue.toFixed(2)) }]
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
  if (gclid) eventPayload.referrer_url = sourceUrl || '';

  const body = { data: [eventPayload] };
  if (META_TEST_EVENT_CODE) body.test_event_code = META_TEST_EVENT_CODE;

  console.log(`${tag} firing — pixel:${PIXEL_ID} ref:${ref} value:${customData.value} DZD event_id:${eventPayload.event_id} fbp:${fbp || 'none'} fbc:${fbc || 'none'} externalId:${externalId || 'none'} ip:${ip || 'none'}`);

  const res  = await fetch(
    `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
  );
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`Meta CAPI HTTP ${res.status}: ${JSON.stringify(json)}`);
  }
  console.log(`${tag} OK — events_received:${json.events_received} messages:${JSON.stringify(json.messages || [])}`);
}

// ── TikTok Events API — Purchase ─────────────────────────────────────────────

async function trackTikTok({ ref, total, variantId, quantity, productTitle, phone, ttp, ttclid, eventId, sourceUrl, ip, userAgent }) {
  const tag = '[TikTok][Purchase]';
  const PIXEL_ID     = process.env.TIKTOK_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID and TIKTOK_ACCESS_TOKEN are both missing`);
    return;
  }
  if (!PIXEL_ID) {
    console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID is missing`);
    return;
  }
  if (!ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — TIKTOK_ACCESS_TOKEN is missing`);
    return;
  }

  const user  = buildTikTokUser({ phone, ttp, ttclid, ip, userAgent });
  const price = parseFloat(total) || 0;

  console.log(`${tag} firing — pixel:${PIXEL_ID} ref:${ref} value:${price} DZD event_id:${eventId || ref} ttp:${ttp || 'none'}`);

  const res  = await fetch('https://business-api.tiktok.com/open_api/v1.3/event/track/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Access-Token': ACCESS_TOKEN },
    body: JSON.stringify({
      pixel_code:      PIXEL_ID,
      event_source:    'web',
      event_source_id: PIXEL_ID,
      test_event_code: TIKTOK_TEST_EVENT_CODE || undefined,
      data: [{
        event:      'Purchase',
        event_time: Math.floor(Date.now() / 1000),
        event_id:   eventId || ref,
        user,
        page: { url: sourceUrl || '' },
        properties: {
          currency: 'DZD',
          value:    price,
          order_id: ref,
          contents: [{ content_id: String(variantId), content_type: 'product', content_name: productTitle || '', quantity: parseInt(quantity) || 1, price }]
        }
      }]
    })
  });
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`TikTok API HTTP ${res.status}: ${JSON.stringify(json)}`);
  }
  if (json.code !== 0) {
    throw new Error(`TikTok API error code ${json.code}: ${json.message || JSON.stringify(json)}`);
  }
  console.log(`${tag} OK — code:${json.code} message:${json.message}`);
}

// ── Meta CAPI — Funnel event ──────────────────────────────────────────────────

async function trackMetaEvent({ eventName, value, variantId, quantity, productTitle, contentCategory, searchString, phone, name, city, state, fbp, fbc, externalId, eventId, sourceUrl, ip, userAgent }) {
  const tag = `[Meta CAPI][${eventName}]`;
  const PIXEL_ID     = process.env.META_PIXEL_ID;
  const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — META_PIXEL_ID and META_ACCESS_TOKEN are both missing`);
    return;
  }
  if (!PIXEL_ID) {
    console.warn(`${tag} SKIP — META_PIXEL_ID is missing`);
    return;
  }
  if (!ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — META_ACCESS_TOKEN is missing`);
    return;
  }

  const qty      = parseInt(quantity) || 1;
  const val      = parseFloat(value) || 0;
  const userData = buildMetaUserData({ phone, name, city, state, fbp, fbc, externalId, ip, userAgent });

  const customData = {
    value:        val,
    currency:     'DZD',
    content_ids:  variantId ? [String(variantId)] : [],
    content_type: 'product',
    num_items:    qty
  };
  if (variantId)       customData.contents          = [{ id: String(variantId), quantity: qty, item_price: qty ? Number((val / qty).toFixed(2)) : val }];
  if (productTitle)    customData.content_name      = productTitle;
  if (contentCategory) customData.content_category  = contentCategory;
  if (eventName === 'Search' && searchString) customData.search_string = searchString;

  const body = {
    data: [{
      event_name:       eventName,
      event_time:       Math.floor(Date.now() / 1000),
      event_id:         eventId || undefined,
      event_source_url: sourceUrl || '',
      action_source:    'website',
      user_data:        userData,
      custom_data:      customData
    }]
  };
  if (META_TEST_EVENT_CODE) body.test_event_code = META_TEST_EVENT_CODE;

  console.log(`${tag} firing — pixel:${PIXEL_ID} value:${val} DZD event_id:${eventId || 'none'} variant:${variantId || 'none'} fbp:${fbp || 'none'} externalId:${externalId || 'none'}`);

  const res  = await fetch(
    `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
  );
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`Meta CAPI HTTP ${res.status}: ${JSON.stringify(json)}`);
  }
  console.log(`${tag} OK — events_received:${json.events_received} messages:${JSON.stringify(json.messages || [])}`);
}

// ── TikTok Events API — Funnel event ─────────────────────────────────────────

async function trackTikTokEvent({ eventName, value, variantId, quantity, productTitle, searchString, phone, ttp, ttclid, eventId, sourceUrl, ip, userAgent }) {
  const tag = `[TikTok][${eventName}]`;
  const PIXEL_ID     = process.env.TIKTOK_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID and TIKTOK_ACCESS_TOKEN are both missing`);
    return;
  }
  if (!PIXEL_ID) {
    console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID is missing`);
    return;
  }
  if (!ACCESS_TOKEN) {
    console.warn(`${tag} SKIP — TIKTOK_ACCESS_TOKEN is missing`);
    return;
  }

  const user = buildTikTokUser({ phone, ttp, ttclid, ip, userAgent });
  const val  = parseFloat(value) || 0;

  console.log(`${tag} firing — pixel:${PIXEL_ID} value:${val} DZD event_id:${eventId || 'none'} variant:${variantId || 'none'} ttp:${ttp || 'none'}`);

  const res  = await fetch('https://business-api.tiktok.com/open_api/v1.3/event/track/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Access-Token': ACCESS_TOKEN },
    body: JSON.stringify({
      pixel_code:      PIXEL_ID,
      event_source:    'web',
      event_source_id: PIXEL_ID,
      test_event_code: TIKTOK_TEST_EVENT_CODE || undefined,
      data: [{
        event:      eventName,
        event_time: Math.floor(Date.now() / 1000),
        event_id:   eventId || undefined,
        user,
        page: { url: sourceUrl || '' },
        properties: Object.assign(
          {
            currency: 'DZD',
            value:    val,
            contents: variantId ? [{
              content_id:   String(variantId),
              content_type: 'product',
              content_name: productTitle || '',
              quantity:     parseInt(quantity) || 1,
              price:        Number((val / (parseInt(quantity) || 1)).toFixed(2))
            }] : []
          },
          eventName === 'Search' && searchString ? { query: searchString } : {}
        )
      }]
    })
  });
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`TikTok API HTTP ${res.status}: ${JSON.stringify(json)}`);
  }
  if (json.code !== 0) {
    throw new Error(`TikTok API error code ${json.code}: ${json.message || JSON.stringify(json)}`);
  }
  console.log(`${tag} OK — code:${json.code} message:${json.message}`);
}

// ── Public exports ────────────────────────────────────────────────────────────

export async function trackPurchase(data) {
  console.log(`[tracking] Purchase — ref:${data.ref} GA4_set:${!!(process.env.GA4_MEASUREMENT_ID && process.env.GA4_API_SECRET)} Meta_set:${!!(process.env.META_PIXEL_ID && process.env.META_ACCESS_TOKEN)} TikTok_set:${!!(process.env.TIKTOK_PIXEL_ID && process.env.TIKTOK_ACCESS_TOKEN)}`);

  const results = await Promise.allSettled([
    trackGA4(data),
    trackMeta(data),
    trackTikTok(data),
  ]);

  results.forEach((r, i) => {
    if (r.status === 'rejected') {
      console.error(`[tracking][${['GA4', 'Meta', 'TikTok'][i]}] Purchase FAILED:`, r.reason?.message || r.reason);
    }
  });
}

const META_ONLY = new Set(['FindLocation']);

export async function trackEvent(data) {
  console.log(`[tracking] ${data.eventName} — Meta_set:${!!(process.env.META_PIXEL_ID && process.env.META_ACCESS_TOKEN)} TikTok_set:${!!(process.env.TIKTOK_PIXEL_ID && process.env.TIKTOK_ACCESS_TOKEN)}`);

  const tasks     = [trackMetaEvent(data)];
  const platforms = ['Meta'];
  if (!META_ONLY.has(data.eventName)) {
    tasks.push(trackTikTokEvent(data));
    platforms.push('TikTok');
  }

  const results = await Promise.allSettled(tasks);
  results.forEach((r, i) => {
    if (r.status === 'rejected') {
      console.error(`[tracking][${platforms[i]}] ${data.eventName} FAILED:`, r.reason?.message || r.reason);
    }
  });
}
