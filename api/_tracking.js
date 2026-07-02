// ============================================================
//  Server-Side Conversion Tracking
//  Purchase  → GA4 (MP) + Meta CAPI + TikTok Events API
//  Funnel    → Meta CAPI + TikTok Events API
// ============================================================

import crypto from 'crypto';
import { fetchWithTimeout, log } from './_security.js';

const META_TEST_EVENT_CODE   = process.env.META_TEST_EVENT_CODE   || '';
const TIKTOK_TEST_EVENT_CODE = process.env.TIKTOK_TEST_EVENT_CODE || '';

// ── Utilities ─────────────────────────────────────────────────────────────────

const sha256  = str => crypto.createHash('sha256').update(String(str || '').toLowerCase().trim()).digest('hex');
const maskIp  = ip  => ip ? ip.split('.').slice(0, 2).concat(['*', '*']).join('.') : 'none'; // privacy: log only first 2 octets

function toE164DZ(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.startsWith('213') && digits.length >= 12) return digits;
  if (digits.startsWith('0') && digits.length === 10)  return '213' + digits.slice(1);
  if (digits.length === 9)                              return '213' + digits;
  return digits;
}

// Meta flags a "modified fbclid" (lowercased / truncated / %-encoded) in the
// fbc parameter and drops Event Match Quality. The cookie should already be a
// raw `fb.1.<ts>.<fbclid>`, but a small slice of ad clicks arrive via redirects
// that URL-encode the fbclid, so the stored fbc still carries `%3D` padding.
// Validate the shape and undo only that stray percent-encoding — NEVER change
// case or length. A value that doesn't parse is dropped rather than sent
// (Meta still matches on IP + UA + the hashed identifiers).
function cleanFbc(fbc) {
  const m = /^fb\.1\.(\d{6,})\.(.+)$/.exec(String(fbc || '').trim());
  if (!m) return '';
  let id = m[2];
  if (/%[0-9A-Fa-f]{2}/.test(id)) { try { id = decodeURIComponent(id); } catch { /* keep raw */ } }
  return `fb.1.${m[1]}.${id}`;
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
  const fbcClean = cleanFbc(fbc);
  if (fbcClean) userData.fbc = fbcClean;
  return userData;
}

function buildTikTokUser({ phone, ttp, ttclid, ip, userAgent, externalId }) {
  const user = { ip: ip || '', user_agent: userAgent || '' };
  const e164 = toE164DZ(phone);
  if (e164)       user.phone       = sha256(e164);
  if (ttp)        user.ttp         = ttp;
  if (ttclid)     user.ttclid      = ttclid;
  if (externalId) user.external_id = sha256(String(externalId));
  return user;
}

// ── GA4 Measurement Protocol ──────────────────────────────────────────────────

async function trackGA4({ ref, total, unitPrice, variantId, quantity, productTitle, gaClientId, sessionId }) {
  const tag = '[GA4][purchase]';
  const MEASUREMENT_ID = process.env.GA4_MEASUREMENT_ID;
  const API_SECRET     = process.env.GA4_API_SECRET;

  if (!MEASUREMENT_ID && !API_SECRET) { console.warn(`${tag} SKIP — GA4_MEASUREMENT_ID and GA4_API_SECRET both missing`); return; }
  if (!MEASUREMENT_ID) { console.warn(`${tag} SKIP — GA4_MEASUREMENT_ID missing`); return; }
  if (!API_SECRET)     { console.warn(`${tag} SKIP — GA4_API_SECRET missing`); return; }

  const params = {
    transaction_id:       ref,
    value:                parseFloat(total) || 0,
    currency:             'DZD',
    engagement_time_msec: 100,
    items: [{ item_id: String(variantId), item_name: productTitle || '', quantity: parseInt(quantity) || 1, price: parseFloat(unitPrice) || 0 }]
  };
  if (sessionId) params.session_id = sessionId;

  const clientId = gaClientId || ('server.' + Date.now());
  log(`${tag} firing — ref:${ref} value:${params.value} DZD`);

  const res = await fetchWithTimeout(
    `https://www.google-analytics.com/mp/collect?measurement_id=${MEASUREMENT_ID}&api_secret=${API_SECRET}`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ client_id: clientId, events: [{ name: 'purchase', params }] }) },
    10_000
  );

  if (!res.ok) throw new Error(`GA4 MP HTTP ${res.status}`);
  log(`${tag} OK — HTTP ${res.status}`);
}

// ── Meta CAPI — Purchase ──────────────────────────────────────────────────────

async function trackMeta({ ref, total, variantId, quantity, productTitle, contentCategory, phone, name, city, state, fbp, fbc, externalId, gclid, eventId, sourceUrl, ip, userAgent, metaTestCode }) {
  const tag = '[Meta CAPI][Purchase]';
  const PIXEL_ID     = process.env.META_PIXEL_ID;
  const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) { console.warn(`${tag} SKIP — META_PIXEL_ID and META_ACCESS_TOKEN both missing`); return; }
  if (!PIXEL_ID)     { console.warn(`${tag} SKIP — META_PIXEL_ID missing`); return; }
  if (!ACCESS_TOKEN) { console.warn(`${tag} SKIP — META_ACCESS_TOKEN missing`); return; }

  const qty       = parseInt(quantity) || 1;
  const unitValue = (parseFloat(total) || 0) / qty;
  const userData  = buildMetaUserData({ phone, name, city, state, fbp, fbc, externalId, ip, userAgent });

  const customData = {
    value: parseFloat(total) || 0, currency: 'DZD', order_id: ref,
    content_ids: [String(variantId)], content_type: 'product', num_items: qty,
    contents: [{ id: String(variantId), quantity: qty, item_price: Number(unitValue.toFixed(2)) }]
  };
  if (productTitle)    customData.content_name     = productTitle;
  if (contentCategory) customData.content_category = contentCategory;

  const eventPayload = {
    event_name: 'Purchase', event_time: Math.floor(Date.now() / 1000),
    event_id: eventId || ref, event_source_url: sourceUrl || '',
    action_source: 'website', user_data: userData, custom_data: customData
  };
  if (gclid) eventPayload.referrer_url = sourceUrl || '';

  const body = { data: [eventPayload] };
  // null = explicitly skipped by caller (production or staff skip) — never fall back to env var
  const _metaPurchaseCode = metaTestCode !== undefined ? (metaTestCode || undefined) : (META_TEST_EVENT_CODE || undefined);
  if (_metaPurchaseCode) body.test_event_code = _metaPurchaseCode;

  log(`${tag} firing — ref:${ref} value:${customData.value} DZD ip:${maskIp(ip)}`);

  const res  = await fetchWithTimeout(
    `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
    10_000
  );
  const json = await res.json().catch(() => ({}));

  if (!res.ok) throw new Error(`Meta CAPI HTTP ${res.status}`);
  log(`${tag} OK — events_received:${json.events_received}`);
}

// ── TikTok Events API — Purchase ─────────────────────────────────────────────

async function trackTikTok({ ref, total, variantId, quantity, productTitle, contentCategory, brand, description, phone, ttp, ttclid, externalId, eventId, sourceUrl, ip, userAgent, tiktokTestCode }) {
  const tag = '[TikTok][Purchase]';
  const PIXEL_ID     = process.env.TIKTOK_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) { console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID and TIKTOK_ACCESS_TOKEN both missing`); return; }
  if (!PIXEL_ID)     { console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID missing`); return; }
  if (!ACCESS_TOKEN) { console.warn(`${tag} SKIP — TIKTOK_ACCESS_TOKEN missing`); return; }

  const user      = buildTikTokUser({ phone, ttp, ttclid, ip, userAgent, externalId });
  const value     = parseFloat(total) || 0;
  const qty       = parseInt(quantity) || 1;
  const unitPrice = Number((value / qty).toFixed(2));

  const props = {
    currency:  'DZD',
    value,
    order_id:  ref,
    price:     unitPrice,
    num_items: qty,
    contents:  [{ content_id: String(variantId), content_type: 'product', content_name: productTitle || '', quantity: qty, price: unitPrice }]
  };
  if (brand)           props.brand            = brand;
  if (contentCategory) props.content_category = contentCategory;
  if (productTitle)    props.content_name     = productTitle;
  if (description)     props.description      = description;

  log(`${tag} firing — ref:${ref} value:${value} DZD ip:${maskIp(ip)}`);

  const res  = await fetchWithTimeout('https://business-api.tiktok.com/open_api/v1.3/event/track/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Access-Token': ACCESS_TOKEN },
    body: JSON.stringify({
      pixel_code: PIXEL_ID, event_source: 'web', event_source_id: PIXEL_ID,
      test_event_code: tiktokTestCode !== undefined ? (tiktokTestCode || undefined) : (TIKTOK_TEST_EVENT_CODE || undefined),
      data: [{ event: 'Purchase', event_time: Math.floor(Date.now() / 1000), event_id: eventId || ref, user,
        page: { url: sourceUrl || '' },
        properties: props
      }]
    })
  }, 10_000);
  const json = await res.json().catch(() => ({}));

  if (!res.ok) throw new Error(`TikTok API HTTP ${res.status}`);
  if (json.code !== 0) throw new Error(`TikTok API error code ${json.code}: ${json.message}`);
  log(`${tag} OK — code:${json.code}`);
}

// ── Meta CAPI — Funnel event ──────────────────────────────────────────────────

async function trackMetaEvent({ eventName, value, variantId, quantity, productTitle, contentCategory, searchString, phone, name, city, state, fbp, fbc, externalId, eventId, sourceUrl, ip, userAgent, metaTestCode }) {
  const tag = `[Meta CAPI][${eventName}]`;
  const PIXEL_ID     = process.env.META_PIXEL_ID;
  const ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) { console.warn(`${tag} SKIP — META_PIXEL_ID and META_ACCESS_TOKEN both missing`); return; }
  if (!PIXEL_ID)     { console.warn(`${tag} SKIP — META_PIXEL_ID missing`); return; }
  if (!ACCESS_TOKEN) { console.warn(`${tag} SKIP — META_ACCESS_TOKEN missing`); return; }

  const qty      = parseInt(quantity) || 1;
  const val      = parseFloat(value) || 0;
  const userData = buildMetaUserData({ phone, name, city, state, fbp, fbc, externalId, ip, userAgent });

  const customData = { value: val, currency: 'DZD', content_ids: variantId ? [String(variantId)] : [], content_type: 'product', num_items: qty };
  if (variantId)       customData.contents          = [{ id: String(variantId), quantity: qty, item_price: qty ? Number((val / qty).toFixed(2)) : val }];
  if (productTitle)    customData.content_name      = productTitle;
  if (contentCategory) customData.content_category  = contentCategory;
  if (eventName === 'Search' && searchString) customData.search_string = searchString;

  const body = { data: [{ event_name: eventName, event_time: Math.floor(Date.now() / 1000), event_id: eventId || undefined,
    event_source_url: sourceUrl || '', action_source: 'website', user_data: userData, custom_data: customData }] };
  const _metaCode = metaTestCode !== undefined ? (metaTestCode || undefined) : (META_TEST_EVENT_CODE || undefined);
  if (_metaCode) body.test_event_code = _metaCode;

  console.log(`${tag} firing — value:${val} DZD ip:${maskIp(ip)}`);

  const res  = await fetchWithTimeout(
    `https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
    10_000
  );
  const json = await res.json().catch(() => ({}));

  if (!res.ok) throw new Error(`Meta CAPI HTTP ${res.status}`);
  log(`${tag} OK — events_received:${json.events_received}`);
}

// ── TikTok Events API — Funnel event ─────────────────────────────────────────

async function trackTikTokEvent({ eventName, value, variantId, quantity, productTitle, contentCategory, brand, description, searchString, phone, ttp, ttclid, externalId, eventId, sourceUrl, ip, userAgent, tiktokTestCode }) {
  const tag = `[TikTok][${eventName}]`;
  const PIXEL_ID     = process.env.TIKTOK_PIXEL_ID;
  const ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN;

  if (!PIXEL_ID && !ACCESS_TOKEN) { console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID and TIKTOK_ACCESS_TOKEN both missing`); return; }
  if (!PIXEL_ID)     { console.warn(`${tag} SKIP — TIKTOK_PIXEL_ID missing`); return; }
  if (!ACCESS_TOKEN) { console.warn(`${tag} SKIP — TIKTOK_ACCESS_TOKEN missing`); return; }

  const user      = buildTikTokUser({ phone, ttp, ttclid, ip, userAgent, externalId });
  const val       = parseFloat(value) || 0;
  const qty       = parseInt(quantity) || 1;
  const unitPrice = Number((val / qty).toFixed(2));

  const props = {
    currency:  'DZD',
    value:     val,
    price:     unitPrice,
    num_items: qty,
    contents:  variantId ? [{ content_id: String(variantId), content_type: 'product', content_name: productTitle || '', quantity: qty, price: unitPrice }] : []
  };
  if (brand)           props.brand            = brand;
  if (contentCategory) props.content_category = contentCategory;
  if (productTitle)    props.content_name     = productTitle;
  if (description)     props.description      = description;
  if (eventName === 'Search' && searchString) props.search_string = searchString;

  log(`${tag} firing — value:${val} DZD ip:${maskIp(ip)}`);

  const res  = await fetchWithTimeout('https://business-api.tiktok.com/open_api/v1.3/event/track/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Access-Token': ACCESS_TOKEN },
    body: JSON.stringify({
      pixel_code: PIXEL_ID, event_source: 'web', event_source_id: PIXEL_ID,
      test_event_code: tiktokTestCode !== undefined ? (tiktokTestCode || undefined) : (TIKTOK_TEST_EVENT_CODE || undefined),
      data: [{ event: eventName, event_time: Math.floor(Date.now() / 1000), event_id: eventId || undefined, user,
        page: { url: sourceUrl || '' },
        properties: props
      }]
    })
  }, 10_000);
  const json = await res.json().catch(() => ({}));

  if (!res.ok) throw new Error(`TikTok API HTTP ${res.status}`);
  if (json.code !== 0) throw new Error(`TikTok API error code ${json.code}: ${json.message}`);
  log(`${tag} OK — code:${json.code}`);
}

// ── Public exports ────────────────────────────────────────────────────────────

export async function trackPurchase(data) {
  const tasks = [], labels = [];
  if (!data.skipGA4)    { tasks.push(trackGA4(data));                                                  labels.push('GA4');    }
  if (!data.skipMeta)   { tasks.push(trackMeta({ ...data, metaTestCode: data.metaTestCode }));          labels.push('Meta');   }
  if (!data.skipTikTok) { tasks.push(trackTikTok({ ...data, tiktokTestCode: data.tiktokTestCode }));    labels.push('TikTok'); }
  log(`[tracking] Purchase — ref:${data.ref} firing:[${labels.join(',')}]`);
  const results = await Promise.allSettled(tasks);
  results.forEach((r, i) => {
    if (r.status === 'rejected')
      console.error(`[tracking][${labels[i]}] Purchase FAILED:`, r.reason?.message);
  });
  // Per-platform outcome for callers that report back (admin fire-event).
  // Existing callers (create-order) ignore the return value.
  return results.map((r, i) => ({
    platform: labels[i],
    ok:       r.status === 'fulfilled',
    error:    r.status === 'rejected' ? String(r.reason?.message || r.reason) : null,
  }));
}

// Events that should never be sent to TikTok (Meta custom events with no TikTok equivalent).
const META_ONLY = new Set(['CustomizeProduct']);

export async function trackEvent(data) {
  const tasks = [], platforms = [];
  if (!data.skipMeta)   { tasks.push(trackMetaEvent({ ...data, metaTestCode: data.metaTestCode }));                                              platforms.push('Meta');   }
  if (!data.skipTikTok && !META_ONLY.has(data.eventName)) { tasks.push(trackTikTokEvent({ ...data, tiktokTestCode: data.tiktokTestCode }));     platforms.push('TikTok'); }
  log(`[tracking] ${data.eventName} — firing:[${platforms.join(',')}]`);
  const results = await Promise.allSettled(tasks);
  results.forEach((r, i) => {
    if (r.status === 'rejected')
      console.error(`[tracking][${platforms[i]}] ${data.eventName} FAILED:`, r.reason?.message);
  });
}
