// ============================================================
//  Shopify COD — Vercel Serverless Function
//  POST /api/create-order
//  Creates a Draft Order then immediately completes it
//  Security: HMAC + timestamp + rate limiting + origin check
// ============================================================

import { runSecurityChecks, verifyTurnstile, fetchWithTimeout, log } from './_security.js';
import { trackPurchase } from './_tracking.js';
import { insertOrder, updateOrderShopify } from './_orders-db.js';
import { markLeadConverted } from './_leads-db.js';
import { markSessionConverted } from './_funnel-db.js';
import { detectSource, resolveAdPlatform } from './_attribution.js';
import { getTestMode }   from './_test-mode.js';

// ── Idempotency — the same eventId never creates two orders ──────────────────
// The theme retries automatically on timeout/network failure; if the first
// attempt actually reached Shopify, the retry gets the cached response back
// instead of a duplicate order. Railway runs one persistent process, so an
// in-memory Map is reliable here.
const idempotencyStore = new Map(); // eventId → { ts, pending, status, body }
const IDEM_TTL = 15 * 60 * 1000;
setInterval(() => {
  const cutoff = Date.now() - IDEM_TTL;
  for (const [k, v] of idempotencyStore) {
    if (v.ts < cutoff) idempotencyStore.delete(k);
  }
}, 5 * 60 * 1000).unref();

// ── Money guards — the client must never dictate what we store or report ─────
// COD totals are confirmed by phone, but a tampered client could still poison
// our orders-DB totals and (worse) the conversion VALUE we report to Meta /
// TikTok / GA4, which trains ad delivery on garbage. We bound every monetary
// input and RECOMPUTE the order total server-side from the validated line
// items — the client-supplied total is never trusted for money.
const MAX_UNIT_PRICE_DZD  = 200_000;    // ceiling for a single perfume / bundle unit
const MAX_SHIPPING_DZD    = 5_000;      // ZR / Yalidine COD shipping tops out ~1.5k
const MAX_ORDER_TOTAL_DZD = 2_000_000;  // hard cap on a whole order

// Coerce to a clean DZD integer within [0, max]; returns null when the value is
// non-finite, negative, or above the ceiling so callers can REJECT (not clamp)
// a hostile price — clamping a 999999 down to the ceiling still pollutes pixels.
function cleanMoney(v, max) {
  const n = Math.round(Number(v));
  if (!Number.isFinite(n) || n < 0 || n > max) return null;
  return n;
}

// Order-line thumbnail URL. Client-supplied (the landing sends the product's
// CDN image), so accept ONLY https Shopify CDN hosts — it ends up in an admin
// <img src> and must never become an arbitrary external/tracking URL. Anything
// else is dropped (the admin falls back to initials). Returns '' when invalid.
function cleanImageUrl(v) {
  if (typeof v !== 'string' || v.length > 1000) return '';
  try {
    const u = new URL(v.trim());
    if (u.protocol !== 'https:') return '';
    if (!/(^|\.)(cdn\.shopify\.com|shopifycdn\.com|myshopify\.com)$/.test(u.hostname)) return '';
    return u.toString();
  } catch { return ''; }
}

// ── Per-phone order cap — the only spam guard a real buyer can never hit ─────
const phoneOrderStore = new Map(); // phone → { count, windowStart }
const PHONE_MAX_ORDERS = 6;
const PHONE_WINDOW     = 60 * 60 * 1000;
setInterval(() => {
  const cutoff = Date.now() - PHONE_WINDOW;
  for (const [k, v] of phoneOrderStore) {
    if (v.windowStart < cutoff) phoneOrderStore.delete(k);
  }
}, 10 * 60 * 1000).unref();

// ── Shopify draft create → offer price-match → complete ──────────────────────
// Returns the completed draft_order (carries order_id, total_price, line_items)
// or throws an Error (with `.status`) on a hard failure. Used by BOTH the
// synchronous fallback path and the fire-and-forget background push of the
// orders-DB-first fast path — keep the two behaviours identical by routing all
// Shopify writes through here.
async function pushOrderToShopify({ SHOP, TOKEN, API_VER, draftPayload, lineItems, merchTotalDzd, ref }) {
  // 1) Create the draft
  const draftRes = await fetchWithTimeout(
    `https://${SHOP}/admin/api/${API_VER}/draft_orders.json`,
    { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN }, body: JSON.stringify(draftPayload) },
    15_000
  );
  if (!draftRes.ok) {
    const e = new Error('Failed to create draft order'); e.status = 502; throw e;
  }
  const draftData = await draftRes.json();
  const draftId = draftData.draft_order.id;
  log('Draft order created:', draftId);

  // 2) Offer price-match — reconcile the Shopify subtotal to the advertised
  //    offer total. Never fail the order over a price drift (best-effort).
  const targetMerch = Number(merchTotalDzd);
  if (Number.isFinite(targetMerch) && targetMerch > 0) {
    const draftSubtotal = parseFloat(draftData.draft_order.subtotal_price || '0');
    const diff = draftSubtotal - targetMerch; // >0 → discount, <0 → surcharge
    const sane = draftSubtotal > 0 && targetMerch >= draftSubtotal * 0.05 && targetMerch <= draftSubtotal * 20;

    if (!sane) {
      console.warn(`[order] offer total ${targetMerch} vs subtotal ${draftSubtotal} implausible — price-match skipped (REF ${ref})`);
    } else if (diff >= 1) {
      try {
        const discRes = await fetchWithTimeout(
          `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}.json`,
          { method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
            body: JSON.stringify({ draft_order: { id: draftId, applied_discount: {
              description: 'سعر العرض — landing offer price', title: 'OFFER',
              value_type: 'fixed_amount', value: diff.toFixed(2), amount: diff.toFixed(2) } } }) },
          10_000
        );
        if (discRes.ok) log(`[order] offer discount applied: -${diff.toFixed(2)} DZD (target ${targetMerch})`);
        else console.warn('[order] offer discount PUT failed — HTTP', discRes.status, `(REF ${ref})`);
      } catch (err) { console.warn('[order] offer discount error:', err.message, `(REF ${ref})`); }
    } else if (diff <= -1) {
      const extra = -diff;
      try {
        const surchargeLines = [...lineItems, { title: 'سعر العرض — landing offer price', price: extra.toFixed(2), quantity: 1 }];
        const surRes = await fetchWithTimeout(
          `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}.json`,
          { method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
            body: JSON.stringify({ draft_order: { id: draftId, line_items: surchargeLines } }) },
          10_000
        );
        if (surRes.ok) log(`[order] offer surcharge applied: +${extra.toFixed(2)} DZD (target ${targetMerch})`);
        else console.warn('[order] offer surcharge PUT failed — HTTP', surRes.status, `(REF ${ref})`);
      } catch (err) { console.warn('[order] offer surcharge error:', err.message, `(REF ${ref})`); }
    }
  }

  // 3) Complete the draft (payment pending = COD)
  const completeRes = await fetchWithTimeout(
    `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}/complete.json?payment_pending=true`,
    { method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN } },
    15_000
  );
  if (!completeRes.ok) {
    const e = new Error('Order created but could not be completed'); e.status = 502; throw e;
  }
  const completeData = await completeRes.json();
  return completeData.draft_order;
}

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, { skipHmac: true, rateBucket: 'order', rateMax: 60 });
  if (blocked) return;
  // ── Staff test mode — skip Turnstile if valid staff token present ──
  const testMode = getTestMode(req.body || {});
  // TURNSTILE DISABLED — verifyTurnstile is a no-op stub; call kept for easy re-enable
  if (!testMode) {
    const turnstileBlock = await verifyTurnstile(req, res);
    if (turnstileBlock) return;
  }
  
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const SHOP    = process.env.SHOPIFY_MYSHOPIFY_DOMAIN || process.env.SHOPIFY_STORE_DOMAIN;
  const TOKEN   = process.env.SHOPIFY_ADMIN_TOKEN;
  const API_VER = '2024-10';

  if (!SHOP || !TOKEN) {
    return res.status(500).json({ error: 'Missing Shopify credentials in environment' });
  }

  const {
    variantId,
    quantity       = 1,
    items          = null,  // optional multi-item orders: [{ variantId?, title?, priceDzd?, quantity }]
    merchTotalDzd  = null,  // page-advertised merchandise subtotal — draft is discounted down to it
    name,
    phone,
    wilaya,
    baladiya,
    address,
    deliveryType,
    shippingCost,
    note: extraNote = '',
    // ── Tracking fields from frontend ──
    eventId         = '',
    fbp             = '',
    fbc             = '',
    gaClientId      = '',
    sessionId       = '',
    externalId      = '',
    ttp             = '',
    ttclid          = '',
    gclid           = '',
    sourceUrl       = '',
    entryUrl        = '',
    referrer        = '',
    trafficSource   = '',
    productTitle    = '',
    contentCategory = '',
    brand           = '',
    description     = '',
    leadId          = '',
    funnelSessionId = ''
  } = req.body;

  const hasItems = Array.isArray(items) && items.length > 0;
  if ((!variantId && !hasItems) || !name || !phone || !wilaya || !baladiya) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const isOfficePickup = deliveryType === 'استلام من المكتب';

  const sanitize = str => String(str || '').replace(/<[^>]*>/g, '').trim().slice(0, 200);

  // ── CHANGE 1: ref moved above sanitize calls so it's ready for orderNote ──
  const ref = 'H&N-ORD-' + Date.now().toString(36).toUpperCase().slice(-6);

  const cleanName     = sanitize(name);
  const cleanPhone    = sanitize(phone).replace(/\s/g, '');
  const cleanWilaya   = sanitize(wilaya);
  const cleanBaladiya = sanitize(baladiya);
  const cleanAddress  = sanitize(address);
  const cleanNote = sanitize(extraNote).replace(/[\n\r]/g, ' ');

  // Name-test detection: if any word in the customer name is exactly "test" (case-insensitive),
  // treat as a QA order — insert into custom DB, create Shopify DRAFT only (no complete), skip tracking.
  const isNameTest = cleanName.trim().split(/\s+/).some(w => w.toLowerCase() === 'test');

  const orderSource = detectSource({
    trafficSource,
    fbc, ttclid, gclid,
    userAgent: req.headers['user-agent'] || '',
    referrer:  sanitize(referrer)
  });
  log(`[order] source detected: ${orderSource}`);
  // Attribution gate — only the hn_src 7-day window decides which CAPI fires.
  const adPlatform = resolveAdPlatform(trafficSource);

  const algerianPhoneRegex = /^(05|06|07)\d{8}$/;
  if (!algerianPhoneRegex.test(cleanPhone)) {
    return res.status(400).json({ error: 'Invalid Algerian phone number' });
  }

  // ── Build draft line items ──────────────────────────────────────────────
  // items[] supports landing-only products (custom title+price lines) and
  // upsell bumps; the single top-level variantId remains for the theme.
  const MAX_ITEMS = 5;
  const clampQty  = q => Math.min(Math.max(parseInt(q) || 1, 1), 10);

  let lineItems;
  if (hasItems) {
    if (items.length > MAX_ITEMS) {
      return res.status(400).json({ error: 'Too many items' });
    }
    lineItems = [];
    for (const item of items) {
      const qty = clampQty(item?.quantity);
      const vid = parseInt(item?.variantId);
      if (!isNaN(vid) && vid > 0) {
        lineItems.push({ variant_id: vid, quantity: qty });
        continue;
      }
      const title = sanitize(item?.title);
      const price = Number(item?.priceDzd);
      if (!title || !Number.isFinite(price) || price < 0 || price > 1_000_000) {
        return res.status(400).json({ error: 'Invalid order item' });
      }
      lineItems.push({ title, price: price.toFixed(2), quantity: qty });
    }
  } else {
    const variantIdInt = parseInt(variantId);
    if (isNaN(variantIdInt) || variantIdInt <= 0) {
      return res.status(400).json({ error: 'Invalid variant ID' });
    }
    lineItems = [{ variant_id: variantIdInt, quantity: clampQty(quantity) }];
  }

  // Tracking identifiers — first real variant in the order, total unit count
  const variantIdInt = lineItems.find(li => li.variant_id)?.variant_id || 0;
  const totalQty     = lineItems.reduce((n, li) => n + li.quantity, 0);

  // ── Idempotency: duplicate submit with the same eventId ──
  const idemKey = eventId && String(eventId).slice(0, 64);
  if (idemKey) {
    const seen = idempotencyStore.get(idemKey);
    if (seen) {
      if (seen.pending) {
        // First attempt still running — tell the client to wait and re-ask
        return res.status(409).json({ error: 'Order is already being processed', code: 'in_progress' });
      }
      log(`[order] idempotent replay for ${idemKey}`);
      return res.status(seen.status).json(seen.body);
    }
    idempotencyStore.set(idemKey, { ts: Date.now(), pending: true });
  }
  // Cache the outcome (or clear on failure so a manual retry can try again)
  const idemResolve = (status, body) => {
    if (idemKey) {
      if (status === 200) idempotencyStore.set(idemKey, { ts: Date.now(), pending: false, status, body });
      else idempotencyStore.delete(idemKey);
    }
    return res.status(status).json(body);
  };

  // ── Per-phone cap (real orders only — test mode bypasses) ──
  if (!testMode) {
    const now   = Date.now();
    const entry = phoneOrderStore.get(cleanPhone);
    if (entry && now - entry.windowStart <= PHONE_WINDOW && entry.count >= PHONE_MAX_ORDERS) {
      console.warn(`[order] phone cap hit: ${cleanPhone}`);
      return idemResolve(429, { error: 'Order limit reached for this phone number. Please try later.', code: 'phone_limit' });
    }
  }

  // ── Shared tracking helper for mock / draft modes ──
  const fireTestTracking = (ref, total) => trackPurchase({
    ref, total, unitPrice: '0',
    variantId: variantIdInt, quantity: totalQty,
    phone: cleanPhone, name: cleanName, city: cleanBaladiya, state: cleanWilaya,
    eventId: eventId || ref, fbp, fbc, externalId, ttp, ttclid, sourceUrl,
    productTitle, contentCategory, brand, description,
    skipGA4:    true,
    skipMeta:   testMode.metaMode   === 'skip',
    skipTikTok: testMode.tiktokMode === 'skip',
    metaTestCode:   testMode.metaMode   === 'test' ? (testMode.metaTestCode   || null) : null,
    tiktokTestCode: testMode.tiktokMode === 'test' ? (testMode.tiktokTestCode || null) : null,
    ip: req.headers['x-forwarded-for']?.split(',')[0]?.trim() || '',
    userAgent: req.headers['user-agent'] || ''
  }).catch(err => console.error('[test] trackPurchase error:', err.message));

  // ── Mock mode: skip Shopify entirely, return fake response ──
  if (testMode?.orderMode === 'mock') {
    const fakeRef = 'H&N-TEST-' + Date.now().toString(36).toUpperCase().slice(-6);
    log(`[test] mock order: ${fakeRef}`);
    await fireTestTracking(fakeRef, String(shippingCost || 0));
    return idemResolve(200, { success: true, ref: fakeRef, orderId: 0, orderName: fakeRef, name: cleanName, phone: cleanPhone, wilaya: cleanWilaya, baladiya: cleanBaladiya, address: cleanAddress, deliveryType: deliveryType || 'توصيل للمنزل', total: String(shippingCost || 0), lineItems: [], _test: true });
  }

  const orderNote = [
    `REF: ${ref}`,
    `الاسم: ${cleanName}`,
    `الهاتف: ${cleanPhone}`,
    `الولاية: ${cleanWilaya}`,
    `البلدية: ${cleanBaladiya}`,
    `العنوان: ${cleanAddress}`,
    `نوع التوصيل: ${deliveryType || 'توصيل للمنزل'}`,
    `طريقة الدفع: الدفع عند الاستلام (COD)`,
    cleanNote ? `ملاحظة: ${cleanNote}` : ''
  ].filter(Boolean).join('\n');

  // Validated shipping (DZD integer in range, else null). Used as-is on the
  // draft's shipping line; the fast path rejects outright when it's null.
  const validShipping = cleanMoney(shippingCost, MAX_SHIPPING_DZD);

  const draftPayload = {
    draft_order: {
      line_items: lineItems,
      shipping_address: {
        first_name: cleanName, phone: cleanPhone,
        address1: cleanAddress, city: cleanBaladiya,
        province: cleanWilaya, country: 'DZ', zip: ''
      },
      billing_address: {
        first_name: cleanName, phone: cleanPhone,
        address1: cleanAddress, city: cleanBaladiya,
        province: cleanWilaya, country: 'DZ', zip: ''
      },
      note: orderNote,
      // ── Shipping line — records delivery cost visibly in Shopify admin ──
      shipping_line: {
        title: deliveryType === 'استلام من المكتب' ? 'Office Pickup / استلام من المكتب' : 'Home Delivery / توصيل للمنزل',
        price: (validShipping ?? 0).toFixed(2),
        code:  deliveryType === 'استلام من المكتب' ? 'office-pickup' : 'home-delivery'
      },
      tags: `COD, ${cleanWilaya}, ${deliveryType === 'استلام من المكتب' ? 'office-pickup' : 'home-delivery'}, REF-${ref}, src-${orderSource}${testMode ? ', TEST, DO-NOT-FULFILL' : ''}${isNameTest ? ', TEST-NAME, DO-NOT-FULFILL' : ''}`,
      send_receipt: false,
      send_fulfillment_receipt: false,
      use_customer_default_address: false
    }
  };

  // ── FAST PATH: our orders DB is primary, Shopify becomes background ──────────
  // The landing posts `displayItems` (titled, priced lines). We persist the
  // order to Postgres FIRST and reply the instant that succeeds, then create the
  // Shopify order + fire conversion tracking as detached background work — the
  // customer never waits on Shopify. The theme (which sends no displayItems) and
  // any DB outage fall through to the proven synchronous path below, so live
  // behaviour is unchanged until DATABASE_URL is configured. Test modes skip this.
  const fastItems = Array.isArray(req.body.displayItems) ? req.body.displayItems : null;
  if (!testMode && fastItems && fastItems.length) {
    // isNameTest orders still use the fast path but skip tracking + skip completing the Shopify draft.
    // Validate every line price — REJECT (don't clamp) a hostile value so a
    // tampered client can neither under- nor over-report what we persist and
    // send to the ad pixels. price 0 is allowed (free gift / sample lines).
    const displayItems = [];
    for (const it of fastItems.slice(0, MAX_ITEMS)) {
      const priceDzd = cleanMoney(it?.priceDzd, MAX_UNIT_PRICE_DZD);
      if (priceDzd === null) {
        return idemResolve(400, { error: 'Invalid item price' });
      }
      displayItems.push({
        title:    sanitize(it?.title) || productTitle || 'Order',
        quantity: clampQty(it?.quantity),
        priceDzd,
        imageUrl: cleanImageUrl(it?.imageUrl),
      });
    }

    if (validShipping === null) {
      return idemResolve(400, { error: 'Invalid shipping cost' });
    }
    const shipDzd = validShipping;
    // Merch total is DERIVED from the validated lines — never the client's
    // merchTotalDzd (that value is only a hint for the Shopify price-match
    // below, which has its own 5%–20× sanity gate against the catalog subtotal).
    const merchDzd = displayItems.reduce((s, i) => s + i.priceDzd * i.quantity, 0);
    const totalDzd = Math.min(merchDzd + shipDzd, MAX_ORDER_TOTAL_DZD);

    // Derive the landing slug from the URL path (/p/<slug>) — more reliable
    // than productTitle which is a display string that can change.
    const slugMatch = sourceUrl.match(/\/p\/([^/?#]+)/);
    const origin    = slugMatch ? slugMatch[1] : (productTitle || sourceUrl || '');

    const savedToDb = await insertOrder({
      ref, status: 'pending',
      name: cleanName, phone: cleanPhone,
      wilaya: cleanWilaya, baladiya: cleanBaladiya, address: cleanAddress,
      deliveryType: deliveryType || 'توصيل للمنزل',
      shippingCost: shipDzd, items: displayItems,
      merchTotalDzd: merchDzd, totalDzd,
      note: (isNameTest ? '[TEST-NAME] ' : '') + cleanNote,
      source: orderSource,
      origin,
      originUrl: sourceUrl || '',
      entryUrl:  entryUrl  || '',
    });

    if (savedToDb) {
      // Link & convert the matching lead (funnel close). Fire-and-forget —
      // a missing lead (organic/direct submit) is fine, never blocks the order.
      markLeadConverted({
        leadId, orderRef: ref, phone: cleanPhone,
        name: cleanName, wilaya: cleanWilaya, baladiya: cleanBaladiya,
        deliveryType: deliveryType || 'توصيل للمنزل',
        merchTotalDzd: merchDzd, shippingCost: shipDzd, totalDzd,
        source: orderSource, origin, originUrl: sourceUrl || '',
      }).catch(err => console.error('[order] lead convert error:', err?.message));
      markSessionConverted({ sessionId: funnelSessionId, leadId, phone: cleanPhone })
        .catch(err => console.error('[order] session convert error:', err?.message));

      // Count this order against the per-phone cap before replying.
      const now = Date.now();
      const entry = phoneOrderStore.get(cleanPhone);
      if (!entry || now - entry.windowStart > PHONE_WINDOW) phoneOrderStore.set(cleanPhone, { count: 1, windowStart: now });
      else entry.count++;

      // Reply immediately — the sale is recorded in our system.
      idemResolve(200, {
        success: true, ref, orderId: 0, orderName: ref,
        name: cleanName, phone: cleanPhone, wilaya: cleanWilaya, baladiya: cleanBaladiya,
        address: cleanAddress, deliveryType: deliveryType || 'توصيل للمنزل',
        total: totalDzd.toFixed(2),
        lineItems: displayItems.map(i => ({ title: i.title, variant: '', quantity: i.quantity, price: i.priceDzd.toFixed(2) })),
      });

      // ── Background (fire-and-forget) — never affects the customer's outcome ──
      if (isNameTest) {
        // Name-based test order: create Shopify DRAFT only (no complete), skip all tracking.
        log(`[order] name-test order ${ref} — draft only, no tracking`);
        fetchWithTimeout(
          `https://${SHOP}/admin/api/${API_VER}/draft_orders.json`,
          { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN }, body: JSON.stringify(draftPayload) },
          15_000
        ).then(r => r.json())
         .then(d => { log('[order] name-test Shopify draft created:', d.draft_order?.id); })
         .catch(err => console.error('[order] name-test Shopify draft failed:', err?.message, `(REF ${ref})`));
      } else {
        // 1) Conversion tracking, using our own computed total (no Shopify needed).
        const unitPrice = String((merchDzd / Math.max(totalQty, 1)).toFixed(2));
        trackPurchase({
          ref, total: totalDzd.toFixed(2), unitPrice,
          variantId: variantIdInt, quantity: totalQty,
          phone: cleanPhone, name: cleanName, city: cleanBaladiya, state: cleanWilaya,
          eventId: eventId || ref,
          fbp, fbc, gaClientId, sessionId, externalId, ttp, ttclid, gclid, sourceUrl,
          productTitle, contentCategory, brand, description,
          skipGA4: false, skipMeta: adPlatform !== 'meta', skipTikTok: adPlatform !== 'tiktok',
          ip: req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || '',
          userAgent: req.headers['user-agent'] || ''
        }).catch(err => console.error('[order] trackPurchase error:', err?.message));

        // 2) Create the real Shopify order, then stamp its id on our row (or flag
        //    the row if Shopify never accepted it — the sale is safe either way).
        pushOrderToShopify({ SHOP, TOKEN, API_VER, draftPayload, lineItems, merchTotalDzd, ref })
          .then(order => { log('[order] background Shopify order:', order.order_id); return updateOrderShopify(ref, { shopifyOrderId: order.order_id }); })
          .catch(err => { console.error('[order] background Shopify failed:', err?.message, `(REF ${ref})`); return updateOrderShopify(ref, { syncFailed: true }); });
      }

      return;
    }

    // DB unreachable (no DATABASE_URL / outage) → fall through to the proven
    // synchronous Shopify path so the sale is never lost.
    log('[order] orders DB unavailable — using synchronous Shopify path');
  }

  let draftId;

  try {
    const draftRes = await fetchWithTimeout(
      `https://${SHOP}/admin/api/${API_VER}/draft_orders.json`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
        body: JSON.stringify(draftPayload)
      },
      15_000
    );
    if (!draftRes.ok) {
      console.error('Draft order creation failed — HTTP', draftRes.status);
      return idemResolve(502, { error: 'Failed to create draft order' });
    }
    const draftData = await draftRes.json();
    draftId = draftData.draft_order.id;
    log('Draft order created:', draftId);

    // ── Offer price-match ────────────────────────────────────────────────
    // Bundles / bump offers ship as REAL variant lines (so fulfillment can
    // resolve the products), but those bill at Shopify catalog prices. The
    // landing sends the exact subtotal the customer saw (merchTotalDzd);
    // reconcile the difference so the Shopify order total equals the page:
    //   • catalog dearer than the offer  → order-level DISCOUNT (e.g. bundle deal)
    //   • catalog cheaper than the offer → custom EXTRA line (surcharge)
    // Never fail the order over this — a price drift is recoverable on the
    // confirmation call, a lost order is not.
    const targetMerch = Number(merchTotalDzd);
    if (Number.isFinite(targetMerch) && targetMerch > 0) {
      const draftSubtotal = parseFloat(draftData.draft_order.subtotal_price || '0');
      const diff = draftSubtotal - targetMerch; // >0 → discount, <0 → surcharge
      // Sanity gate against a corrupt merchTotalDzd zeroing or ballooning the
      // order: the offer must stay within 5%–20× of the catalog subtotal. This
      // is deliberately loose so legitimately deep bundle discounts go through.
      const sane = draftSubtotal > 0
        && targetMerch >= draftSubtotal * 0.05
        && targetMerch <= draftSubtotal * 20;

      if (!sane) {
        console.warn(`[order] offer total ${targetMerch} vs subtotal ${draftSubtotal} implausible — price-match skipped (REF ${ref})`);
      } else if (diff >= 1) {
        // Catalog is dearer than the page — discount the order down to the offer.
        try {
          const discRes = await fetchWithTimeout(
            `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}.json`,
            {
              method: 'PUT',
              headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
              body: JSON.stringify({
                draft_order: {
                  id: draftId,
                  applied_discount: {
                    description: 'سعر العرض — landing offer price',
                    title:       'OFFER',
                    value_type:  'fixed_amount',
                    value:       diff.toFixed(2),
                    amount:      diff.toFixed(2)
                  }
                }
              })
            },
            10_000
          );
          if (discRes.ok) log(`[order] offer discount applied: -${diff.toFixed(2)} DZD (target ${targetMerch})`);
          else console.warn('[order] offer discount PUT failed — HTTP', discRes.status, `(REF ${ref})`);
        } catch (err) {
          console.warn('[order] offer discount error:', err.message, `(REF ${ref})`);
        }
      } else if (diff <= -1) {
        // Catalog is cheaper than the page — Shopify can't apply a negative
        // discount, so add the extra as a custom line item. PUT replaces the
        // line_items array, so resend the originals plus the surcharge line.
        const extra = -diff;
        try {
          const surchargeLines = [
            ...lineItems,
            { title: 'سعر العرض — landing offer price', price: extra.toFixed(2), quantity: 1 }
          ];
          const surRes = await fetchWithTimeout(
            `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}.json`,
            {
              method: 'PUT',
              headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
              body: JSON.stringify({ draft_order: { id: draftId, line_items: surchargeLines } })
            },
            10_000
          );
          if (surRes.ok) log(`[order] offer surcharge applied: +${extra.toFixed(2)} DZD (target ${targetMerch})`);
          else console.warn('[order] offer surcharge PUT failed — HTTP', surRes.status, `(REF ${ref})`);
        } catch (err) {
          console.warn('[order] offer surcharge error:', err.message, `(REF ${ref})`);
        }
      }
    }

    // ── Draft-only mode: stop here, don't complete (no stock deduction) ──
    if (testMode?.orderMode === 'draft' || isNameTest) {
      log(`[${isNameTest ? 'name-test' : 'test'}] draft-only order: ${draftId}`);
      const draftTotal = String(draftData.draft_order?.total_price || shippingCost || 0);
      if (testMode?.orderMode === 'draft') await fireTestTracking(ref, draftTotal);
      // name-test: no tracking at all
      return idemResolve(200, { success: true, ref, orderId: 0, orderName: ref, name: cleanName, phone: cleanPhone, wilaya: cleanWilaya, baladiya: cleanBaladiya, address: cleanAddress, deliveryType: deliveryType || 'توصيل للمنزل', total: draftTotal, lineItems: [], _test: !!testMode, _nameTest: isNameTest });
    }
  } catch (err) {
    console.error('Network error creating draft:', err);
    return idemResolve(500, { error: 'Network error' });
  }

  try {
    const completeRes = await fetchWithTimeout(
      `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}/complete.json?payment_pending=true`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN }
      },
      15_000
    );
    if (!completeRes.ok) {
      console.error('Draft completion failed — HTTP', completeRes.status, 'draftId:', draftId);
      return idemResolve(502, { error: 'Order created but could not be completed' });
    }

    const completeData = await completeRes.json();
    const order        = completeData.draft_order;
    log('Order completed:', order.order_id);

    // ── Respond to the customer IMMEDIATELY ──────────────────────────────────
    // The order is now created AND completed in Shopify — the sale is done.
    // Everything past this point (conversion tracking, the Postgres mirror) is
    // non-essential to the sale and must NOT sit on the response's critical path:
    // on hostile mobile networks a slow Meta/TikTok CAPI call (or the extra
    // Shopify re-fetch) used to push the total response time past the client's
    // timeout, making a SUCCESSFUL order surface as the "call us on WhatsApp"
    // failure. The completed draft_order already carries the final total and line
    // items, so we never need a second round-trip to Shopify either.
    const qty           = totalQty;
    const totalFloat    = parseFloat(order.total_price || 0);
    const shippingFloat = parseFloat(shippingCost) || 0;
    const computedUnitPrice = String(((totalFloat - shippingFloat) / qty).toFixed(2));

    // Count this successful order against the phone cap before replying.
    if (!testMode) {
      const now   = Date.now();
      const entry = phoneOrderStore.get(cleanPhone);
      if (!entry || now - entry.windowStart > PHONE_WINDOW) {
        phoneOrderStore.set(cleanPhone, { count: 1, windowStart: now });
      } else {
        entry.count++;
      }
    }

    idemResolve(200, {
      success:      true,
      ref,
      orderId:      order.order_id,
      orderName:    order.name,
      name:         cleanName,
      phone:        cleanPhone,
      wilaya:       cleanWilaya,
      baladiya:     cleanBaladiya,
      address:      cleanAddress,
      deliveryType: deliveryType || 'توصيل للمنزل',
      total:        order.total_price || '0',
      lineItems:    (order.line_items || []).map(item => ({
        title:    item.title,
        variant:  item.variant_title || '',
        quantity: item.quantity,
        price:    item.price
      }))
    });

    // ── Background work (fire-and-forget) ────────────────────────────────────
    // Runs after the response is sent. Railway is one persistent process, so
    // detached promises here complete normally; each swallows/logs its own
    // errors and can never affect the customer's order outcome.

    // 1) Conversion tracking — Meta CAPI + TikTok + GA4
    trackPurchase({
      ref,
      total:           order.total_price || '0',
      unitPrice:       computedUnitPrice,
      variantId:       variantIdInt,
      quantity:        qty,
      phone:           cleanPhone,
      name:            cleanName,
      city:            cleanBaladiya,
      state:           cleanWilaya,
      eventId:         eventId || ref,
      fbp, fbc, gaClientId, sessionId, externalId, ttp, ttclid, gclid, sourceUrl,
      productTitle, contentCategory, brand, description,
      skipGA4:    testMode?.ga4Mode === 'skip',
      skipMeta:   testMode ? testMode.metaMode   === 'skip' : adPlatform !== 'meta',
      skipTikTok: testMode ? testMode.tiktokMode === 'skip' : adPlatform !== 'tiktok',
      metaTestCode:   testMode?.metaMode   === 'test' ? (testMode.metaTestCode   || null) : null,
      tiktokTestCode: testMode?.tiktokMode === 'test' ? (testMode.tiktokTestCode || null) : null,
      ip:        req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || '',
      userAgent: req.headers['user-agent'] || ''
    }).catch(err => console.error('[order] trackPurchase error:', err?.message));

    // 2) Mirror the order into Postgres for the custom orders admin (/admin/orders).
    {
      const orderItems = (order.line_items || lineItems).map(it => ({
        title:    it.title || it.name || 'Item',
        quantity: it.quantity || 1,
        priceDzd: Math.round(parseFloat(it.price) || 0),
      }));
      const totalDzd = Math.round(parseFloat(order.total_price || 0));
      const shipDzd  = Math.round(shippingFloat);
      insertOrder({
        ref,
        status:        'pending',
        name:          cleanName,
        phone:         cleanPhone,
        wilaya:        cleanWilaya,
        baladiya:      cleanBaladiya,
        address:       cleanAddress,
        deliveryType:  deliveryType || 'توصيل للمنزل',
        shippingCost:  shipDzd,
        items:         orderItems,
        merchTotalDzd: Math.max(totalDzd - shipDzd, 0),
        totalDzd,
        note:          cleanNote,
        source:        orderSource,
        origin:        productTitle || sourceUrl || '',
        shopifyOrderId: order.order_id,
      }).catch(err => console.error('[order] mirror insert error:', err?.message));

      // Link & convert the matching lead (funnel close) — fallback Shopify path.
      markLeadConverted({
        leadId, orderRef: ref, shopifyOrderId: order.order_id, phone: cleanPhone,
        name: cleanName, wilaya: cleanWilaya, baladiya: cleanBaladiya,
        deliveryType: deliveryType || 'توصيل للمنزل',
        merchTotalDzd: Math.max(totalDzd - shipDzd, 0), shippingCost: shipDzd, totalDzd,
        source: orderSource, origin: productTitle || sourceUrl || '', originUrl: sourceUrl || '',
      }).catch(err => console.error('[order] lead convert error:', err?.message));
      markSessionConverted({ sessionId: funnelSessionId, leadId, phone: cleanPhone })
        .catch(err => console.error('[order] session convert error:', err?.message));
    }

    return;

  } catch (err) {
    console.error('Network error completing draft:', err);
    return idemResolve(500, { error: 'Network error completing order' });
  }
}
