// ============================================================
//  Shopify COD — Vercel Serverless Function
//  POST /api/create-order
//  Creates a Draft Order then immediately completes it
//  Security: HMAC + timestamp + rate limiting + origin check
// ============================================================

import { runSecurityChecks, verifyTurnstile, fetchWithTimeout, log } from './_security.js';
import { trackPurchase } from './_tracking.js';
import { detectSource }  from './_attribution.js';
import { getTestMode }   from './_test-mode.js';

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, { skipHmac: true });
  if (blocked) return;
  // ── Staff test mode — skip Turnstile if valid staff token present ──
  const testMode = getTestMode(req.body || {});
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
    referrer        = '',
    trafficSource   = '',
    productTitle    = '',
    contentCategory = '',
    brand           = '',
    description     = ''
  } = req.body;

  if (!variantId || !name || !phone || !wilaya || !baladiya) {
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

  const orderSource = detectSource({
    trafficSource,
    fbc, ttclid, gclid,
    userAgent: req.headers['user-agent'] || '',
    referrer:  sanitize(referrer)
  });
  log(`[order] source detected: ${orderSource}`);

  const algerianPhoneRegex = /^(05|06|07)\d{8}$/;
  if (!algerianPhoneRegex.test(cleanPhone)) {
    return res.status(400).json({ error: 'Invalid Algerian phone number' });
  }

  const variantIdInt = parseInt(variantId);
  if (isNaN(variantIdInt) || variantIdInt <= 0) {
    return res.status(400).json({ error: 'Invalid variant ID' });
  }

  // ── Mock mode: skip Shopify entirely, return fake response ──
  if (testMode?.orderMode === 'mock') {
    const fakeRef = 'H&N-TEST-' + Date.now().toString(36).toUpperCase().slice(-6);
    log(`[test] mock order: ${fakeRef}`);
    return res.status(200).json({ success: true, ref: fakeRef, orderId: 0, orderName: fakeRef, name: cleanName, phone: cleanPhone, wilaya: cleanWilaya, baladiya: cleanBaladiya, address: cleanAddress, deliveryType: deliveryType || 'توصيل للمنزل', total: String(shippingCost || 0), lineItems: [], _test: true });
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

  const draftPayload = {
    draft_order: {
      line_items: [
        { variant_id: variantIdInt, quantity: Math.min(parseInt(quantity) || 1, 10) }
      ],
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
        price: (typeof shippingCost === 'number' && shippingCost >= 0) ? shippingCost.toFixed(2) : '0.00',
        code:  deliveryType === 'استلام من المكتب' ? 'office-pickup' : 'home-delivery'
      },
      tags: `COD, ${cleanWilaya}, ${deliveryType === 'استلام من المكتب' ? 'office-pickup' : 'home-delivery'}, REF-${ref}, src-${orderSource}${testMode ? ', TEST, DO-NOT-FULFILL' : ''}`,
      send_receipt: false,
      send_fulfillment_receipt: false,
      use_customer_default_address: false
    }
  };

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
      return res.status(502).json({ error: 'Failed to create draft order' });
    }
    const draftData = await draftRes.json();
    draftId = draftData.draft_order.id;
    log('Draft order created:', draftId);

    // ── Draft-only mode: stop here, don't complete (no stock deduction) ──
    if (testMode?.orderMode === 'draft') {
      log(`[test] draft-only order: ${draftId}`);
      return res.status(200).json({ success: true, ref, orderId: 0, orderName: ref, name: cleanName, phone: cleanPhone, wilaya: cleanWilaya, baladiya: cleanBaladiya, address: cleanAddress, deliveryType: deliveryType || 'توصيل للمنزل', total: String(shippingCost || 0), lineItems: [], _test: true });
    }
  } catch (err) {
    console.error('Network error creating draft:', err);
    return res.status(500).json({ error: 'Network error' });
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
      return res.status(502).json({ error: 'Order created but could not be completed' });
    }

    const completeData = await completeRes.json();
    const order        = completeData.draft_order;
    log('Order completed:', order.order_id);

   // ── Fetch full order to get line_items ──
let fullOrder = null;

try {
  const fullOrderRes = await fetchWithTimeout(
    `https://${SHOP}/admin/api/${API_VER}/orders/${order.order_id}.json`,
    { headers: { 'X-Shopify-Access-Token': TOKEN } },
    10_000
  );

  if (!fullOrderRes.ok) {
    console.error('Failed to fetch full order details:', order.order_id);
  } else {
    const fullOrderData = await fullOrderRes.json();
    fullOrder = fullOrderData.order;
  }
} catch (err) {
  console.error('Network error fetching full order:', err);
}

// ── Server-side conversion tracking — awaited so Vercel doesn't cut it off ──
await trackPurchase({
  ref,
  total:           fullOrder?.total_price || order.total_price || '0',
  unitPrice:       fullOrder?.line_items?.[0]?.price || '0',
  variantId:       variantIdInt,
  quantity:        Math.min(parseInt(quantity) || 1, 10),
  phone:           cleanPhone,
  name:            cleanName,
  city:            cleanBaladiya,
  state:           cleanWilaya,
  eventId:         eventId || ref,
  fbp,
  fbc,
  gaClientId,
  sessionId,
  externalId,
  ttp,
  ttclid,
  gclid,
  sourceUrl,
  productTitle,
  contentCategory,
  brand,
  description,
  ip:              req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || '',
  userAgent:       req.headers['user-agent'] || ''
}).catch(err => console.error('[order] trackPurchase error:', err.message));

return res.status(200).json({
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
  total:        fullOrder?.total_price || order.total_price || '0',
  lineItems:    (fullOrder?.line_items || []).map(item => ({
    title:    item.title,
    variant:  item.variant_title || '',
    quantity: item.quantity,
    price:    item.price
  }))
});

  } catch (err) {
    console.error('Network error completing draft:', err);
    return res.status(500).json({ error: 'Network error completing order' });
  }
}
