// ============================================================
//  Shopify COD — Vercel Serverless Function
//  POST /api/create-order
//  Creates a Draft Order then immediately completes it
//  Security: HMAC + timestamp + rate limiting + origin check
// ============================================================

import { runSecurityChecks } from './_security.js';

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res);
  if (blocked) return;

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const SHOP    = process.env.SHOPIFY_STORE_DOMAIN;
  const TOKEN   = process.env.SHOPIFY_ADMIN_TOKEN;
  const API_VER = '2024-01';

  if (!SHOP || !TOKEN) {
    return res.status(500).json({ error: 'Missing Shopify credentials in environment' });
  }

  const {
    variantId,
    quantity    = 1,
    name,
    phone,
    wilaya,
    baladiya,
    address,
    deliveryType,
    note: extraNote = ''
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
  const cleanNote     = sanitize(extraNote);

  const algerianPhoneRegex = /^(05|06|07)\d{8}$/;
  if (!algerianPhoneRegex.test(cleanPhone)) {
    return res.status(400).json({ error: 'Invalid Algerian phone number' });
  }

  const variantIdInt = parseInt(variantId);
  if (isNaN(variantIdInt) || variantIdInt <= 0) {
    return res.status(400).json({ error: 'Invalid variant ID' });
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
      // ── CHANGE 2: cleaner tags — wilaya for filtering, delivery type, ref ──
      tags: `COD, ${cleanWilaya}, ${deliveryType === 'استلام من المكتب' ? 'office-pickup' : 'home-delivery'}, REF-${ref}`,
      send_receipt: false,
      send_fulfillment_receipt: false,
      use_customer_default_address: false
    }
  };

  let draftId;

  try {
    const draftRes = await fetch(
      `https://${SHOP}/admin/api/${API_VER}/draft_orders.json`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
        body: JSON.stringify(draftPayload)
      }
    );
    if (!draftRes.ok) {
      const err = await draftRes.json();
      console.error('Draft order creation failed:', err);
      return res.status(502).json({ error: 'Failed to create draft order' });
    }
    const draftData = await draftRes.json();
    draftId = draftData.draft_order.id;
    console.log('Draft order created:', draftId);
  } catch (err) {
    console.error('Network error creating draft:', err);
    return res.status(500).json({ error: 'Network error' });
  }

  try {
    const completeRes = await fetch(
      `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}/complete.json?payment_pending=true`,
      {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN }
      }
    );
    if (!completeRes.ok) {
      const err = await completeRes.json();
      console.error('Completion failed:', err);
      return res.status(502).json({ error: 'Order created but could not be completed', draftId });
    }

    const completeData = await completeRes.json();
    const order        = completeData.draft_order;
    console.log('Order completed:', order.order_id);

    // ── CHANGE 3: fetch full order to get line_items with titles and prices ──
    const fullOrderRes  = await fetch(
      `https://${SHOP}/admin/api/${API_VER}/orders/${order.order_id}.json`,
      { headers: { 'X-Shopify-Access-Token': TOKEN } }
    );
    const fullOrderData = await fullOrderRes.json();
    const fullOrder     = fullOrderData.order;

    // ── CHANGE 4: return ref + line items alongside customer data ──
    return res.status(200).json({
      success:      true,
      ref,                          // H&N-ORD-K3F9QX — shown to customer
      orderId:      order.order_id, // real Shopify ID — kept for internal use
      orderName:    order.name,     // real Shopify name e.g. #D27 — kept for internal use
      name:         cleanName,
      phone:        cleanPhone,
      wilaya:       cleanWilaya,
      baladiya:     cleanBaladiya,
      address:      cleanAddress,
      deliveryType: deliveryType || 'توصيل للمنزل',
      total:        fullOrder?.total_price        || order.total_price || '0',
      // ── CHANGE 5: map line items for the thank you page ──
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
