// ============================================================
//  Shopify COD — Vercel Serverless Function
//  POST /api/create-order
//  Creates a Draft Order then immediately completes it
//  so it appears in Shopify admin as a real order (payment pending).
// ============================================================

export default async function handler(req, res) {
  // ── CORS headers (allows your Shopify storefront to call this) ──
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST')   return res.status(405).json({ error: 'Method not allowed' });

  // ── Pull credentials from Vercel environment variables ──
  const SHOP   = process.env.SHOPIFY_STORE_DOMAIN;   // e.g. your-store.myshopify.com
  const TOKEN  = process.env.SHOPIFY_ADMIN_TOKEN;    // Admin API access token
  const API_VER = '2024-01';

  if (!SHOP || !TOKEN) {
    return res.status(500).json({ error: 'Missing Shopify credentials in environment' });
  }

  // ── Parse request body ──
  const {
    variantId,
    quantity = 1,
    name,
    phone,
    wilaya,
    baladiya,
    address,
    deliveryType,
    note: extraNote = ''
  } = req.body;

  // Basic server-side validation
  if (!variantId || !name || !phone || !wilaya || !baladiya || !address) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const algerianPhoneRegex = /^(05|06|07)\d{8}$/;
  if (!algerianPhoneRegex.test(phone.replace(/\s/g, ''))) {
    return res.status(400).json({ error: 'Invalid Algerian phone number' });
  }

  // ── Build the note that appears on the order ──
  const orderNote = [
    `الاسم: ${name}`,
    `الهاتف: ${phone}`,
    `الولاية: ${wilaya}`,
    `البلدية: ${baladiya}`,
    `العنوان: ${address}`,
    `نوع التوصيل: ${deliveryType || 'توصيل للمنزل'}`,
    `طريقة الدفع: الدفع عند الاستلام (COD)`,
    extraNote ? `ملاحظة: ${extraNote}` : ''
  ].filter(Boolean).join(' | ');

  // ── Step 1: Create a Draft Order ──
  const draftPayload = {
    draft_order: {
      line_items: [
        {
          variant_id: parseInt(variantId),
          quantity:   parseInt(quantity)
        }
      ],
      shipping_address: {
        first_name: name,
        phone:      phone,
        address1:   address,
        city:       baladiya,
        province:   wilaya,
        country:    'DZ',
        zip:        ''
      },
      billing_address: {
        first_name: name,
        phone:      phone,
        address1:   address,
        city:       baladiya,
        province:   wilaya,
        country:    'DZ',
        zip:        ''
      },
      note:        orderNote,
      tags:        'COD, جزائر, الدفع-عند-الاستلام',
      // Send email/phone notifications off — you handle it manually
      send_receipt:          false,
      send_fulfillment_receipt: false,
      use_customer_default_address: false
    }
  };

  let draftId;

  try {
    const draftRes = await fetch(
      `https://${SHOP}/admin/api/${API_VER}/draft_orders.json`,
      {
        method:  'POST',
        headers: {
          'Content-Type':            'application/json',
          'X-Shopify-Access-Token':  TOKEN
        },
        body: JSON.stringify(draftPayload)
      }
    );

    if (!draftRes.ok) {
      const err = await draftRes.json();
      console.error('Draft order creation failed:', err);
      return res.status(502).json({ error: 'Failed to create draft order', details: err });
    }

    const draftData = await draftRes.json();
    draftId = draftData.draft_order.id;
    console.log('Draft order created:', draftId);

  } catch (err) {
    console.error('Network error creating draft:', err);
    return res.status(500).json({ error: 'Network error' });
  }

  // ── Step 2: Complete the Draft Order ──
  // payment_pending=true  →  order status becomes "Payment pending (COD)"
  // This converts it from a draft into a real order in your Shopify admin.
  try {
    const completeRes = await fetch(
      `https://${SHOP}/admin/api/${API_VER}/draft_orders/${draftId}/complete.json?payment_pending=true`,
      {
        method:  'PUT',
        headers: {
          'Content-Type':           'application/json',
          'X-Shopify-Access-Token': TOKEN
        }
      }
    );

    if (!completeRes.ok) {
      const err = await completeRes.json();
      console.error('Draft order completion failed:', err);
      return res.status(502).json({ error: 'Order created but could not be completed', draftId });
    }

    const completeData = await completeRes.json();
    const order = completeData.draft_order;

    console.log('Order completed:', order.order_id);

    // ── Success ──
    return res.status(200).json({
      success:  true,
      orderId:  order.order_id,
      orderName: order.name    // e.g. #1001
    });

  } catch (err) {
    console.error('Network error completing draft:', err);
    return res.status(500).json({ error: 'Network error completing order' });
  }
}
