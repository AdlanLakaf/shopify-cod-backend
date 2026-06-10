// ============================================================
//  Client-side error beacon
//  POST /api/log-error
//  1. type 'page_data_diag' — page-data load diagnostics (no PII)
//  2. type 'failed_order'   — order submit failed after retries;
//     creates a DRAFT order tagged FAILED-ORDER-RECOVERY so staff
//     can see it in Shopify admin and call the customer back.
// ============================================================

import { runSecurityChecks, fetchWithTimeout } from './_security.js';

// Per-phone guard so a stuck client can't flood admin with recovery drafts
const recoveryStore = new Map(); // phone → last draft ts
const RECOVERY_COOLDOWN = 30 * 60 * 1000;
setInterval(() => {
  const cutoff = Date.now() - RECOVERY_COOLDOWN;
  for (const [k, ts] of recoveryStore) {
    if (ts < cutoff) recoveryStore.delete(k);
  }
}, 10 * 60 * 1000).unref();

async function createRecoveryDraft(body) {
  const SHOP    = process.env.SHOPIFY_MYSHOPIFY_DOMAIN || process.env.SHOPIFY_STORE_DOMAIN;
  const TOKEN   = process.env.SHOPIFY_ADMIN_TOKEN;
  const API_VER = '2024-10';
  if (!SHOP || !TOKEN) return;

  const sanitize = s => String(s || '').replace(/<[^>]*>/g, '').trim().slice(0, 200);
  const phone = sanitize(body.phone).replace(/\s/g, '');
  if (!/^(05|06|07)\d{8}$/.test(phone)) return; // only real-looking submissions

  const last = recoveryStore.get(phone);
  if (last && Date.now() - last < RECOVERY_COOLDOWN) return;
  recoveryStore.set(phone, Date.now());

  const name      = sanitize(body.name) || 'Unknown';
  const wilaya    = sanitize(body.wilaya);
  const baladiya  = sanitize(body.baladiya);
  const variantId = parseInt(body.variantId);
  const qty       = Math.min(Math.max(parseInt(body.quantity) || 1, 1), 10);
  const reason    = sanitize(body.reason).slice(0, 120);

  const note = [
    '⚠️ طلب فشل إرساله من الموقع — يرجى الاتصال بالزبون لتأكيد الطلب',
    `الاسم: ${name}`,
    `الهاتف: ${phone}`,
    wilaya   ? `الولاية: ${wilaya}`   : '',
    baladiya ? `البلدية: ${baladiya}` : '',
    reason   ? `سبب الفشل: ${reason}` : ''
  ].filter(Boolean).join('\n');

  const draftPayload = {
    draft_order: {
      line_items: (!isNaN(variantId) && variantId > 0)
        ? [{ variant_id: variantId, quantity: qty }]
        : [{ title: 'Failed order — call customer / طلب فاشل', price: '0.00', quantity: 1 }],
      note,
      tags: `FAILED-ORDER-RECOVERY, CALL-BACK, ${wilaya}`,
      shipping_address: {
        first_name: name, phone,
        address1: `${baladiya}، ${wilaya}`, city: baladiya || wilaya,
        province: wilaya, country: 'DZ', zip: ''
      },
      send_receipt: false,
      use_customer_default_address: false
    }
  };

  try {
    const r = await fetchWithTimeout(
      `https://${SHOP}/admin/api/${API_VER}/draft_orders.json`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
        body: JSON.stringify(draftPayload)
      },
      10_000
    );
    if (!r.ok) {
      console.error('[failed-order] recovery draft failed — HTTP', r.status, 'phone:', phone);
    } else {
      const d = await r.json();
      console.error('[failed-order] recovery draft created:', d.draft_order?.id, 'phone:', phone, 'reason:', reason);
    }
  } catch (err) {
    console.error('[failed-order] recovery draft error:', err.message, 'phone:', phone);
  }
}

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, { skipHmac: true, anyOrigin: true, rateBucket: 'beacon', rateMax: 600 });
  if (blocked) return;

  if (req.method !== 'POST') return res.status(405).end();

  const body = req.body || {};

  // ── Failed order → recovery draft in Shopify admin ──
  if (body.type === 'failed_order') {
    await createRecoveryDraft(body);
    return res.status(200).json({ ok: true });
  }

  const level = body.level === 'warn' ? 'warn' : 'error';

  console[level]('[client-diag]', JSON.stringify({
    type:    body.type    || 'unknown',
    level,
    s1:      body.s1      || null,
    s2:      body.s2      || null,
    s3:      body.s3      || null,
    s4:      body.s4      || null,
    lsAvail: body.lsAvail ?? null,
    online:  body.online  ?? null,
    conn:    body.conn    || null,
    ua:      String(body.ua || '').slice(0, 150)
  }));

  return res.status(200).json({ ok: true });
}
