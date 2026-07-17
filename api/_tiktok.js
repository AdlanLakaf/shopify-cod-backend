// ============================================================
//  TikTok Lead Generation — API client + lead → order pipeline
//
//  One pipeline, two entrances:
//   • realtime: /api/tiktok/lead webhook (TikTok retries 72h
//     until it gets our 200, so the webhook alone is already
//     loss-resistant)
//   • backup:   pollTikTokLeads() cron — bulk-pulls recent
//     leads per form, catching anything a webhook outage missed
//  Both call processTikTokLead(), which claims the lead in the
//  tiktok_leads ledger first — a lead id is processed exactly
//  once no matter how many times or ways it arrives.
//
//  Pipeline: claim → extract name/phone/answers → resolve
//  product mapping (ad → adgroup → form → campaign → default)
//  → upsert CRM lead (source 'tiktok_form') → insert order in
//  Postgres (realtime in /admin/orders via pg_notify) → push
//  Shopify order in background (same pushOrderToShopify as the
//  website) → mark lead converted.
//
//  Env: TIKTOK_MARKETING_TOKEN, TIKTOK_ADVERTISER_ID (enrichment +
//  polling), TIKTOK_WEBHOOK_TOKEN (shared-secret on the
//  callback URL), TIKTOK_PAGE_IDS (comma list, enables polling).
//
//  Deliberately NO ad-platform conversion events here: lead-gen
//  campaigns already count the in-app form submit as their
//  conversion; firing Purchase without click context would
//  double-count and pollute delivery optimisation.
// ============================================================

import { fetchWithTimeout, log } from './_security.js';
import { claimLead, markLeadOutcome, resolveMapping } from './_tiktok-db.js';
import { insertOrder, updateOrderShopify, findRecentOrderByPhone } from './_orders-db.js';
import { upsertLead, markLeadConverted } from './_leads-db.js';
import { pushOrderToShopify } from './create-order.js';

const TT_BASE = 'https://business-api.tiktok.com/open_api/v1.3';

// ── Low-level TikTok Business API call ───────────────────────────────────────
async function ttGet(path, params = {}, timeoutMs = 10_000) {
  const token = process.env.TIKTOK_MARKETING_TOKEN;
  if (!token) return null;
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') qs.set(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
  }
  const url = `${TT_BASE}${path}?${qs.toString()}`;
  try {
    const res = await fetchWithTimeout(url, { headers: { 'Access-Token': token } }, timeoutMs);
    const json = await res.json().catch(() => null);
    if (!json || json.code !== 0) {
      log(`[tiktok] GET ${path} → code ${json?.code} ${json?.message || ''}`);
      return null;
    }
    return json.data ?? null;
  } catch (err) {
    console.warn(`[tiktok] GET ${path} failed:`, err.message);
    return null;
  }
}

async function ttPost(path, body = {}, timeoutMs = 10_000) {
  const token = process.env.TIKTOK_MARKETING_TOKEN;
  if (!token) return null;
  try {
    const res = await fetchWithTimeout(`${TT_BASE}${path}`, {
      method: 'POST',
      headers: { 'Access-Token': token, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }, timeoutMs);
    const json = await res.json().catch(() => null);
    if (!json || json.code !== 0) {
      log(`[tiktok] POST ${path} → code ${json?.code} ${json?.message || ''}`);
      return null;
    }
    return json.data ?? null;
  } catch (err) {
    console.warn(`[tiktok] POST ${path} failed:`, err.message);
    return null;
  }
}

// ── Campaign / adgroup / ad name enrichment (24h in-memory cache) ────────────
const nameCache = new Map(); // `${kind}:${id}` → { name, ts }
const NAME_TTL = 24 * 60 * 60 * 1000;

async function entityName(kind, id) {
  if (!id) return '';
  const key = `${kind}:${id}`;
  const hit = nameCache.get(key);
  if (hit && Date.now() - hit.ts < NAME_TTL) return hit.name;
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  if (!advertiserId) return '';
  const cfg = {
    campaign: { path: '/campaign/get/', filter: 'campaign_ids', list: 'campaign_name' },
    adgroup:  { path: '/adgroup/get/',  filter: 'adgroup_ids',  list: 'adgroup_name' },
    ad:       { path: '/ad/get/',       filter: 'ad_ids',       list: 'ad_name' },
  }[kind];
  if (!cfg) return '';
  const data = await ttGet(cfg.path, {
    advertiser_id: advertiserId,
    filtering: { [cfg.filter]: [String(id)] },
    page_size: 1,
  });
  const name = data?.list?.[0]?.[cfg.list] || '';
  nameCache.set(key, { name, ts: Date.now() });
  return name;
}

// ── Field extraction ─────────────────────────────────────────────────────────

/** +213 / 00213 / E.164 / spaced → local 0X XX XX XX XX, or '' if not a DZ mobile. */
export function normalizeDzPhone(raw) {
  let v = String(raw ?? '').replace(/[\s\-().]/g, '');
  if (v.startsWith('+213')) v = '0' + v.slice(4);
  else if (v.startsWith('00213')) v = '0' + v.slice(5);
  else if (/^213\d{9}$/.test(v)) v = '0' + v.slice(3);
  return /^(05|06|07)\d{8}$/.test(v) ? v : '';
}

// Wilaya answer matcher — accepts "23", "Annaba", "عنابة - 23"…
const WILAYA_KEYS = /wilaya|province|state|ولاية|الولاية/i;
const NAME_KEYS   = /full.?name|first.?name|^name$|الاسم|اسم/i;
const PHONE_KEYS  = /phone|mobile|tel|هاتف|الهاتف|رقم/i;

/**
 * Pull { name, phone, wilaya, answers[] } out of ANY TikTok lead shape.
 * TikTok payloads vary (webhook vs detail API vs bulk download): answers may
 * live under `answers`, `field_data`, `questions`, or as flat keys. We scan
 * tolerantly rather than trusting one schema.
 */
export function extractLeadFields(obj) {
  const answers = [];   // { q, a }
  const seen = new Set();

  const push = (q, a) => {
    const question = String(q ?? '').trim().slice(0, 120);
    const answer   = String(a ?? '').trim().slice(0, 300);
    if (!question || !answer) return;
    const k = question + '|' + answer;
    if (seen.has(k)) return;
    seen.add(k);
    answers.push({ q: question, a: answer });
  };

  (function walk(node, depth) {
    if (!node || depth > 6) return;
    if (Array.isArray(node)) { for (const it of node) walk(it, depth + 1); return; }
    if (typeof node !== 'object') return;
    // Common Q/A pair shapes
    const q = node.question ?? node.field_name ?? node.name ?? node.title ?? node.key;
    const a = node.answer ?? node.value ?? node.values?.join?.(', ') ?? node.answers?.join?.(', ');
    if (q !== undefined && a !== undefined && (typeof a === 'string' || typeof a === 'number')) push(q, a);
    // Flat well-known keys
    for (const [k, v] of Object.entries(node)) {
      if ((typeof v === 'string' || typeof v === 'number') &&
          /^(user_)?(name|full_name|first_name|phone|phone_number|email|wilaya|city|province)$/i.test(k)) {
        push(k, v);
      }
      if (typeof v === 'object') walk(v, depth + 1);
    }
  })(obj, 0);

  let name = '', phone = '', wilaya = '';
  for (const { q, a } of answers) {
    if (!phone) { const ph = normalizeDzPhone(a); if (ph && PHONE_KEYS.test(q)) phone = ph; }
    if (!name && NAME_KEYS.test(q) && !normalizeDzPhone(a)) name = a.slice(0, 120);
    if (!wilaya && WILAYA_KEYS.test(q)) wilaya = a.slice(0, 120);
  }
  // Last resort: any answer that IS a valid DZ phone
  if (!phone) for (const { a } of answers) { const ph = normalizeDzPhone(a); if (ph) { phone = ph; break; } }
  return { name, phone, wilaya, answers };
}

// ── Lead detail fetch (when the webhook payload has no answers) ──────────────
// Endpoint naming differs across TikTok doc versions — try candidates in
// order; the first that answers wins. Verified live via the smoke script.
async function fetchLeadDetail(leadId) {
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  const candidates = [
    ['/page/lead/get/',  { advertiser_id: advertiserId, lead_ids: [String(leadId)] }],
    ['/pages/leads/get/', { advertiser_id: advertiserId, lead_ids: [String(leadId)] }],
    ['/lead/get/',        { advertiser_id: advertiserId, lead_ids: [String(leadId)] }],
  ];
  for (const [path, params] of candidates) {
    const data = await ttGet(path, params);
    if (data) return data;
  }
  console.warn(`[tiktok] no lead-detail endpoint answered for lead ${leadId} — check API scope / endpoint (run smoke test)`);
  return null;
}

// ── Main pipeline ────────────────────────────────────────────────────────────

/**
 * Process one TikTok lead end-to-end. Idempotent on tiktokLeadId (ledger
 * claim). Never throws — every failure is stamped on the ledger + logged.
 */
export async function processTikTokLead({ tiktokLeadId, formId = '', pageId = '', campaignId = '', adgroupId = '', adId = '', raw = null, via = 'webhook' }) {
  const owned = await claimLead({ tiktokLeadId, via, formId, pageId, campaignId, adgroupId, adId, raw });
  if (!owned) { log(`[tiktok] lead ${tiktokLeadId} already claimed — skip (${via})`); return; }

  try {
    // 1) Extract customer fields — from the webhook payload first, then the API.
    let fields = extractLeadFields(raw || {});
    if (!fields.phone) {
      const detail = await fetchLeadDetail(tiktokLeadId);
      if (detail) {
        const f2 = extractLeadFields(detail);
        fields = {
          name:   fields.name   || f2.name,
          phone:  fields.phone  || f2.phone,
          wilaya: fields.wilaya || f2.wilaya,
          answers: [...fields.answers, ...f2.answers],
        };
      }
    }
    if (!fields.phone) {
      await markLeadOutcome(tiktokLeadId, { status: 'invalid_phone', failReason: 'no valid DZ phone in answers' });
      console.warn(`[tiktok] lead ${tiktokLeadId}: no valid DZ phone — not imported`);
      return;
    }
    const name = fields.name || 'TikTok Lead';

    // 2) Attribution names (best-effort, cached).
    const [campaignName, adgroupName, adName] = await Promise.all([
      entityName('campaign', campaignId), entityName('adgroup', adgroupId), entityName('ad', adId),
    ]);
    const originBits = [campaignName || campaignId, adName || adId].filter(Boolean);
    const origin = ('tt-form:' + originBits.join(' / ')).slice(0, 200);

    // 3) CRM lead — same table the website funnel uses, so /admin sees it live.
    const crmLeadId = `tt-${String(tiktokLeadId).slice(0, 70)}`;
    await upsertLead({
      leadId: crmLeadId, stage: 'enriched',
      name, phone: fields.phone, wilaya: fields.wilaya,
      source: 'tiktok_form', origin,
      originUrl: `tiktok://form/${formId || pageId || ''}`,
      entryUrl: [
        campaignId && `campaign:${campaignId}${campaignName ? ` (${campaignName})` : ''}`,
        adgroupId  && `adgroup:${adgroupId}${adgroupName ? ` (${adgroupName})` : ''}`,
        adId       && `ad:${adId}${adName ? ` (${adName})` : ''}`,
      ].filter(Boolean).join(' | '),
    });

    // 4) Same-customer guard — double form submits get a new tiktok_lead_id, so
    //    the ledger can't catch them; a recent tiktok_form order for the same
    //    phone means "already imported" → link, don't duplicate.
    const existing = await findRecentOrderByPhone(fields.phone, { source: 'tiktok_form', hours: 24 });
    if (existing) {
      await markLeadOutcome(tiktokLeadId, { status: 'duplicate_phone', orderRef: existing.ref, failReason: 'recent tiktok_form order for this phone' });
      log(`[tiktok] lead ${tiktokLeadId}: phone already ordered (${existing.ref}) — linked, no new order`);
      return;
    }

    // 5) Resolve WHAT they're buying.
    const map = await resolveMapping({ adId, adgroupId, formId: formId || pageId, campaignId });
    if (!map || (!map.variant_id && !(map.title && map.price_dzd > 0))) {
      await markLeadOutcome(tiktokLeadId, { status: 'no_mapping', failReason: `no product mapping (ad ${adId || '-'} / form ${formId || pageId || '-'} / campaign ${campaignId || '-'})` });
      console.warn(`[tiktok] lead ${tiktokLeadId}: no product mapping — CRM lead saved, order NOT created. Configure /api/admin/tiktok-map`);
      return;
    }

    // 6) Create the order — Postgres first (instant in /admin/orders), Shopify
    //    in background, exactly like the website fast path.
    const ref = 'H&N-TTL-' + Date.now().toString(36).toUpperCase().slice(-6);
    const qty = map.quantity || 1;
    const priceDzd = Math.max(0, map.price_dzd || 0);
    const merchDzd = priceDzd * qty;
    const itemTitle = map.title || `TikTok offer (variant ${map.variant_id})`;
    const answersTxt = fields.answers.map(x => `${x.q}: ${x.a}`).join(' | ').slice(0, 600);
    const note = [
      'طلب من نموذج تيك توك — يجب الاتصال للتأكيد',
      campaignName && `الحملة: ${campaignName}`,
      adName && `الإعلان: ${adName}`,
      answersTxt && `إجابات النموذج: ${answersTxt}`,
    ].filter(Boolean).join(' | ');

    const saved = await insertOrder({
      ref, status: 'pending',
      name, phone: fields.phone,
      wilaya: fields.wilaya, baladiya: '', address: '',
      deliveryType: '',                       // unknown until the confirmation call
      shippingCost: 0,
      items: [{ title: itemTitle, quantity: qty, priceDzd }],
      merchTotalDzd: merchDzd, totalDzd: merchDzd,
      note, source: 'tiktok_form', origin,
    });
    if (!saved) {
      await markLeadOutcome(tiktokLeadId, { status: 'db_failed', failReason: 'orders DB insert failed' });
      console.error(`[tiktok] lead ${tiktokLeadId}: orders DB insert failed`);
      return;
    }
    await markLeadOutcome(tiktokLeadId, { status: 'ordered', orderRef: ref });
    markLeadConverted({
      leadId: crmLeadId, orderRef: ref, phone: fields.phone, name,
      wilaya: fields.wilaya, merchTotalDzd: merchDzd, shippingCost: 0, totalDzd: merchDzd,
      source: 'tiktok_form', origin,
    }).catch(err => console.error('[tiktok] lead convert error:', err?.message));

    // 7) Background Shopify push — never blocks, failure is flagged on the row.
    const SHOP    = process.env.SHOPIFY_MYSHOPIFY_DOMAIN || process.env.SHOPIFY_STORE_DOMAIN;
    const TOKEN   = process.env.SHOPIFY_ADMIN_TOKEN;
    if (SHOP && TOKEN) {
      const lineItems = map.variant_id
        ? [{ variant_id: Number(map.variant_id), quantity: qty }]
        : [{ title: itemTitle, price: priceDzd.toFixed(2), quantity: qty }];
      const draftPayload = {
        draft_order: {
          line_items: lineItems,
          shipping_address: { first_name: name, phone: fields.phone, address1: '', city: '', province: fields.wilaya || '', country: 'DZ', zip: '' },
          note: `REF: ${ref}\n${note.replace(/ \| /g, '\n')}`,
          tags: `COD, TIKTOK-FORM, CALL-TO-CONFIRM, REF-${ref}, src-tiktok_form`,
          send_receipt: false, send_fulfillment_receipt: false, use_customer_default_address: false,
        },
      };
      pushOrderToShopify({ SHOP, TOKEN, API_VER: '2024-10', draftPayload, lineItems, merchTotalDzd: merchDzd, ref })
        .then(order => { log(`[tiktok] Shopify order for ${ref}:`, order.order_id); return updateOrderShopify(ref, { shopifyOrderId: order.order_id }); })
        .catch(err => { console.error(`[tiktok] Shopify push failed (${ref}):`, err?.message); return updateOrderShopify(ref, { syncFailed: true }); });
    }
    console.log(`[tiktok] lead ${tiktokLeadId} → order ${ref} (${origin})`);
  } catch (err) {
    console.error(`[tiktok] processTikTokLead crashed (${tiktokLeadId}):`, err?.message);
    await markLeadOutcome(tiktokLeadId, { status: 'failed', failReason: String(err?.message || err).slice(0, 300) });
  }
}

// ── Polling backup — bulk-pull recent leads per form/page ────────────────────
// Runs from a cron when TIKTOK_PAGE_IDS is set. Uses the task-based bulk
// export (create task → poll status → download). Everything funnels through
// processTikTokLead, so overlap with webhooks is harmless.
export async function pollTikTokLeads() {
  const advertiserId = process.env.TIKTOK_ADVERTISER_ID;
  const pageIds = String(process.env.TIKTOK_PAGE_IDS || '').split(',').map(x => x.trim()).filter(Boolean);
  if (!process.env.TIKTOK_MARKETING_TOKEN || !advertiserId || !pageIds.length) return 0;

  let imported = 0;
  for (const pageId of pageIds) {
    try {
      const task = await ttPost('/page/lead/task/create/', { advertiser_id: advertiserId, page_id: pageId });
      const taskId = task?.task_id;
      if (!taskId) { log(`[tiktok-poll] no task for page ${pageId}`); continue; }

      let download = null;
      for (let i = 0; i < 10 && !download; i++) {
        await new Promise(r => setTimeout(r, 3000));
        const st = await ttGet('/page/lead/task/get/', { advertiser_id: advertiserId, task_id: taskId });
        const status = String(st?.status || '').toUpperCase();
        if (status === 'SUCCESS' || status === 'COMPLETED') {
          download = await ttGet('/page/lead/task/download/', { advertiser_id: advertiserId, task_id: taskId });
        } else if (status === 'FAILED') break;
      }
      const leads = download?.leads || download?.list || [];
      for (const ld of leads) {
        const id = ld.lead_id || ld.id;
        if (!id) continue;
        await processTikTokLead({
          tiktokLeadId: String(id),
          formId: String(ld.form_id || ld.page_id || pageId),
          pageId: String(pageId),
          campaignId: String(ld.campaign_id || ''),
          adgroupId:  String(ld.adgroup_id || ''),
          adId:       String(ld.ad_id || ''),
          raw: ld, via: 'poll',
        });
        imported++;
      }
      log(`[tiktok-poll] page ${pageId}: ${leads.length} lead(s) seen`);
    } catch (err) {
      console.error(`[tiktok-poll] page ${pageId} failed:`, err?.message);
    }
  }
  return imported;
}
