// ============================================================
//  POST /api/tiktok/lead — TikTok Lead Generation webhook
//
//  Server-to-server (no browser, no CORS). Auth is a shared
//  secret in the registered callback URL: register
//    https://<railway>/api/tiktok/lead?t=<TIKTOK_WEBHOOK_TOKEN>
//  in the TikTok developer portal. Requests without the token
//  are dropped (403) once the env var is set; before it's set
//  we accept-and-log so the initial URL verification can pass.
//
//  Contract with TikTok: answer 200 FAST (they retry with
//  backoff for 72h otherwise). So we ACK immediately after
//  parsing and run the pipeline detached — the ledger claim in
//  processTikTokLead makes redelivery harmless either way.
//
//  GET handles TikTok's URL-verification challenge (echo).
// ============================================================

import { processTikTokLead } from './_tiktok.js';

// Pull ids out of the many envelope shapes TikTok uses
// ({ content: {...} }, { data: {...} }, flat, or an array of events).
function unwrapEvents(body) {
  if (!body || typeof body !== 'object') return [];
  if (Array.isArray(body)) return body.flatMap(unwrapEvents);
  if (Array.isArray(body.list))   return body.list.flatMap(unwrapEvents);
  if (Array.isArray(body.events)) return body.events.flatMap(unwrapEvents);
  let content = body.content ?? body.data ?? body;
  if (typeof content === 'string') { try { content = JSON.parse(content); } catch { content = {}; } }
  return [{ envelope: body, content }];
}

export default async function handler(req, res) {
  // URL-verification challenge (GET with echo/challenge param).
  if (req.method === 'GET') {
    const challenge = req.query?.challenge || req.query?.echostr || '';
    if (challenge) return res.status(200).send(String(challenge));
    return res.status(200).json({ ok: true });
  }
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const secret = process.env.TIKTOK_WEBHOOK_TOKEN;
  if (secret && req.query?.t !== secret) {
    console.warn('[tiktok-webhook] bad/missing token — dropped');
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (!secret) console.warn('[tiktok-webhook] TIKTOK_WEBHOOK_TOKEN not set — accepting unauthenticated (set it!)');

  const events = unwrapEvents(req.body);
  const jobs = [];
  for (const { content } of events) {
    const c = content || {};
    const leadId = c.lead_id || c.leadId || c.id;
    if (!leadId) continue;
    jobs.push({
      tiktokLeadId: String(leadId),
      formId:     String(c.form_id || c.page_id || ''),
      pageId:     String(c.page_id || ''),
      campaignId: String(c.campaign_id || ''),
      adgroupId:  String(c.adgroup_id || ''),
      adId:       String(c.ad_id || ''),
      raw: req.body, via: 'webhook',
    });
  }

  // ACK first — processing must never make TikTok think delivery failed.
  res.status(200).json({ ok: true, received: jobs.length });

  if (!jobs.length) {
    console.warn('[tiktok-webhook] payload had no lead_id — raw:', JSON.stringify(req.body).slice(0, 800));
    return;
  }
  for (const job of jobs) {
    processTikTokLead(job).catch(err => console.error('[tiktok-webhook] pipeline error:', err?.message));
  }
}
