// ============================================================
//  Client-side error beacon
//  POST /api/log-error
//  Receives page-data load diagnostics from the storefront.
//  Fire-and-forget from browser via sendBeacon — no PII stored.
// ============================================================

import { runSecurityChecks } from './_security.js';

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, { skipHmac: true, anyOrigin: true });
  if (blocked) return;

  if (req.method !== 'POST') return res.status(405).end();

  const body = req.body || {};
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
