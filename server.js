// ============================================================
//  Express HTTP server — for Railway (production)
//  Vercel continues to work via vercel.json (testing only)
// ============================================================

import express from 'express';
import cron    from 'node-cron';

import createOrderHandler      from './api/create-order.js';
import getPageDataHandler      from './api/get-page-data.js';
import getDeliveryRatesHandler from './api/get-delivery-rates.js';
import getStopdesksHandler     from './api/get-stopdesks.js';
import trackEventHandler       from './api/track-event.js';
import countHubsHandler        from './api/count-hubs.js';
import logErrorHandler         from './api/log-error.js';
import leadHandler             from './api/lead.js';
import funnelHandler           from './api/funnel.js';
import { syncPageData }        from './api/sync-page-data.js';
import { setCorsHeaders }      from './api/_security.js';
import { pruneLeads }          from './api/_leads-db.js';
import { pruneSessions }       from './api/_funnel-db.js';
import { rollupClosedBuckets, pruneRollups, rollupHours } from './api/_funnel-rollup-db.js';

const app  = express();
const PORT = process.env.PORT || 3000;

app.set('trust proxy', 1);
// Parse JSON bodies. We ALSO parse `text/plain` as JSON: cross-origin
// `navigator.sendBeacon` may only use CORS-safelisted content-types, so the
// lead / funnel / log-error beacons send their JSON as text/plain to avoid a
// preflight that sendBeacon cannot perform. type-is matches the Blob's
// `text/plain;charset=UTF-8` against 'text/plain'.
// 16kb matches MAX_BODY_BYTES in api/_security.js — a real ad-click order
// (fbclid/ttclid-laden URLs + bundle display items + Arabic text) runs 4–6 KB,
// and the old 5kb limit here 413'd those orders before any handler ran.
app.use(express.json({ limit: '16kb', type: ['application/json', 'text/plain'] }));

// ── Root status page ──────────────────────────────────────────────────────────
app.get('/', (_req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>HandsNose COD Backend</title>
      <style>
        body { font-family: system-ui, sans-serif; max-width: 480px; margin: 80px auto; padding: 0 24px; color: #111; }
        h1   { font-size: 1.4rem; margin-bottom: 4px; }
        p    { color: #555; margin: 4px 0; }
        code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; font-size: .9em; }
        .ok  { color: #16a34a; font-weight: 600; }
      </style>
    </head>
    <body>
      <h1>HandsNose COD Backend</h1>
      <p class="ok">&#x2713; Server is running</p>
      <p>Endpoints:</p>
      <ul>
        <li><code>POST /api/create-order</code></li>
        <li><code>GET  /api/get-page-data</code></li>
        <li><code>GET  /api/get-delivery-rates</code></li>
        <li><code>GET  /api/get-stopdesks</code></li>
        <li><code>POST /api/track-event</code></li>
        <li><code>POST /api/admin/sync-page-data</code> (manual trigger)</li>
        <li><code>GET  /api/count-hubs</code> (diagnostic)</li>
      </ul>
    </body>
    </html>
  `);
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ ok: true, ts: Date.now() }));

// ── API routes ────────────────────────────────────────────────────────────────
app.all('/api/create-order',       createOrderHandler);
app.all('/api/get-page-data',      getPageDataHandler);
app.all('/api/get-delivery-rates', getDeliveryRatesHandler);
app.all('/api/get-stopdesks',      getStopdesksHandler);
app.all('/api/track-event',        trackEventHandler);
app.all('/api/count-hubs',         countHubsHandler);
app.all('/api/log-error',          logErrorHandler);
app.all('/api/lead',               leadHandler);
app.all('/api/funnel',             funnelHandler);

// ── Manual sync trigger (protected with ADMIN_SECRET bearer token) ────────────
app.post('/api/admin/sync-page-data', async (req, res) => {
  const secret = process.env.ADMIN_SECRET;
  // Fail closed — if ADMIN_SECRET is not set, block all requests
  if (!secret || req.headers.authorization !== `Bearer ${secret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    await syncPageData();
    res.json({ ok: true, message: 'page-data.json updated on Shopify CDN' });
  } catch (err) {
    console.error('[admin/sync-page-data]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Daily cron — midnight Algeria time (UTC+1) = 23:00 UTC ───────────────────
// Runs once per day to push fresh ZR Express data to Shopify theme assets.
// This eliminates all ZR Express API calls from user page loads.
cron.schedule('0 23 * * *', async () => {
  console.log('[cron] Starting daily page-data sync...');
  try {
    await syncPageData();
    console.log('[cron] Daily page-data sync completed successfully');
  } catch (err) {
    console.error('[cron] Daily page-data sync failed:', err.message);
  }

  // Retention prune — drop leads older than 30 days so the table stays small
  // and the abandoned-callback queue stays fresh. Best-effort; no-op without a DB.
  try {
    const removed = await pruneLeads(30);
    if (removed) console.log(`[cron] Pruned ${removed} stale lead(s)`);
  } catch (err) {
    console.error('[cron] Lead prune failed:', err.message);
  }
  try {
    const removed = await pruneSessions(30);
    if (removed) console.log(`[cron] Pruned ${removed} stale session(s)`);
  } catch (err) {
    console.error('[cron] Session prune failed:', err.message);
  }
  try {
    const removed = await pruneRollups(30);
    if (removed) console.log(`[cron] Pruned ${removed} stale funnel rollup(s)`);
  } catch (err) {
    console.error('[cron] Rollup prune failed:', err.message);
  }
}, { timezone: 'UTC' });

// ── Hourly cron — summarise closed funnel windows ────────────────────────────
// Folds every fully-elapsed N-hour window of raw `sessions` into compact
// `funnel_rollups` rows and drops the raw rows. Raw sessions are thus kept only
// for the current open window (the live events the admin shows in real time).
// N = FUNNEL_ROLLUP_HOURS (default 1). Best-effort; no-op without a DB.
cron.schedule('5 * * * *', async () => {
  try {
    const folded = await rollupClosedBuckets();
    if (folded) console.log(`[cron] Funnel rollup folded ${folded} session(s) (window ${rollupHours()}h)`);
  } catch (err) {
    console.error('[cron] Funnel rollup failed:', err.message);
  }
}, { timezone: 'UTC' });

// ── Error handler — body-parser (413/400) and route errors ───────────────────
// Without this, an oversized/malformed body dies in express.json BEFORE any
// handler sets CORS headers → the browser sees an opaque network failure and
// the storefront can't distinguish it from the backend being down. Always
// reply JSON with CORS so the order form gets a readable error + can report it.
app.use((err, req, res, _next) => {
  setCorsHeaders(req, res);
  const status = err?.status || err?.statusCode || 500;
  console.error(`[server] ${req.method} ${req.path} failed pre-handler:`, status, err?.message,
    'content-length:', req.headers['content-length'] || '?');
  if (res.headersSent) return;
  res.status(status).json({
    error: status === 413 ? 'Payload too large' : (status < 500 ? 'Bad request' : 'Server error'),
    code:  status === 413 ? 'payload_too_large' : 'request_error',
  });
});

// ── Startup sync — populate static file immediately on first deploy ───────────
// Runs in background so it doesn't block the server from starting.
(async () => {
  try {
    await syncPageData();
    console.log('[startup] Initial page-data sync completed');
  } catch (err) {
    console.warn('[startup] Initial page-data sync skipped:', err.message);
  }
})();

app.listen(PORT, () => {
  console.log(`[server] Running on port ${PORT}`);
});
