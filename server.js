// ============================================================
//  Express HTTP server — for Railway (production)
//  Vercel continues to work via vercel.json (testing only)
//
//  Railway provides PORT via env. All handler files are
//  Express-compatible as-is; only body parsing differs
//  (Vercel auto-parses, Express needs express.json()).
// ============================================================

import express from 'express';

import createOrderHandler    from './api/create-order.js';
import getPageDataHandler    from './api/get-page-data.js';
import getDeliveryRatesHandler from './api/get-delivery-rates.js';
import getStopdesksHandler   from './api/get-stopdesks.js';
import trackEventHandler     from './api/track-event.js';

const app  = express();
const PORT = process.env.PORT || 3000;

// Trust Railway's reverse proxy so req.headers['x-forwarded-for'] is reliable
app.set('trust proxy', 1);

// Body parsing — must come before routes (Vercel did this automatically)
app.use(express.json({ limit: '5kb' }));

// Health check — used by Railway healthcheck probe
app.get('/health', (_req, res) => res.json({ ok: true, ts: Date.now() }));

// API routes — handlers export default function handler(req, res), same as Express middleware
app.all('/api/create-order',       createOrderHandler);
app.all('/api/get-page-data',      getPageDataHandler);
app.all('/api/get-delivery-rates', getDeliveryRatesHandler);
app.all('/api/get-stopdesks',      getStopdesksHandler);
app.all('/api/track-event',        trackEventHandler);

app.listen(PORT, () => {
  console.log(`[server] Running on port ${PORT}`);
});
