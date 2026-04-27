/**
 * GET /api/get-delivery-rates
 *
 * Fetches delivery pricing from ZR Express API, caches in-memory for 10 min,
 * and returns a simplified wilaya-code-keyed map to the Shopify frontend.
 *
 * Security applied (via shared middleware):
 *   ✔ CORS          — restricted to your Shopify store domains only
 *   ✔ Rate limiting — 30 requests / 5 min per IP
 *   ✔ Method guard  — GET only (OPTIONS handled by middleware)
 *   ✗ HMAC          — intentionally skipped (GET has no request body to sign)
 *   ✗ Payload size  — N/A for GET
 *
 * Required env vars:
 *   ZR_TENANT_ID             — X-Tenant value from ZR Express dashboard
 *   SHOPIFY_STORE_DOMAIN     — e.g. handsnose.com
 *   SHOPIFY_MYSHOPIFY_DOMAIN — e.g. handsnose.myshopify.com
 *
 * Response shape:
 * {
 *   rates: {
 *     "1":  { home: 400, desk: 250 },
 *     "16": { home: 350, desk: 200 },
 *     ...
 *   },
 *   cachedAt: 1714200000000
 * }
 */

import { runSecurityChecks } from './_security.js';

const ZR_RATES_URL = 'https://api.zrexpress.app/api/v1/delivery-pricing/rates';
const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes

// In-memory cache — survives warm Vercel function instances
let _cache   = null;
let _cacheTs = 0;

/**
 * Normalise raw ZR Express rates into { wilayaCode: { home, desk } }.
 * toTerritoryCode matches Algeria wilaya numbers 1–58.
 * Uses discountedPrice when available, falls back to price.
 */
function normalise(raw) {
  const rates = {};

  for (const territory of (raw.rates || [])) {
    const code = territory.toTerritoryCode;
    if (!code) continue;

    const entry = { home: null, desk: null };

    for (const dp of (territory.deliveryPrices || [])) {
      const type  = (dp.deliveryType || '').toLowerCase();
      const price = (dp.discountedPrice != null) ? dp.discountedPrice : dp.price;

      if (
        type === 'home_delivery' ||
        type.includes('home')     ||
        type.includes('domicile') ||
        type.includes('door')
      ) {
        entry.home = price;
      } else if (
        type === 'desk_delivery' ||
        type.includes('desk')     ||
        type.includes('office')   ||
        type.includes('stopdesk')
      ) {
        entry.desk = price;
      } else {
        // Unknown type: fill home first, then desk
        if      (entry.home === null) entry.home = price;
        else if (entry.desk === null) entry.desk = price;
      }
    }

    rates[String(code)] = entry;
  }

  return rates;
}

export default async function handler(req, res) {
  // ── Security ─────────────────────────────────────────────────────────────
  // skipHmac: true — GET has no request body to sign or verify.
  // Middleware still handles: CORS (store-restricted), rate limiting, OPTIONS.
  const blocked = runSecurityChecks(req, res, { skipHmac: true });
  if (blocked) return;

  // ── Method guard ─────────────────────────────────────────────────────────
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  // ── Config check ─────────────────────────────────────────────────────────
  const tenantId = process.env.ZR_TENANT;
  if (!tenantId) {
    console.error('[get-delivery-rates] ZR_TENANT env var is missing');
    return res.status(500).json({ error: 'Delivery pricing not configured — set ZR_TENANT' });
  }

  // ── Serve cache if still fresh ────────────────────────────────────────────
  const now = Date.now();
  if (_cache && (now - _cacheTs) < CACHE_TTL_MS) {
    res.setHeader('X-Cache', 'HIT');
    return res.status(200).json({ rates: _cache, cachedAt: _cacheTs });
  }

  // ── Fetch fresh data from ZR Express ─────────────────────────────────────
  try {
    const upstream = await fetch(ZR_RATES_URL, {
      method:  'GET',
      headers: {
        'Accept':   'application/json',
        'X-Tenant': tenantId,
      },
    });

    if (!upstream.ok) {
      const body = await upstream.text();
      console.error('[get-delivery-rates] ZR Express API error:', upstream.status, body);

      // Serve stale cache rather than returning an error to the customer
      if (_cache) {
        res.setHeader('X-Cache', 'STALE');
        return res.status(200).json({ rates: _cache, cachedAt: _cacheTs, stale: true });
      }

      return res.status(502).json({
        error:  'Failed to fetch delivery rates from ZR Express',
        status: upstream.status,
      });
    }

    const raw        = await upstream.json();
    console.log(raw);
    const normalised = normalise(raw);

    _cache   = normalised;
    _cacheTs = now;

    res.setHeader('X-Cache', 'MISS');
    return res.status(200).json({ rates: normalised, cachedAt: _cacheTs });

  } catch (err) {
    console.error('[get-delivery-rates] Unexpected error:', err);

    // Serve stale cache on network error so customers still see prices
    if (_cache) {
      res.setHeader('X-Cache', 'STALE');
      return res.status(200).json({ rates: _cache, cachedAt: _cacheTs, stale: true });
    }

    return res.status(500).json({ error: 'Internal server error' });
  }
}
