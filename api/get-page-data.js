/**
 * GET /api/get-page-data
 *
 * Single endpoint that returns everything the Shopify order form needs:
 *   • wilayas[]       — all 58 wilayas with names (ar + en)
 *   • communes[]      — all communes keyed by wilaya_code
 *   • delivery rates  — home + desk prices per wilaya
 *   • hubs[]          — pickup points per wilaya (wilayaId = numeric wilaya code)
 *
 * Cached in-memory for 10 minutes. One call replaces three.
 *
 * Security: CORS (store-restricted) + rate limiting + GET guard.
 * No HMAC — GET has no body to sign.
 *
 * Response shape:
 * {
 *   wilayas: {
 *     "23": {
 *       id: 23,
 *       nameAr: "عنابة",
 *       nameEn: "Annaba",
 *       homePrice: 400,
 *       deskPrice: 250,
 *       hubs: [
 *         {
 *           id: "...",
 *           name: "...",
 *           city: "...",
 *           district: "...",
 *           street: "...",
 *           openingHours: "...",
 *           phone: "..."
 *         }
 *       ],
 *       communes: [
 *         { id: 841, nameAr: "عين الباردة", nameEn: "Ain El Berda" }
 *       ]
 *     },
 *     ...
 *   },
 *   cachedAt: 1714200000000
 * }
 */

import { runSecurityChecks } from './_security.js';

// ── Static data (wilayas + communes shipped with the bundle) ─────────────────
// These never change at runtime so we import them directly.
import WILAYAS_RAW  from '../data/wilayas.json'   assert { type: 'json' };
import COMMUNES_RAW from '../data/communes.json'  assert { type: 'json' };

// ── ZR Express config ─────────────────────────────────────────────────────────
const ZR_RATES_URL = 'https://api.zrexpress.app/api/v1/delivery-pricing/rates';
const ZR_HUBS_URL  = 'https://api.zrexpress.app/api/v1/hubs/search';

// ── In-memory cache ───────────────────────────────────────────────────────────
const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
let _cache   = null;
let _cacheTs = 0;

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Parse wilaya code from a postal code string.
 * ZR Express postal codes are 5-digit strings like "23001".
 * We strip leading zeros and return the numeric wilaya id.
 */
function wilayaCodeFromPostal(postalCode) {
  if (!postalCode) return null;
  const s = String(postalCode).trim();
  // Take first 2 chars, parse as int to drop leading zero ("01" → 1, "23" → 23)
  const prefix = parseInt(s.slice(0, 2), 10);
  return isNaN(prefix) ? null : prefix;
}

/**
 * Normalise ZR Express delivery-pricing response into
 * { [wilayaCode]: { home, desk } }
 */
function normaliseRates(raw) {
  const map = {};
  for (const territory of (raw.rates || [])) {
    const code = territory.toTerritoryCode;
    if (!code) continue;

    const entry = { home: null, desk: null };
    for (const dp of (territory.deliveryPrices || [])) {
      const type  = (dp.deliveryType || '').toLowerCase();
      const price = dp.discountedPrice != null ? dp.discountedPrice : dp.price;

      if (type === 'home') {
        entry.home = price;
      } else if (type === 'pickup-point') {
        entry.desk = price;
      } else {
        if (entry.home === null) entry.home = price;
        else if (entry.desk === null) entry.desk = price;
      }
    }
    map[String(code)] = entry;
  }
  return map;
}

/**
 * Build the unified wilaya map from static JSON + live ZR data.
 *
 * wilayas.json shape:  { wilaya_code, wilaya_name (ar), wilaya_name_ascii (en) }
 * communes.json shape: { id, wilaya_code, commune_name (ar), commune_name_ascii (en) }
 */
function buildWilayaMap(rates, hubs) {
  // Index communes by wilaya_code for O(1) lookup
  const communesByWilaya = {};
  for (const c of COMMUNES_RAW) {
    const key = String(parseInt(c.wilaya_code, 10)); // "01" → "1"
    if (!communesByWilaya[key]) communesByWilaya[key] = [];
    communesByWilaya[key].push({
      id:    c.id,
      nameAr: c.commune_name       || '',
      nameEn: c.commune_name_ascii || ''
    });
  }

  // Index hubs by wilaya code
  const hubsByWilaya = {};
  for (const hub of hubs) {
    const key = String(hub.wilayaId);
    if (!hubsByWilaya[key]) hubsByWilaya[key] = [];
    hubsByWilaya[key].push({
      id:           hub.id,
      name:         hub.name,
      city:         hub.city,
      district:     hub.district,
      street:       hub.street,
      openingHours: hub.openingHours,
      phone:        hub.phone
    });
  }

  const result = {};
  for (const w of WILAYAS_RAW) {
    const numericId = parseInt(w.wilaya_code, 10); // e.g. 23
    const key       = String(numericId);            // "23"
    const rate      = rates[key] || { home: null, desk: null };

    result[key] = {
      id:        numericId,
      nameAr:    w.wilaya_name       || '',
      nameEn:    w.wilaya_name_ascii || '',
      homePrice: rate.home,
      deskPrice: rate.desk,
      hubs:      hubsByWilaya[key]     || [],
      communes:  communesByWilaya[key] || []
    };
  }
  return result;
}

// ── Fetch live data from ZR Express ──────────────────────────────────────────

async function fetchRates(tenant, apiKey) {
  const res = await fetch(ZR_RATES_URL, {
    method: 'GET',
    headers: {
      Accept:      'application/json',
      'X-Tenant':  tenant,
      'X-Api-Key': apiKey
    }
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`ZR rates ${res.status}: ${body}`);
  }
  return res.json();
}

async function fetchHubs(tenant, apiKey) {
  const res = await fetch(ZR_HUBS_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept:         'application/json',
      'X-Tenant':     tenant,
      'X-Api-Key':    apiKey
    },
    body: JSON.stringify({ pageNumber: 1, pageSize: 1000 })
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`ZR hubs ${res.status}: ${body}`);
  }
  const data = await res.json();

  // Only pickup points, and attach wilayaId resolved from postal code
  return (data.items || [])
    .filter(hub => hub.isPickupPoint === true)
    .map(hub => {
      const postal   = hub.address?.postalCode || '';
      const wilayaId = wilayaCodeFromPostal(postal);
      return {
        id:           hub.id,
        name:         hub.name || '',
        city:         hub.address?.city     || '',
        district:     hub.address?.district || '',
        street:       hub.address?.street   || '',
        openingHours: hub.openingHours      || '',
        phone:        hub.phone?.number1    || '',
        wilayaId                              // ← numeric, matches wilaya key
      };
    })
    .filter(hub => hub.wilayaId !== null); // drop any hub with unparseable postal
}

// ── Handler ───────────────────────────────────────────────────────────────────

export default async function handler(req, res) {
  // Security: CORS + rate limiting (no HMAC for GET)
  const blocked = runSecurityChecks(req, res, { skipHmac: true });
  if (blocked) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const tenant = process.env.ZR_TENANT;
  const apiKey = process.env.ZR_API_KEY;

  if (!tenant || !apiKey) {
    console.error('[get-page-data] Missing ZR_TENANT or ZR_API_KEY');
    return res.status(500).json({ error: 'Server misconfiguration: missing ZR Express credentials' });
  }

  // ── Serve cache if fresh ─────────────────────────────────────────────────
  const now = Date.now();
  if (_cache && now - _cacheTs < CACHE_TTL_MS) {
    res.setHeader('X-Cache', 'HIT');
    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate');
    return res.status(200).json({ wilayas: _cache, cachedAt: _cacheTs });
  }

  // ── Fetch fresh data in parallel ─────────────────────────────────────────
  try {
    const [rawRates, hubs] = await Promise.all([
      fetchRates(tenant, apiKey),
      fetchHubs(tenant, apiKey)
    ]);

    const rates   = normaliseRates(rawRates);
    const wilayas = buildWilayaMap(rates, hubs);

    _cache   = wilayas;
    _cacheTs = now;

    res.setHeader('X-Cache', 'MISS');
    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate');
    return res.status(200).json({ wilayas, cachedAt: _cacheTs });

  } catch (err) {
    console.error('[get-page-data] Fetch error:', err);

    // Serve stale cache rather than a hard error
    if (_cache) {
      res.setHeader('X-Cache', 'STALE');
      res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate');
      return res.status(200).json({ wilayas: _cache, cachedAt: _cacheTs, stale: true });
    }

    return res.status(502).json({ error: 'Failed to load page data from ZR Express' });
  }
}
