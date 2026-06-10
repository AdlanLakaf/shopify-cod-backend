/**
 * GET /api/get-page-data
 *
 * Single endpoint returning everything the Shopify order form needs:
 *   wilayas, communes, delivery rates, and pickup hubs — all merged.
 *
 * Cached in-memory for 10 minutes.
 */

import { runSecurityChecks, fetchWithTimeout } from './_security.js';
import { readFileSync }      from 'fs';
import { fileURLToPath }     from 'url';
import { dirname, join }     from 'path';

// ── Load static JSON files (compatible with all Node versions) ───────────────
const __dirname = dirname(fileURLToPath(import.meta.url));

let WILAYAS_RAW, COMMUNES_RAW;
try {
  WILAYAS_RAW  = JSON.parse(readFileSync(join(__dirname, 'data', 'wilayas.json'),  'utf8'));
  COMMUNES_RAW = JSON.parse(readFileSync(join(__dirname, 'data', 'communes.json'), 'utf8'));
} catch (e) {
  console.error('[get-page-data] Failed to load static JSON files:', e.message);
}

// ── ZR Express endpoints ─────────────────────────────────────────────────────
const ZR_RATES_URL = 'https://api.zrexpress.app/api/v1/delivery-pricing/rates';
const ZR_HUBS_URL  = 'https://api.zrexpress.app/api/v1/hubs/search';

// ── In-memory cache ───────────────────────────────────────────────────────────
const CACHE_TTL_MS = 10 * 60 * 1000;
let _cache   = null;
let _cacheTs = 0;

// ── Helpers ───────────────────────────────────────────────────────────────────

function normaliseRates(raw) {
  const map = {};
  for (const territory of (raw.rates || [])) {
    const code = territory.toTerritoryCode;
    if (!code) continue;
    // Normalize to integer string — ZR Express may return "04" (leading zero)
    // but wilaya keys in buildWilayaMap use String(parseInt(...)) = "4"
    const key = String(parseInt(String(code), 10));
    if (key === 'NaN') continue;
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
    map[key] = entry;
  }
  return map;
}

function buildWilayaMap(rates, hubs) {
  const communesByWilaya = {};
  for (const c of COMMUNES_RAW) {
    const key = String(parseInt(c.wilaya_code, 10));
    if (!communesByWilaya[key]) communesByWilaya[key] = [];
    communesByWilaya[key].push({
      id:     c.id,
      nameAr: c.commune_name       || '',
      nameEn: c.commune_name_ascii || ''
    });
  }

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
    const numericId = parseInt(w.wilaya_code, 10);
    const key       = String(numericId);
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

// ── ZR Express fetchers ───────────────────────────────────────────────────────

async function fetchRates(tenant, apiKey) {
  const res = await fetchWithTimeout(ZR_RATES_URL, {
    method:  'GET',
    headers: { Accept: 'application/json', 'X-Tenant': tenant, 'X-Api-Key': apiKey }
  }, 12_000);
  if (!res.ok) throw new Error(`ZR rates HTTP ${res.status}`);
  return res.json();
}

function mapHub(hub) {
  const wilayaId = (() => {
    // Prefer explicit API fields — most reliable
    if (hub.wilayaCode)          return parseInt(hub.wilayaCode,          10);
    if (hub.wilayaId)            return parseInt(hub.wilayaId,            10);
    if (hub.address?.wilayaCode) return parseInt(hub.address.wilayaCode,  10);
    // Last resort: postal code prefix — Algeria uses 5-digit codes where the
    // first 2 digits historically matched the wilaya number. However, when Algeria
    // expanded from 48 to 58 wilayas, new wilayas inherited old postal prefixes,
    // so this mapping can be wrong for wilayas 49–58. Use with caution.
    if (hub.address?.postalCode) {
      const prefix = parseInt(String(hub.address.postalCode).slice(0, 2), 10);
      if (!isNaN(prefix) && prefix >= 1 && prefix <= 58) return prefix;
    }
    return null;
  })();
  return {
    id:           String(hub.id),              // always string — prevents === mismatch with DOM values
    name:         hub.name                || '',
    city:         hub.address?.city       || '',
    district:     hub.address?.district   || '',
    street:       hub.address?.street     || '',
    openingHours: hub.openingHours        || '',
    phone:        hub.phone?.number1      || '',
    wilayaId:     (wilayaId >= 1 && wilayaId <= 58) ? wilayaId : null
  };
}

async function fetchHubs(tenant, apiKey) {
  // Fetch all pages — the API may cap pageSize below our requested value,
  // so we loop until a page returns fewer items than requested.
  const PAGE_SIZE  = 200; // conservative; avoids hitting API limits
  const MAX_PAGES  = 20;  // safety ceiling (~4 000 hubs max)
  const allItems   = [];
  let   pageNumber = 1;

  while (pageNumber <= MAX_PAGES) {
    const res = await fetchWithTimeout(ZR_HUBS_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'X-Tenant': tenant, 'X-Api-Key': apiKey },
      body:    JSON.stringify({ pageNumber, pageSize: PAGE_SIZE })
    }, 12_000);
    if (!res.ok) throw new Error(`ZR hubs HTTP ${res.status} (page ${pageNumber})`);
    const data  = await res.json();
    const items = data.items || [];

    allItems.push(...items);

    // Fewer items than requested → this was the last page
    if (items.length < PAGE_SIZE) break;
    pageNumber++;
  }

  return allItems
    // Bug fix: use !! instead of === true — API may return 1 or "true"
    .filter(hub => !!hub.isPickupPoint)
    .map(mapHub)
    .filter(hub => hub.wilayaId !== null);
}

// ── Handler ───────────────────────────────────────────────────────────────────

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, { skipHmac: true, rateBucket: 'pagedata', rateMax: 300 });
  if (blocked) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  if (!WILAYAS_RAW || !COMMUNES_RAW) {
    return res.status(500).json({
      error: 'Static data files not found. Make sure api/data/wilayas.json and api/data/communes.json exist.'
    });
  }

  const tenant = process.env.ZR_TENANT;
  const apiKey = process.env.ZR_API_KEY;
  if (!tenant || !apiKey) {
    return res.status(500).json({ error: 'Missing ZR_TENANT or ZR_API_KEY env vars' });
  }

  const now = Date.now();
  if (_cache && now - _cacheTs < CACHE_TTL_MS) {
    res.setHeader('X-Cache', 'HIT');
    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate');
    return res.status(200).json({ wilayas: _cache, cachedAt: _cacheTs });
  }

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
    console.error('[get-page-data] Error:', err.message);

    if (_cache) {
      res.setHeader('X-Cache', 'STALE');
      return res.status(200).json({ wilayas: _cache, cachedAt: _cacheTs, stale: true });
    }

    return res.status(502).json({ error: 'Failed to fetch delivery data. Please try again.' });
  }
}
