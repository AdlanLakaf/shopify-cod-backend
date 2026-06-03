// ============================================================
//  sync-page-data.js
//  Fetches ZR Express rates + hubs, builds the wilaya map,
//  and uploads the result to Shopify theme assets as
//  assets/page-data.json — one call per day from the cron,
//  not on every user visit.
//
//  Required env vars:
//    ZR_TENANT, ZR_API_KEY
//    SHOPIFY_MYSHOPIFY_DOMAIN, SHOPIFY_ADMIN_TOKEN
//    SHOPIFY_THEME_ID   ← numeric ID of the live theme
// ============================================================

import { fetchWithTimeout, log } from './_security.js';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join }  from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SHOPIFY_API_VER = '2024-10';

// ── Static data ───────────────────────────────────────────────────────────────
let WILAYAS_RAW, COMMUNES_RAW;
try {
  WILAYAS_RAW  = JSON.parse(readFileSync(join(__dirname, 'data', 'wilayas.json'),  'utf8'));
  COMMUNES_RAW = JSON.parse(readFileSync(join(__dirname, 'data', 'communes.json'), 'utf8'));
} catch (e) {
  console.error('[sync-page-data] Failed to load static JSON files:', e.message);
}

// ── ZR Express endpoints ──────────────────────────────────────────────────────
const ZR_RATES_URL = 'https://api.zrexpress.app/api/v1/delivery-pricing/rates';
const ZR_HUBS_URL  = 'https://api.zrexpress.app/api/v1/hubs/search';

// ── Helpers ───────────────────────────────────────────────────────────────────

function normaliseRates(raw) {
  const map = {};
  for (const territory of (raw.rates || [])) {
    const code = territory.toTerritoryCode;
    if (!code) continue;
    // Normalize to integer string — ZR Express may return "04" (leading zero)
    const key = String(parseInt(String(code), 10));
    if (key === 'NaN') continue;
    const entry = { home: null, desk: null };
    for (const dp of (territory.deliveryPrices || [])) {
      const type  = (dp.deliveryType || '').toLowerCase();
      const price = dp.discountedPrice != null ? dp.discountedPrice : dp.price;
      if      (type === 'home')         entry.home = price;
      else if (type === 'pickup-point') entry.desk = price;
      else {
        if      (entry.home === null) entry.home = price;
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
    communesByWilaya[key].push({ id: c.id, nameAr: c.commune_name || '', nameEn: c.commune_name_ascii || '' });
  }

  const hubsByWilaya = {};
  for (const hub of hubs) {
    const key = String(hub.wilayaId);
    if (!hubsByWilaya[key]) hubsByWilaya[key] = [];
    hubsByWilaya[key].push({
      id: hub.id, name: hub.name, city: hub.city,
      district: hub.district, street: hub.street,
      openingHours: hub.openingHours, phone: hub.phone
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

async function fetchRates(tenant, apiKey) {
  const res = await fetchWithTimeout(ZR_RATES_URL, {
    headers: { Accept: 'application/json', 'X-Tenant': tenant, 'X-Api-Key': apiKey }
  }, 12_000);
  if (!res.ok) throw new Error(`ZR rates HTTP ${res.status}`);
  return res.json();
}

function mapHub(hub) {
  const wilayaId = (() => {
    if (hub.wilayaCode)          return parseInt(hub.wilayaCode,          10);
    if (hub.wilayaId)            return parseInt(hub.wilayaId,            10);
    if (hub.address?.wilayaCode) return parseInt(hub.address.wilayaCode,  10);
    // Postal code prefix — unreliable for new wilayas 49–58 which kept old parent
    // wilaya postal codes when Algeria expanded from 48 to 58 wilayas.
    if (hub.address?.postalCode) {
      const prefix = parseInt(String(hub.address.postalCode).slice(0, 2), 10);
      if (!isNaN(prefix) && prefix >= 1 && prefix <= 58) return prefix;
    }
    return null;
  })();
  return {
    id:           String(hub.id),
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
  const PAGE_SIZE  = 200;
  const MAX_PAGES  = 20;
  const allItems   = [];
  let   pageNumber = 1;

  while (pageNumber <= MAX_PAGES) {
    const res = await fetchWithTimeout(ZR_HUBS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'X-Tenant': tenant, 'X-Api-Key': apiKey },
      body:   JSON.stringify({ pageNumber, pageSize: PAGE_SIZE })
    }, 12_000);
    if (!res.ok) throw new Error(`ZR hubs HTTP ${res.status} (page ${pageNumber})`);
    const data  = await res.json();
    const items = data.items || [];

    allItems.push(...items);
    if (items.length < PAGE_SIZE) break;
    pageNumber++;
  }

  return allItems
    .filter(hub => !!hub.isPickupPoint)
    .map(mapHub)
    .filter(hub => hub.wilayaId !== null);
}

// ── Main export ───────────────────────────────────────────────────────────────

export async function syncPageData() {
  const SHOP     = process.env.SHOPIFY_MYSHOPIFY_DOMAIN;
  const TOKEN    = process.env.SHOPIFY_ADMIN_TOKEN;
  const THEME_ID = process.env.SHOPIFY_THEME_ID;
  const tenant   = process.env.ZR_TENANT;
  const apiKey   = process.env.ZR_API_KEY;

  if (!SHOP || !TOKEN || !THEME_ID) {
    throw new Error('Missing SHOPIFY_MYSHOPIFY_DOMAIN, SHOPIFY_ADMIN_TOKEN, or SHOPIFY_THEME_ID');
  }
  if (!tenant || !apiKey) {
    throw new Error('Missing ZR_TENANT or ZR_API_KEY');
  }
  if (!WILAYAS_RAW || !COMMUNES_RAW) {
    throw new Error('Static data files (wilayas.json / communes.json) not loaded');
  }

  log('[sync-page-data] Fetching ZR Express rates + hubs...');
  const [rawRates, hubs] = await Promise.all([
    fetchRates(tenant, apiKey),
    fetchHubs(tenant, apiKey)
  ]);

  const rates   = normaliseRates(rawRates);
  const wilayas = buildWilayaMap(rates, hubs);
  const payload = JSON.stringify({ wilayas, generatedAt: Date.now() });

  log(`[sync-page-data] Built map for ${Object.keys(wilayas).length} wilayas. Uploading to Shopify...`);

  const uploadRes = await fetchWithTimeout(
    `https://${SHOP}/admin/api/${SHOPIFY_API_VER}/themes/${THEME_ID}/assets.json`,
    {
      method:  'PUT',
      headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
      body:    JSON.stringify({ asset: { key: 'assets/page-data.json', value: payload } })
    },
    15_000
  );

  if (!uploadRes.ok) {
    throw new Error(`Shopify asset upload failed HTTP ${uploadRes.status}`);
  }

  log('[sync-page-data] Done — page-data.json updated on Shopify CDN');
  return wilayas;
}
