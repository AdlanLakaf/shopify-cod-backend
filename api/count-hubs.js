/**
 * GET /api/count-hubs
 *
 * Diagnostic: exposes exactly what ZR Express returns vs what we keep.
 * Key addition over v1: shows ALL raw hubs for new wilayas 49–58 BEFORE
 * any isPickupPoint / wilaya-resolution filter, so you can see the real
 * field values on every hub and spot why some are dropped.
 *
 * Only active when TEST_MODE=true env var is set.
 */

import { runSecurityChecks, fetchWithTimeout } from './_security.js';
import { readFileSync }  from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname   = dirname(fileURLToPath(import.meta.url));
const WILAYAS_RAW = JSON.parse(readFileSync(join(__dirname, 'data', 'wilayas.json'), 'utf8'));
const ZR_HUBS_URL = 'https://api.zrexpress.app/api/v1/hubs/search';

const WILAYA_NAMES = {};
for (const w of WILAYAS_RAW) {
  WILAYA_NAMES[String(parseInt(w.wilaya_code, 10))] = w.wilaya_name;
}

async function fetchAllRawHubs(tenant, apiKey) {
  const PAGE_SIZE = 200, MAX_PAGES = 20;
  const all = [];
  let page = 1;
  while (page <= MAX_PAGES) {
    const res = await fetchWithTimeout(ZR_HUBS_URL, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'X-Tenant': tenant, 'X-Api-Key': apiKey },
      body:    JSON.stringify({ pageNumber: page, pageSize: PAGE_SIZE })
    }, 15_000);
    if (!res.ok) throw new Error(`ZR hubs HTTP ${res.status} (page ${page})`);
    const data  = await res.json();
    const items = data.items || [];
    all.push(...items);
    if (items.length < PAGE_SIZE) break;
    page++;
  }
  return all;
}

// Mirrors exact resolution order in get-page-data.js mapHub()
function resolveWilayaId(hub) {
  if (hub.wilayaCode) {
    const id = parseInt(hub.wilayaCode, 10);
    if (id >= 1 && id <= 58) return { id, via: 'wilayaCode' };
  }
  if (hub.wilayaId) {
    const id = parseInt(hub.wilayaId, 10);
    if (id >= 1 && id <= 58) return { id, via: 'wilayaId' };
  }
  if (hub.address?.wilayaCode) {
    const id = parseInt(hub.address.wilayaCode, 10);
    if (id >= 1 && id <= 58) return { id, via: 'address.wilayaCode' };
  }
  if (hub.address?.postalCode) {
    const prefix = parseInt(String(hub.address.postalCode).slice(0, 2), 10);
    if (!isNaN(prefix) && prefix >= 1 && prefix <= 58) return { id: prefix, via: 'postalCode' };
  }
  return { id: null, via: 'none' };
}

// Strip only the fields we need to understand a hub — avoids huge payloads
function summariseHub(hub) {
  const { id, via } = resolveWilayaId(hub);
  return {
    hubId:            hub.id,
    name:             hub.name,
    isPickupPoint:    hub.isPickupPoint,
    // Direct wilaya fields
    wilayaCode:       hub.wilayaCode       ?? null,
    wilayaId:         hub.wilayaId         ?? null,
    addrWilayaCode:   hub.address?.wilayaCode ?? null,
    postalCode:       hub.address?.postalCode ?? null,
    city:             hub.address?.city    ?? null,
    // How our resolver sees it
    resolvedWilaya:   id,
    resolvedVia:      via,
    resolvedName:     id ? (WILAYA_NAMES[String(id)] || '?') : null,
  };
}

export default async function handler(req, res) {
  if (process.env.TEST_MODE !== 'true') {
    return res.status(404).json({ error: 'Not found' });
  }

  const blocked = runSecurityChecks(req, res, { skipHmac: true });
  if (blocked) return;

  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const tenant = process.env.ZR_TENANT;
  const apiKey = process.env.ZR_API_KEY;
  if (!tenant || !apiKey) return res.status(500).json({ error: 'Missing ZR_TENANT or ZR_API_KEY env vars' });

  try {
    const rawHubs = await fetchAllRawHubs(tenant, apiKey);

    // ── Counts before any filter ──────────────────────────────────────────────
    const totalFromApi   = rawHubs.length;
    const pickupHubs     = rawHubs.filter(h => !!h.isPickupPoint);
    const nonPickupHubs  = rawHubs.filter(h => !h.isPickupPoint);
    const pickupTotal    = pickupHubs.length;

    // ── Per-wilaya counts for pickup hubs (after isPickupPoint filter) ────────
    const viaBreakdown = { wilayaCode: 0, wilayaId: 0, 'address.wilayaCode': 0, postalCode: 0, none: 0 };
    const apiPerWilaya = {};
    const unresolved   = [];

    for (const hub of pickupHubs) {
      const { id, via } = resolveWilayaId(hub);
      viaBreakdown[via] = (viaBreakdown[via] || 0) + 1;
      if (!id) { unresolved.push(summariseHub(hub)); continue; }
      const key = String(id);
      apiPerWilaya[key] = (apiPerWilaya[key] || 0) + 1;
    }

    // ── New wilayas 49–58: full hub list BEFORE isPickupPoint filter ──────────
    // Shows every hub that resolves to one of these wilayas so you can see
    // the real isPickupPoint value and all wilaya-resolution fields.
    const NEW_WILAYA_KEYS = new Set(['49','50','51','52','53','54','55','56','57','58']);

    const newWilayaAllHubs = rawHubs
      .map(hub => summariseHub(hub))
      .filter(s => s.resolvedWilaya && NEW_WILAYA_KEYS.has(String(s.resolvedWilaya)));

    // Also catch hubs that mention new-wilaya cities by name but resolved elsewhere
    // (postal-code cross-mapping) — look at ALL hubs resolved to old wilayas whose
    // postalCode prefix differs from the resolved wilaya id
    const postalCodeMismatches = pickupHubs
      .map(hub => summariseHub(hub))
      .filter(s => {
        if (s.resolvedVia !== 'postalCode') return false;
        // flag if the postal prefix maps to a wilaya number that doesn't match
        // what a direct wilayaCode/wilayaId field would say (those are absent,
        // which is why we fell through to postalCode)
        return true; // already means wilayaCode/wilayaId were absent
      });

    // ── Summary per new wilaya ────────────────────────────────────────────────
    const newWilayaSummary = {};
    for (const k of NEW_WILAYA_KEYS) {
      const allForWilaya     = newWilayaAllHubs.filter(h => String(h.resolvedWilaya) === k);
      const pickupForWilaya  = allForWilaya.filter(h => !!h.isPickupPoint);
      newWilayaSummary[k] = {
        name:              WILAYA_NAMES[k] || '?',
        totalHubs:         allForWilaya.length,    // all, regardless of isPickupPoint
        isPickupPointTrue: pickupForWilaya.length, // what we'd include
        hubs:              allForWilaya,            // full detail for every hub
      };
    }

    // ── One raw sample hub (first pickup hub) so you can see the full schema ──
    const rawSample = pickupHubs[0] ?? rawHubs[0] ?? null;

    // ── Full per-wilaya table (pickup hubs only, sorted) ─────────────────────
    const perWilaya = Object.fromEntries(
      Object.entries(apiPerWilaya)
        .sort((a, b) => parseInt(a[0]) - parseInt(b[0]))
        .map(([k, count]) => [k, { name: WILAYA_NAMES[k] || '?', count }])
    );

    return res.status(200).json({
      counts: {
        apiRawTotal:        totalFromApi,
        isPickupPointTrue:  pickupTotal,
        isPickupPointFalse: nonPickupHubs.length,
        mappedToWilaya:     pickupTotal - unresolved.length,
        unresolved:         unresolved.length,
      },
      viaBreakdown,
      // ── THE KEY SECTION: every hub for wilayas 49–58, unfiltered ──
      newWilayas49to58: newWilayaSummary,
      // ── Hubs using postalCode fallback (no wilayaCode/wilayaId in API) ──
      postalCodeFallbackCount:  postalCodeMismatches.length,
      postalCodeFallbackSample: postalCodeMismatches.slice(0, 5),
      // ── Unresolvable pickup hubs ──
      unresolvedSample: unresolved.slice(0, 10),
      // ── Full pickup-hub breakdown by wilaya ──
      perWilaya,
      // ── Raw schema sample (first hub, all fields) ──
      rawHubSample: rawSample,
    });

  } catch (err) {
    return res.status(502).json({ error: err.message });
  }
}
