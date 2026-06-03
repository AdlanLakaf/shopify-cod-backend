/**
 * GET /api/count-hubs
 *
 * Diagnostic: compares hub counts between:
 *   1. ZR Express API  — raw isPickupPoint=true total
 *   2. Our mapped output — what buildWilayaMap would put in page-data.json
 *
 * Exposes which wilaya-resolution path each hub took (wilayaCode / wilayaId /
 * address.wilayaCode / postalCode / none) so you can spot the postal-code
 * mismatch bug for new wilayas 49–58.
 */

import { runSecurityChecks, fetchWithTimeout } from './_security.js';
import { readFileSync }  from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname   = dirname(fileURLToPath(import.meta.url));
const WILAYAS_RAW = JSON.parse(readFileSync(join(__dirname, 'data', 'wilayas.json'), 'utf8'));
const ZR_HUBS_URL = 'https://api.zrexpress.app/api/v1/hubs/search';

// Build a name lookup once at module load
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

// Mirrors the exact resolution order used in get-page-data.js mapHub()
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

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, { skipHmac: true });
  if (blocked) return;

  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const tenant = process.env.ZR_TENANT;
  const apiKey = process.env.ZR_API_KEY;
  if (!tenant || !apiKey) return res.status(500).json({ error: 'Missing ZR_TENANT or ZR_API_KEY env vars' });

  try {
    const rawHubs    = await fetchAllRawHubs(tenant, apiKey);
    const apiTotal   = rawHubs.length;
    const pickupHubs = rawHubs.filter(h => !!h.isPickupPoint);
    const pickupTotal = pickupHubs.length;

    const viaBreakdown = { wilayaCode: 0, wilayaId: 0, 'address.wilayaCode': 0, postalCode: 0, none: 0 };
    const apiPerWilaya = {};  // wilayaId string → count
    const unresolved   = [];

    for (const hub of pickupHubs) {
      const { id, via } = resolveWilayaId(hub);
      viaBreakdown[via] = (viaBreakdown[via] || 0) + 1;

      if (!id) {
        unresolved.push({
          hubId:      hub.id,
          name:       hub.name,
          postalCode: hub.address?.postalCode ?? null,
          rawWilayaCode: hub.wilayaCode ?? null,
          rawWilayaId:   hub.wilayaId   ?? null
        });
        continue;
      }
      const key = String(id);
      apiPerWilaya[key] = (apiPerWilaya[key] || 0) + 1;
    }

    // New wilayas spotlight — these are most likely to have postal-code mismatch
    const NEW_WILAYAS = ['49','50','51','52','53','54','55','56','57','58'];
    const newWilayaReport = {};
    for (const k of NEW_WILAYAS) {
      newWilayaReport[k] = {
        name:    WILAYA_NAMES[k] || '?',
        inApi:   apiPerWilaya[k] || 0
      };
    }

    // Hubs whose postalCode maps to a new-wilaya number — suspect cross-mapping
    const postalCodeMapped = pickupHubs
      .filter(hub => {
        const { via } = resolveWilayaId(hub);
        return via === 'postalCode';
      })
      .map(hub => {
        const { id } = resolveWilayaId(hub);
        return {
          hubId:      hub.id,
          name:       hub.name,
          postalCode: hub.address?.postalCode,
          mappedTo:   id,
          mappedName: WILAYA_NAMES[String(id)] || '?'
        };
      });

    // Full per-wilaya table (sorted by wilaya id)
    const perWilaya = Object.fromEntries(
      Object.entries(apiPerWilaya)
        .sort((a, b) => parseInt(a[0]) - parseInt(b[0]))
        .map(([k, count]) => [k, { name: WILAYA_NAMES[k] || '?', count }])
    );

    return res.status(200).json({
      // ── Summary ──
      counts: {
        apiRawTotal:         apiTotal,
        isPickupPointTotal:  pickupTotal,
        mappedToWilaya:      pickupTotal - unresolved.length,
        unresolved:          unresolved.length
      },
      // ── How each hub was resolved ──
      viaBreakdown,
      // ── New wilayas (49–58) — compare inApi with what page-data shows ──
      newWilayas49to58: newWilayaReport,
      // ── Hubs resolved only via postalCode prefix (risky for 49–58) ──
      postalCodeMappedCount: postalCodeMapped.length,
      postalCodeMappedSample: postalCodeMapped.slice(0, 10),
      // ── Hubs that couldn't be assigned to any wilaya ──
      unresolvedSample: unresolved.slice(0, 10),
      // ── Full breakdown ──
      perWilaya
    });

  } catch (err) {
    return res.status(502).json({ error: err.message });
  }
}
