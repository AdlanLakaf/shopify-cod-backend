// ============================================================
//  ZR Express — Fetch Stopdesks
//  GET /api/get-stopdesks
//  Security: rate limiting + origin check (no HMAC — GET request)
// ============================================================

import { runSecurityChecks } from './_security.js';

export default async function handler(req, res) {
  const blocked = runSecurityChecks(req, res, { skipHmac: true });
  if (blocked) return;

  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const TENANT  = process.env.ZR_TENANT;
  const API_KEY = process.env.ZR_API_KEY;

  if (!TENANT || !API_KEY) {
    return res.status(500).json({ error: 'Missing ZR Express credentials in environment' });
  }

  try {
    // Fetch all hubs with no filter — filter by type client-side
    // This avoids any operator/value mismatch with the ZR Express API
    const zrRes = await fetch('https://api.zrexpress.app/api/v1/hubs/search', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'accept':       'application/json',
        'X-Tenant':     TENANT,
        'X-Api-Key':    API_KEY
      },
      body: JSON.stringify({
        pageNumber: 1,
        pageSize:   1000
      })
    });

    if (!zrRes.ok) {
      const err = await zrRes.text();
      console.error('ZR Express error:', err);
      return res.status(502).json({ error: 'Failed to fetch stopdesks from ZR Express' });
    }
    
    const data = await zrRes.json();
    console.log('All hubs:', JSON.stringify(data.items, null, 2));
    // Log all types returned so we can see exact values
    const allTypes = [...new Set((data.items || []).map(h => h.type))];
    console.log('Hub types returned by ZR Express:', allTypes);

    // Filter client-side — type contains 'stopdesk' (case-insensitive)
    const stopdesks = (data.items || [])
      .filter(hub => hub.type?.toLowerCase().includes('stopdesk'))
      .map(hub => ({
        id:           hub.id,
        name:         hub.name,
        city:         hub.address?.city     || '',
        district:     hub.address?.district || '',
        street:       hub.address?.street   || '',
        openingHours: hub.openingHours      || '',
        phone:        hub.phone?.number1    || ''
      }));

    console.log(`Found ${stopdesks.length} stopdesks out of ${data.items?.length || 0} total hubs`);

    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate');
    return res.status(200).json({ stopdesks });

  } catch (err) {
    console.error('Network error fetching stopdesks:', err);
    return res.status(500).json({ error: 'Network error' });
  }
}
