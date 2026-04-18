// ============================================================
//  ZR Express — Fetch Stopdesks
//  GET /api/get-stopdesks
//  Fetches all hubs of type "stopdesk" from ZR Express API
//  Credentials stay server-side in Vercel env vars
// ============================================================

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const TENANT  = process.env.ZR_TENANT;    // Your X-Tenant ID
  const API_KEY = process.env.ZR_API_KEY;   // Your X-Api-Key

  if (!TENANT || !API_KEY) {
    return res.status(500).json({ error: 'Missing ZR Express credentials in environment' });
  }

  try {
    // Fetch all stopdesks — paginate if needed (pageSize 100 covers most cases)
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
        pageSize:   100,
        advancedFilter: {
          logic: 'AND',
          filters: [
            {
              field:    'type',
              operator: 'eq',
              value:    'stopdesk'
            }
          ]
        }
      })
    });

    if (!zrRes.ok) {
      const err = await zrRes.text();
      console.error('ZR Express error:', err);
      return res.status(502).json({ error: 'Failed to fetch stopdesks from ZR Express' });
    }

    const data = await zrRes.json();

    // Shape the response — only send what the form needs
    console.log('ZR raw response:', JSON.stringify(data).slice(0, 500));
    const stopdesks = (data.items || []).map(hub => ({
      id:           hub.id,
      name:         hub.name,
      city:         hub.address?.city     || '',
      district:     hub.address?.district || '',
      street:       hub.address?.street   || '',
      openingHours: hub.openingHours      || '',
      phone:        hub.phone?.number1    || ''
    }));

    // Cache for 10 minutes — stopdesks don't change often
    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate');
    return res.status(200).json({ stopdesks });

  } catch (err) {
    console.error('Network error fetching stopdesks:', err);
    return res.status(500).json({ error: 'Network error' });
  }
}
