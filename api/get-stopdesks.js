// api/get-stopdesks.js
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const TENANT  = process.env.ZR_TENANT;
  const API_KEY = process.env.ZR_API_KEY;

  if (!TENANT || !API_KEY) {
    return res.status(500).json({ error: 'Missing ZR Express credentials in environment' });
  }

  try {
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
        // no filter — return all hubs
      })
    });

    if (!zrRes.ok) {
      const err = await zrRes.text();
      console.error('ZR Express error:', err);
      return res.status(502).json({ error: 'Failed to fetch stopdesks from ZR Express' });
    }

    const data = await zrRes.json();
    console.log('ZR total hubs:', data.totalCount);

    const stopdesks = (data.items || []).map(hub => ({
      id:           hub.id,
      name:         hub.name,
      type:         hub.type,
      city:         hub.address?.city     || '',
      district:     hub.address?.district || '',
      street:       hub.address?.street   || '',
      openingHours: hub.openingHours      || '',
      phone:        hub.phone?.number1    || ''
    }));

    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate');
    return res.status(200).json({ stopdesks });

  } catch (err) {
    console.error('Network error:', err);
    return res.status(500).json({ error: 'Network error' });
  }
}
