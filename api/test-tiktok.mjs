// Live smoke test for the TikTok Business API connection.
// Usage (PowerShell):
//   $env:TIKTOK_MARKETING_TOKEN="..."; $env:TIKTOK_ADVERTISER_ID="..."; node api/test-tiktok.mjs
// Verifies: token validity, advertiser access, lead-gen scope, and which
// lead-detail endpoint variant this account answers on (the client in
// _tiktok.js tries the same candidates in the same order).

const TT = 'https://business-api.tiktok.com/open_api/v1.3';
const token = process.env.TIKTOK_MARKETING_TOKEN;
const adv   = process.env.TIKTOK_ADVERTISER_ID;
if (!token || !adv) { console.error('Set TIKTOK_MARKETING_TOKEN and TIKTOK_ADVERTISER_ID'); process.exit(1); }

async function tt(path, params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) qs.set(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
  const res = await fetch(`${TT}${path}?${qs}`, { headers: { 'Access-Token': token } });
  const json = await res.json().catch(() => ({}));
  return json;
}

console.log('1) Advertiser info…');
const info = await tt('/advertiser/info/', { advertiser_ids: [adv] });
console.log('   code', info.code, info.message || '', '→', info.data?.list?.[0]?.name || '(no name)');

console.log('2) Campaign list (first 3)…');
const camps = await tt('/campaign/get/', { advertiser_id: adv, page_size: 3 });
console.log('   code', camps.code, camps.message || '');
for (const c of camps.data?.list || []) console.log('   •', c.campaign_id, c.campaign_name, c.objective_type || '');

console.log('3) Lead-detail endpoint candidates (expect ONE code 0 or 40001-style param error; 40104/40105 = missing scope)…');
for (const path of ['/page/lead/get/', '/pages/leads/get/', '/lead/get/']) {
  const r = await tt(path, { advertiser_id: adv, lead_ids: ['0'] });
  console.log(`   ${path} → code ${r.code} ${r.message || ''}`);
}

console.log('4) Lead task endpoints (polling backup)…');
for (const path of ['/page/lead/task/get/']) {
  const r = await tt(path, { advertiser_id: adv, task_id: '0' });
  console.log(`   ${path} → code ${r.code} ${r.message || ''}`);
}
console.log('\nDone. Any "code 0" or parameter-validation error means the endpoint EXISTS and the scope works; "permission" / "not found" codes mean that path is wrong for this account — tell Claude which ones answered.');
