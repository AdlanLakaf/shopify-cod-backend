/**
 * test-page-data.mjs
 * 
 * Run locally to validate the /api/get-page-data response
 * without needing the frontend or Shopify.
 *
 * Usage:
 *   1. Add your ZR Express credentials to a .env file or export them:
 *        export ZR_TENANT=your_tenant
 *        export ZR_API_KEY=your_api_key
 *   2. Run:
 *        node test-page-data.mjs
 *   3. Optionally filter to a specific wilaya:
 *        node test-page-data.mjs 58
 */

import 'dotenv/config';   // remove this line if you're not using dotenv

const BACKEND_URL = process.env.BACKEND_URL
  || 'https://shopify-cod-backend-seven.vercel.app/api/get-page-data';

// ── or test locally if you run `vercel dev` ──────────────────────────────────
// const BACKEND_URL = 'http://localhost:3000/api/get-page-data';

const wilayaFilter = process.argv[2]; // e.g. "58" for El Menia

async function main() {
  console.log(`\nFetching: ${BACKEND_URL}\n`);

  const res = await fetch(BACKEND_URL);

  console.log(`Status:   ${res.status} ${res.statusText}`);
  console.log(`X-Cache:  ${res.headers.get('x-cache') || 'n/a'}\n`);

  if (!res.ok) {
    const text = await res.text();
    console.error('Error response:', text);
    process.exit(1);
  }

  const json = await res.json();
  const wilayas = json.wilayas || {};

  // ── Summary ──────────────────────────────────────────────────────────────
  const keys = Object.keys(wilayas);
  console.log(`✅ Wilayas returned: ${keys.length}`);

  const totalHubs     = keys.reduce((n, k) => n + (wilayas[k].hubs?.length     || 0), 0);
  const totalCommunes = keys.reduce((n, k) => n + (wilayas[k].communes?.length || 0), 0);
  const noHomePrice   = keys.filter(k => wilayas[k].homePrice === null);
  const noDeskPrice   = keys.filter(k => wilayas[k].deskPrice === null);
  const noHubs        = keys.filter(k => (wilayas[k].hubs?.length || 0) === 0);

  console.log(`📦 Total hubs (pickup points):  ${totalHubs}`);
  console.log(`🏙️  Total communes:              ${totalCommunes}`);
  console.log(`💰 Wilayas missing home price:  ${noHomePrice.length} → [${noHomePrice.join(', ')}]`);
  console.log(`💰 Wilayas missing desk price:  ${noDeskPrice.length} → [${noDeskPrice.join(', ')}]`);
  console.log(`🏢 Wilayas with zero hubs:      ${noHubs.length} → [${noHubs.join(', ')}]\n`);

  // ── Specific wilaya detail ────────────────────────────────────────────────
  if (wilayaFilter) {
    const key = String(parseInt(wilayaFilter, 10));
    const w   = wilayas[key];
    if (!w) {
      console.error(`❌ Wilaya ${wilayaFilter} not found in response.`);
      process.exit(1);
    }
    console.log(`\n── Wilaya ${key}: ${w.nameAr} (${w.nameEn}) ──`);
    console.log(`   Home price: ${w.homePrice ?? 'null'} DZD`);
    console.log(`   Desk price: ${w.deskPrice ?? 'null'} DZD`);
    console.log(`   Communes:   ${w.communes.length}`);
    if (w.communes.length) {
      console.log('   First 3 communes:');
      w.communes.slice(0, 3).forEach(c =>
        console.log(`     • [${c.id}] ${c.nameAr} / ${c.nameEn}`)
      );
    }
    console.log(`   Hubs:       ${w.hubs.length}`);
    if (w.hubs.length) {
      console.log('   All hubs:');
      w.hubs.forEach(h =>
        console.log(`     • [${h.id}] ${h.name} — ${h.city}, ${h.district}`)
      );
    } else {
      console.log('   ⚠️  No pickup points for this wilaya.');
    }
  } else {
    // ── Print a summary table of every wilaya ────────────────────────────
    console.log('── Per-wilaya summary ──────────────────────────────────────────');
    console.log('ID  | Wilaya              | Home  | Desk  | Hubs | Communes');
    console.log('----|---------------------|-------|-------|------|----------');
    keys
      .sort((a, b) => Number(a) - Number(b))
      .forEach(k => {
        const w    = wilayas[k];
        const id   = String(k).padStart(2, '0');
        const name = (w.nameEn || '').padEnd(19, ' ').slice(0, 19);
        const home = String(w.homePrice ?? '—').padStart(5);
        const desk = String(w.deskPrice ?? '—').padStart(5);
        const hubs = String(w.hubs.length).padStart(4);
        const comm = String(w.communes.length).padStart(8);
        console.log(`${id}  | ${name} | ${home} | ${desk} | ${hubs} | ${comm}`);
      });
    console.log('\nTip: run  node test-page-data.mjs 58  to inspect a specific wilaya');
  }
}

main().catch(err => {
  console.error('Unhandled error:', err);
  process.exit(1);
});
