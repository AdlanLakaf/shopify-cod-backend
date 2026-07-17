// ============================================================
//  Shopify catalog reader — products + variants for the admin
//  variant picker, and single-variant lookup for mapping
//  auto-fill. Read-only, 10-min in-memory cache (one Shopify
//  call per cache window regardless of how many staff browse).
// ============================================================

import { fetchWithTimeout } from './_security.js';

const CACHE_TTL = 10 * 60 * 1000;
let cache = { ts: 0, products: null };

function creds() {
  const SHOP  = process.env.SHOPIFY_MYSHOPIFY_DOMAIN || process.env.SHOPIFY_STORE_DOMAIN;
  const TOKEN = process.env.SHOPIFY_ADMIN_TOKEN;
  return SHOP && TOKEN ? { SHOP, TOKEN } : null;
}

/**
 * All active products flattened to picker rows:
 * { productId, productTitle, image, variantId, variantTitle, priceDzd }
 * Paginates up to 5 × 250 products. Returns [] without creds / on failure.
 */
export async function listVariants() {
  if (cache.products && Date.now() - cache.ts < CACHE_TTL) return cache.products;
  const c = creds();
  if (!c) return [];
  const rows = [];
  try {
    let url = `https://${c.SHOP}/admin/api/2024-10/products.json?limit=250&status=active&fields=id,title,image,variants`;
    for (let page = 0; page < 5 && url; page++) {
      const res = await fetchWithTimeout(url, { headers: { 'X-Shopify-Access-Token': c.TOKEN } }, 15_000);
      if (!res.ok) { console.warn('[catalog] products fetch HTTP', res.status); break; }
      const data = await res.json();
      for (const pr of data.products || []) {
        for (const v of pr.variants || []) {
          rows.push({
            productId:    pr.id,
            productTitle: pr.title,
            image:        pr.image?.src || '',
            variantId:    v.id,
            variantTitle: v.title === 'Default Title' ? '' : v.title,
            priceDzd:     Math.round(parseFloat(v.price) || 0),
          });
        }
      }
      // cursor pagination via Link header
      const link = res.headers.get('link') || '';
      const next = link.split(',').find(x => x.includes('rel="next"'));
      url = next ? next.slice(next.indexOf('<') + 1, next.indexOf('>')) : null;
    }
    cache = { ts: Date.now(), products: rows };
    return rows;
  } catch (err) {
    console.error('[catalog] listVariants failed:', err.message);
    return cache.products || [];
  }
}

/** One variant's { title, priceDzd, image } (product + variant title joined) or null. */
export async function getVariantInfo(variantId) {
  const id = Number(variantId);
  if (!id) return null;
  const all = await listVariants();
  const hit = all.find(v => v.variantId === id);
  if (hit) {
    return { title: hit.variantTitle ? `${hit.productTitle} — ${hit.variantTitle}` : hit.productTitle, priceDzd: hit.priceDzd, image: hit.image || '' };
  }
  // Not in the active-products cache (draft product / brand-new variant) — direct lookup.
  const c = creds();
  if (!c) return null;
  try {
    const res = await fetchWithTimeout(
      `https://${c.SHOP}/admin/api/2024-10/variants/${id}.json`,
      { headers: { 'X-Shopify-Access-Token': c.TOKEN } }, 10_000);
    if (!res.ok) return null;
    const v = (await res.json()).variant;
    return v ? { title: v.title === 'Default Title' ? '' : v.title, priceDzd: Math.round(parseFloat(v.price) || 0), image: '' } : null;
  } catch (err) {
    console.error('[catalog] getVariantInfo failed:', err.message);
    return null;
  }
}
