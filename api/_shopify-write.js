// ============================================================
//  Shopify catalog WRITER — create / update / delete products
//  and variants, in single or bulk form, from our own admin.
//
//  Design rules (these follow from "Shopify is an integration,
//  not a system"):
//
//   1. SKU is never decorative. Every variant we create carries
//      the local stock row's uuid as its SKU — that is the ONLY
//      identity link between Shopify and our catalog, and it is
//      what lets a web order resolve to local stock without any
//      hand-made mapping table.
//   2. Inventory is untracked by default. Shopify must never be
//      able to refuse an order on stock grounds — availability is
//      our computation, pushed for display only. Opt in per call
//      with { track: true } once the numbers are trusted.
//   3. Every write is best-effort and reports what happened.
//      Callers get { ok, ... } or { ok:false, error } — a Shopify
//      outage degrades the storefront, never our own catalog.
//
//  Rate limits: REST admin is a 40-request bucket refilling at
//  2/s. All calls funnel through one serialized queue with a
//  minimum gap, and retry once on 429/5xx honouring Retry-After —
//  so a 200-variant bulk run is slow but never dropped.
// ============================================================

import { fetchWithTimeout } from './_security.js';
import { creds, invalidateCatalog } from './_shopify-catalog.js';

const API = '2024-10';
const MIN_GAP_MS = 260;   // ~3.8 req/s worst case → inside the 2/s refill + burst
const TIMEOUT_MS = 15_000;

let chain = Promise.resolve();
let lastAt = 0;

const sleep = ms => new Promise(r => setTimeout(r, ms));

/**
 * One REST call, serialized behind every other write so bulk runs
 * cannot trip the bucket. Returns the parsed body, or throws with a
 * message carrying Shopify's own field errors (which are the useful
 * part — "sku has already been taken" etc).
 */
function call(method, path, body) {
  const run = async () => {
    const c = creds();
    if (!c) throw new Error('Shopify credentials are not configured');

    const gap = MIN_GAP_MS - (Date.now() - lastAt);
    if (gap > 0) await sleep(gap);

    for (let attempt = 0; attempt < 2; attempt++) {
      lastAt = Date.now();
      const res = await fetchWithTimeout(`https://${c.SHOP}/admin/api/${API}/${path}`, {
        method,
        headers: {
          'X-Shopify-Access-Token': c.TOKEN,
          'Content-Type': 'application/json',
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      }, TIMEOUT_MS);

      if (res.status === 429 || res.status >= 500) {
        if (attempt === 0) {
          const wait = Math.max(1000, (parseFloat(res.headers.get('retry-after')) || 1) * 1000);
          await sleep(wait);
          continue;
        }
      }
      const text = await res.text();
      let data = null;
      try { data = text ? JSON.parse(text) : {}; } catch { /* non-JSON error page */ }

      if (!res.ok) throw new Error(`Shopify ${res.status}: ${describeErrors(data) || text.slice(0, 200)}`);
      return data;
    }
    throw new Error('Shopify unreachable after retry');
  };

  // Serialize: each call waits for the previous to settle, success or not.
  const queued = chain.then(run, run);
  chain = queued.catch(() => {});
  return queued;
}

/** Shopify returns { errors: { sku: ["has already been taken"] } } or a string. */
function describeErrors(data) {
  const e = data?.errors;
  if (!e) return '';
  if (typeof e === 'string') return e;
  return Object.entries(e).map(([k, v]) => `${k} ${Array.isArray(v) ? v.join(', ') : v}`).join('; ');
}

const money = dzd => (Math.round(Number(dzd) || 0)).toFixed(2);

/** Shape one of our variant descriptors into Shopify's variant payload. */
function variantPayload(v, { track = false } = {}) {
  const p = {
    price: money(v.priceDzd),
    sku: String(v.uuid || v.sku || ''),        // rule 1: SKU carries local identity
    inventory_management: track ? 'shopify' : null,
    inventory_policy: 'continue',              // rule 2: never refuse an order
    taxable: false,
    requires_shipping: true,
  };
  if (v.title) p.option1 = String(v.title);
  if (v.barcode) p.barcode = String(v.barcode);
  if (v.compareAtDzd) p.compare_at_price = money(v.compareAtDzd);
  if (v.weightGrams) { p.weight = Number(v.weightGrams); p.weight_unit = 'g'; }
  return p;
}

// ── Products ────────────────────────────────────────────────

/**
 * Create a product with its variants in one call.
 *   { title, brand, category, description, images:[url],
 *     status: 'active'|'draft', tags:[], optionName,
 *     variants: [{ uuid, title, priceDzd, compareAtDzd, barcode }] }
 * `optionName` names the variant axis (default 'الحجم' — volume).
 */
export async function createProduct(input = {}) {
  const variants = Array.isArray(input.variants) ? input.variants : [];
  if (!input.title) return { ok: false, error: 'title is required' };
  if (!variants.length) return { ok: false, error: 'at least one variant is required' };
  const missing = variants.find(v => !v.uuid && !v.sku);
  if (missing) return { ok: false, error: 'every variant needs a uuid — it becomes the SKU' };

  const product = {
    title: input.title,
    body_html: input.description || '',
    vendor: input.brand || '',
    product_type: input.category || '',
    status: input.status === 'draft' ? 'draft' : 'active',
    tags: Array.isArray(input.tags) ? input.tags.join(', ') : (input.tags || ''),
    options: [{ name: input.optionName || 'الحجم' }],
    variants: variants.map(v => variantPayload(v, input)),
  };
  if (Array.isArray(input.images) && input.images.length) {
    product.images = input.images.map(src => ({ src }));
  }

  try {
    const data = await call('POST', 'products.json', { product });
    invalidateCatalog();
    const pr = data.product;
    console.log(`[shopify-write] created product ${pr.id} "${pr.title}" (${pr.variants?.length || 0} variants)`);
    return {
      ok: true,
      productId: pr.id,
      variants: (pr.variants || []).map(v => ({ variantId: v.id, sku: v.sku, title: v.title, priceDzd: Math.round(parseFloat(v.price) || 0) })),
    };
  } catch (err) {
    console.error('[shopify-write] createProduct failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/** Patch product-level fields (title, brand, category, status, description, tags). */
export async function updateProduct(productId, fields = {}) {
  const id = Number(productId);
  if (!id) return { ok: false, error: 'productId is required' };
  const product = { id };
  if (fields.title       !== undefined) product.title        = fields.title;
  if (fields.description !== undefined) product.body_html    = fields.description;
  if (fields.brand       !== undefined) product.vendor       = fields.brand;
  if (fields.category    !== undefined) product.product_type = fields.category;
  if (fields.status      !== undefined) product.status       = fields.status === 'draft' ? 'draft' : 'active';
  if (fields.tags        !== undefined) product.tags         = Array.isArray(fields.tags) ? fields.tags.join(', ') : fields.tags;

  try {
    await call('PUT', `products/${id}.json`, { product });
    invalidateCatalog();
    return { ok: true };
  } catch (err) {
    console.error('[shopify-write] updateProduct failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/**
 * Delete a product. Destructive and unrecoverable on Shopify's side —
 * prefer archiving (status:'draft') unless the product was a mistake.
 * Past orders keep their line items either way.
 */
export async function deleteProduct(productId) {
  const id = Number(productId);
  if (!id) return { ok: false, error: 'productId is required' };
  try {
    await call('DELETE', `products/${id}.json`);
    invalidateCatalog();
    console.log(`[shopify-write] deleted product ${id}`);
    return { ok: true };
  } catch (err) {
    console.error('[shopify-write] deleteProduct failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/** Hide a product from the storefront without destroying it. The safe "delete". */
export const archiveProduct = productId => updateProduct(productId, { status: 'draft' });

// ── Variants ────────────────────────────────────────────────

/** Add one variant to an existing product. */
export async function addVariant(productId, v = {}) {
  const id = Number(productId);
  if (!id) return { ok: false, error: 'productId is required' };
  if (!v.uuid && !v.sku) return { ok: false, error: 'variant needs a uuid — it becomes the SKU' };
  try {
    const data = await call('POST', `products/${id}/variants.json`, { variant: variantPayload(v) });
    invalidateCatalog();
    return { ok: true, variantId: data.variant?.id, sku: data.variant?.sku };
  } catch (err) {
    console.error('[shopify-write] addVariant failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/** Patch one variant — price, sku, title, barcode, compare-at. */
export async function updateVariant(variantId, fields = {}) {
  const id = Number(variantId);
  if (!id) return { ok: false, error: 'variantId is required' };
  const variant = { id };
  if (fields.priceDzd     !== undefined) variant.price             = money(fields.priceDzd);
  if (fields.compareAtDzd !== undefined) variant.compare_at_price  = fields.compareAtDzd ? money(fields.compareAtDzd) : null;
  if (fields.uuid         !== undefined) variant.sku               = String(fields.uuid);
  if (fields.sku          !== undefined) variant.sku               = String(fields.sku);
  if (fields.title        !== undefined) variant.option1           = String(fields.title);
  if (fields.barcode      !== undefined) variant.barcode           = String(fields.barcode);

  try {
    await call('PUT', `variants/${id}.json`, { variant });
    invalidateCatalog();
    return { ok: true };
  } catch (err) {
    console.error('[shopify-write] updateVariant failed:', err.message);
    return { ok: false, error: err.message };
  }
}

export async function deleteVariant(productId, variantId) {
  const p = Number(productId), v = Number(variantId);
  if (!p || !v) return { ok: false, error: 'productId and variantId are required' };
  try {
    await call('DELETE', `products/${p}/variants/${v}.json`);
    invalidateCatalog();
    return { ok: true };
  } catch (err) {
    console.error('[shopify-write] deleteVariant failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/**
 * Stamp a local uuid onto an existing Shopify variant's SKU. This is the
 * healing operation: staff resolve one unmapped variant by hand, and the
 * link becomes permanent and shop-independent instead of a map row.
 */
export const setVariantSku = (variantId, uuid) => updateVariant(variantId, { sku: uuid });

// ── Bulk ────────────────────────────────────────────────────

/**
 * Apply many variant patches, one call each, paced by the queue.
 * Never aborts on a single failure — a bad SKU shouldn't strand the
 * other 199 rows. Returns per-row outcomes so the admin can show
 * exactly which ones need attention.
 *
 *   bulkUpdateVariants([{ variantId, priceDzd }, { variantId, sku }, …])
 *   → { ok, updated, failed, results: [{ variantId, ok, error }] }
 */
export async function bulkUpdateVariants(rows = []) {
  if (!Array.isArray(rows) || !rows.length) return { ok: false, error: 'no rows given' };
  const results = [];
  for (const row of rows) {
    const { variantId, ...fields } = row || {};
    const r = await updateVariant(variantId, fields);
    results.push({ variantId, ok: !!r.ok, error: r.error });
  }
  const updated = results.filter(r => r.ok).length;
  console.log(`[shopify-write] bulk: ${updated}/${results.length} variants updated`);
  return { ok: true, updated, failed: results.length - updated, results };
}

/**
 * Repricing pass: takes { uuid → priceDzd } and applies it to whichever
 * Shopify variants carry those SKUs. Callers work in local identity and
 * never have to know a variant_id — which is the whole point of SKU=uuid.
 */
export async function bulkRepriceByUuid(priceByUuid = {}, allVariants = []) {
  const wanted = new Map(Object.entries(priceByUuid).map(([k, v]) => [String(k), Number(v)]));
  const rows = [];
  for (const v of allVariants) {
    const target = wanted.get(String(v.sku || ''));
    if (target && Math.round(target) !== Math.round(v.priceDzd)) {
      rows.push({ variantId: v.variantId, priceDzd: target });
    }
  }
  if (!rows.length) return { ok: true, updated: 0, failed: 0, results: [], skipped: 'all prices already match' };
  return bulkUpdateVariants(rows);
}
