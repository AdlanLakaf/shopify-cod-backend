// ============================================================
//  Shopify Files upload — host a perfume photo on Shopify's CDN
//  and get back a permanent https URL.
//
//  Why Shopify: we already have Admin API creds, the CDN is free,
//  and it keeps image BYTES out of our Postgres (every thumbnail
//  is then a plain <img src> to a CDN, never a DB read — which is
//  the cheap-on-Railway property we want).
//
//  Flow (GraphQL, 3 steps, only for a NEW photo — callers dedupe
//  by filename so this runs rarely):
//    1. stagedUploadsCreate → a one-time upload target
//    2. PUT the bytes to that target
//    3. fileCreate(originalSource = staged resourceUrl) → poll
//       until the file is READY and its CDN url appears
//
//  Best-effort: any failure returns '' and the caller falls back
//  to a type icon. A broken thumbnail must never break a sync.
// ============================================================

import { fetchWithTimeout } from './_security.js';
import { creds } from './_shopify-catalog.js';

const API = '2024-10';

async function gql(c, query, variables) {
  const res = await fetchWithTimeout(`https://${c.SHOP}/admin/api/${API}/graphql.json`, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': c.TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  }, 20_000);
  const data = await res.json();
  if (data.errors) throw new Error(`GraphQL: ${JSON.stringify(data.errors).slice(0, 200)}`);
  return data.data;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

/**
 * Upload one image (base64 data, no data: prefix needed but tolerated) and
 * return its CDN url, or '' on any failure. `filename` only shapes the stored
 * name; `mime` defaults to png.
 */
export async function uploadImage(dataB64, filename = 'photo.png', mime = 'image/png') {
  const c = creds();
  if (!c || !dataB64) return '';
  try {
    const b64 = String(dataB64).replace(/^data:[^,]*,/, '');
    const bytes = Buffer.from(b64, 'base64');
    if (!bytes.length) return '';

    // 1 — staged target
    const staged = await gql(c, `
      mutation stage($input:[StagedUploadInput!]!){
        stagedUploadsCreate(input:$input){
          stagedTargets{ url resourceUrl parameters{ name value } }
          userErrors{ message }
        }
      }`, {
      input: [{ filename, mimeType: mime, httpMethod: 'POST', resource: 'FILE', fileSize: String(bytes.length) }],
    });
    const target = staged?.stagedUploadsCreate?.stagedTargets?.[0];
    if (!target?.url) return '';

    // 2 — POST the bytes to the staged target (multipart form)
    const form = new FormData();
    for (const p of target.parameters) form.append(p.name, p.value);
    form.append('file', new Blob([bytes], { type: mime }), filename);
    const up = await fetchWithTimeout(target.url, { method: 'POST', body: form }, 25_000);
    if (!up.ok) { console.warn('[shopify-files] staged PUT HTTP', up.status); return ''; }

    // 3 — register the file, then poll for its CDN url
    const created = await gql(c, `
      mutation create($files:[FileCreateInput!]!){
        fileCreate(files:$files){ files{ id fileStatus ... on MediaImage{ image{ url } } } userErrors{ message } }
      }`, {
      files: [{ originalSource: target.resourceUrl, contentType: 'IMAGE' }],
    });
    const file = created?.fileCreate?.files?.[0];
    if (!file?.id) return '';
    if (file.image?.url) return file.image.url;

    // Not READY yet — poll a few times (Shopify processes async).
    for (let i = 0; i < 6; i++) {
      await sleep(700);
      const q = await gql(c, `query($id:ID!){ node(id:$id){ ... on MediaImage{ fileStatus image{ url } } } }`, { id: file.id });
      const url = q?.node?.image?.url;
      if (url) return url;
      if (q?.node?.fileStatus === 'FAILED') break;
    }
    return '';
  } catch (err) {
    console.warn('[shopify-files] uploadImage failed:', err.message);
    return '';
  }
}
