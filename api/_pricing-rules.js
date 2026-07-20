// ============================================================
//  Bulk pricing rules for the ONLINE price only.
//
//  Nothing here writes a local price. The shop's price is the
//  ERP's business; these rules move the web overlay.
//
//  Rules:
//    percent   { pct: +20 }        online = base × 1.20
//    amount    { amount: +500 }    online = base + 500
//    set       { price: 3000 }     online = 3000
//    tierUp    { steps: 1 }        price of the NEXT quality tier at
//                                  the same volume, from the matrix
//
//  Every rule takes { base: 'local'|'online', roundTo, includeOverridden }.
//  `base` matters: repeatedly applying +20% to the ONLINE price compounds,
//  while applying it to the LOCAL price is idempotent. Default is local.
//
//  Every run is a dry run first. apply() returns the same shape as
//  preview() so the UI shows exactly what it is about to do, and the
//  previous price is captured on every row so one bad rule is reversible.
//
//  tierUp only exists for extrait/shopMade — originals and decants have
//  no quality tier. Rows that cannot take the rule are SKIPPED WITH A
//  REASON, never silently left at the old price.
// ============================================================

import { getPool } from './_pg.js';

const num = (v, d = 0) => (Number.isFinite(Number(v)) ? Number(v) : d);

const roundPrice = (v, step) => {
  const s = num(step, 0);
  if (s <= 0) return Math.round(v);
  return Math.round(v / s) * s;
};

/**
 * Compute the new price for one row, or the reason it can't be computed.
 * Pure — no DB, no writes — so preview and apply can never disagree.
 */
export function computeNewPrice(row, rule, ctx) {
  const base = rule.base === 'online'
    ? (row.onlinePriceDzd == null ? row.localPriceDzd : row.onlinePriceDzd)
    : row.localPriceDzd;

  if (row.priceIssue) return { skip: row.priceIssue };
  if (row.priceOverridden && !rule.includeOverridden) {
    return { skip: 'manually overridden — tick "include overridden" to change it' };
  }

  let next;
  switch (rule.type) {
    case 'percent':
      if (!base) return { skip: 'no base price' };
      next = base * (1 + num(rule.pct) / 100);
      break;
    case 'amount':
      if (!base) return { skip: 'no base price' };
      next = base + num(rule.amount);
      break;
    case 'set':
      next = num(rule.price);
      break;
    case 'tierUp': {
      if (row.type !== 'extrait' && row.type !== 'shopMade') {
        return { skip: `${row.type} has no quality tier` };
      }
      if (!row.tierId) return { skip: 'no quality tier set' };
      // The tier itself carries its product_type, so the ladder follows from
      // the row's tier rather than from a second type mapping.
      const ladder = ctx.laddersByType.get(ctx.tierTypeById.get(row.tierId));
      if (!ladder) return { skip: 'no tier ladder for this product type' };
      const at = ladder.indexOf(row.tierId);
      if (at < 0) return { skip: 'tier not in ladder' };
      const target = ladder[at + Math.max(1, num(rule.steps, 1))];
      if (target == null) return { skip: 'already at the highest tier — add the next one manually' };
      const cell = ctx.matrixByCell.get(`${target}|${row.volumeMl}`);
      if (cell == null) {
        return { skip: `tier ${ctx.tierNameById.get(target) || target} has no ${row.volumeMl}ml cell — fill it in the matrix` };
      }
      next = num(cell);
      break;
    }
    default:
      return { skip: `unknown rule "${rule.type}"` };
  }

  if (!Number.isFinite(next) || next < 0) return { skip: 'rule produced an invalid price' };
  return { price: roundPrice(next, rule.roundTo) };
}

/** Load the tier ladders + matrix a tierUp rule needs. */
export async function loadRuleContext(brandId) {
  const p = getPool();
  const ctx = {
    matrixByCell: new Map(), laddersByType: new Map(),
    tierTypeById: new Map(), tierNameById: new Map(),
  };
  if (!p) return ctx;
  const [cats, cells] = await Promise.all([
    p.query(`SELECT local_id, product_type, name, min_price FROM pos_price_categories
              WHERE brand_id=$1 ORDER BY product_type, min_price`, [num(brandId)]),
    p.query(`SELECT category_id, volume_ml, sell_price FROM pos_price_matrix WHERE brand_id=$1`, [num(brandId)]),
  ]);
  for (const c of cats.rows) {
    ctx.tierTypeById.set(c.local_id, c.product_type);
    ctx.tierNameById.set(c.local_id, c.name);
    if (!ctx.laddersByType.has(c.product_type)) ctx.laddersByType.set(c.product_type, []);
    // Ordered by min_price, so "next tier" is the next entry — the ladder IS
    // the price ordering, which is what "upgrade a category" means.
    ctx.laddersByType.get(c.product_type).push(c.local_id);
  }
  for (const c of cells.rows) ctx.matrixByCell.set(`${c.category_id}|${num(c.volume_ml)}`, num(c.sell_price));
  return ctx;
}

/**
 * Dry run: what WOULD change. Never writes.
 * → { changes: [{ sku, name, from, to }], skipped: [{ sku, name, reason }] }
 */
export function previewRule(rows, rule, ctx) {
  const changes = [], skipped = [];
  for (const row of rows) {
    const r = computeNewPrice(row, rule, ctx);
    if (r.skip) { skipped.push({ sku: row.sku, name: row.localName, reason: r.skip }); continue; }
    if (row.onlinePriceDzd != null && Math.round(row.onlinePriceDzd) === Math.round(r.price)) {
      skipped.push({ sku: row.sku, name: row.localName, reason: 'already at that price' });
      continue;
    }
    changes.push({
      sku: row.sku, name: row.localName, volumeMl: row.volumeMl,
      localPrice: row.localPriceDzd, from: row.onlinePriceDzd, to: r.price,
    });
  }
  return { changes, skipped };
}

/**
 * Apply a previewed rule. Captures prev_price_dzd on every row so the whole
 * run is revertible, and writes one audit entry describing the run.
 */
export async function applyRule(brandId, rows, rule, actor = '') {
  const p = getPool();
  if (!p) return { ok: false, error: 'no database' };
  const bid = num(brandId);
  const ctx = await loadRuleContext(bid);
  const { changes, skipped } = previewRule(rows, rule, ctx);
  if (!changes.length) return { ok: true, updated: 0, skipped, changes: [] };

  const client = await p.connect();
  try {
    await client.query('BEGIN');
    for (const c of changes) {
      const row = rows.find(r => r.sku === c.sku);
      await client.query(
        `INSERT INTO pos_online_products (brand_id, sku, product_uuid, volume_ml,
             online_price_dzd, prev_price_dzd, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6, now())
         ON CONFLICT (brand_id, sku) DO UPDATE SET
           online_price_dzd = EXCLUDED.online_price_dzd,
           prev_price_dzd   = pos_online_products.online_price_dzd,
           updated_at       = now()`,
        [bid, c.sku, row?.uuid || '', row?.volumeMl ?? null, c.to, c.from]
      );
    }
    await client.query(
      `INSERT INTO pos_price_audit (brand_id, sku, old_price, new_price, rule, actor, rows_count)
       VALUES ($1,'',NULL,NULL,$2,$3,$4)`,
      [bid, JSON.stringify(rule).slice(0, 500), String(actor).slice(0, 80), changes.length]
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[pricing] applyRule failed:', err.message);
    return { ok: false, error: err.message };
  } finally {
    client.release();
  }

  console.log(`[pricing] brand ${bid}: ${rule.type} applied to ${changes.length} rows (${skipped.length} skipped)`);
  return { ok: true, updated: changes.length, changes, skipped };
}

/** Undo the last run for a set of SKUs by swapping price back to prev. */
export async function revertPrices(brandId, skus = [], actor = '') {
  const p = getPool();
  if (!p) return { ok: false, error: 'no database' };
  if (!Array.isArray(skus) || !skus.length) return { ok: false, error: 'no skus given' };
  try {
    const r = await p.query(
      `UPDATE pos_online_products
          SET online_price_dzd = prev_price_dzd,
              prev_price_dzd   = online_price_dzd,
              updated_at = now()
        WHERE brand_id=$1 AND sku = ANY($2) AND prev_price_dzd IS NOT NULL`,
      [num(brandId), skus.slice(0, 2000)]
    );
    await p.query(
      `INSERT INTO pos_price_audit (brand_id, rule, actor, rows_count) VALUES ($1,'revert',$2,$3)`,
      [num(brandId), String(actor).slice(0, 80), r.rowCount]
    );
    return { ok: true, reverted: r.rowCount };
  } catch (err) {
    console.error('[pricing] revertPrices failed:', err.message);
    return { ok: false, error: err.message };
  }
}
