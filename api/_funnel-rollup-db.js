// ============================================================
//  Funnel rollup DB — periodic summarisation of raw `sessions`
//
//  The raw `sessions` table (see _funnel-db.js) is ONE row per
//  visit. Left unchecked it grows with traffic. This module rolls
//  fully-elapsed time windows of raw sessions into compact summary
//  rows — one per (window, landing/origin, ad source) — then drops
//  the raw rows it summarised. Net effect:
//
//    • raw `sessions` only ever holds the CURRENT open window
//      (the "live" events the admin shows in real time)
//    • history lives in `funnel_rollups`: 24/N rows per day instead
//      of thousands of session rows — tiny, bounded storage.
//
//  The window length is configurable via FUNNEL_ROLLUP_HOURS (N,
//  default 1). Buckets are epoch-aligned to N-hour boundaries, so
//  for the usual divisors of 24 (1,2,3,4,6,8,12,24) a bucket lines
//  up with the wall clock. Each rollup row records its own
//  `bucket_hours`, so changing N later never corrupts old history —
//  the admin reads the value and renders buckets accordingly.
//
//  Best-effort, never on the order path. No DATABASE_URL → no-op.
// ============================================================

import pg from 'pg';
import { FUNNEL_STEPS } from './_funnel-db.js';

let pool = null;
let schemaReady = null;

/** Configured rollup window in hours. Clamped to a sane divisor-ish range. */
export function rollupHours() {
  const n = Math.floor(Number(process.env.FUNNEL_ROLLUP_HOURS) || 1);
  return Math.min(24, Math.max(1, n));
}

function getPool() {
  const url = process.env.DATABASE_URL;
  if (!url) return null;
  if (pool) return pool;
  const wantSsl =
    process.env.DATABASE_SSL === 'true' ||
    (!/railway\.internal/.test(url) && /\b(sslmode=require|proxy\.rlwy\.net|\.railway\.app)\b/.test(url));
  pool = new pg.Pool({
    connectionString: url,
    ssl: wantSsl ? { rejectUnauthorized: false } : undefined,
    max: 3, idleTimeoutMillis: 30_000, connectionTimeoutMillis: 5_000,
  });
  pool.on('error', err => console.error('[funnel-rollup-db] idle client error:', err.message));
  return pool;
}

async function ensureSchema(p) {
  if (schemaReady) return schemaReady;
  schemaReady = p.query(`
    CREATE TABLE IF NOT EXISTS funnel_rollups (
      bucket_start TIMESTAMPTZ NOT NULL,
      bucket_hours INTEGER     NOT NULL,
      origin       TEXT        NOT NULL DEFAULT '',
      source       TEXT        NOT NULL DEFAULT '',
      sessions     INTEGER     NOT NULL DEFAULT 0,
      converted    INTEGER     NOT NULL DEFAULT 0,
      steps        JSONB       NOT NULL DEFAULT '{}'::jsonb,
      rolled_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (bucket_start, bucket_hours, origin, source)
    );
    CREATE INDEX IF NOT EXISTS funnel_rollups_bucket_idx ON funnel_rollups (bucket_start DESC);
  `).catch(err => { console.error('[funnel-rollup-db] schema init failed:', err.message); schemaReady = null; throw err; });
  return schemaReady;
}

// Per-step COUNT(*) FILTER expression list, built once from the allowlist.
const STEP_COUNT_COLS = FUNNEL_STEPS
  .map(k => `count(*) FILTER (WHERE steps ? '${k}') AS "c_${k}"`)
  .join(',\n             ');

// jsonb_build_object('step', count(...), …) for writing the rollup steps map.
const STEP_JSONB = FUNNEL_STEPS
  .map(k => `'${k}', count(*) FILTER (WHERE steps ? '${k}')`)
  .join(',\n               ');

// Sum each step across grouped rollup rows: sum((steps->>'k')::int).
const STEP_SUM_COLS = FUNNEL_STEPS
  .map(k => `COALESCE(sum((steps->>'${k}')::int),0)::int AS "${k}"`)
  .join(',\n           ');

/**
 * Roll every fully-elapsed N-hour window of raw sessions into summary rows,
 * then delete the raw rows that were summarised. Idempotent and atomic: the
 * INSERT + DELETE run in one transaction so a session is counted exactly once.
 * Returns the number of raw sessions folded away. Best-effort.
 */
export async function rollupClosedBuckets() {
  const p = getPool();
  if (!p) return 0;
  const N = rollupHours();
  const span = N * 3600; // seconds per bucket
  let client;
  try {
    await ensureSchema(p);
    client = await p.connect();
    await client.query('BEGIN');

    // Start of the CURRENT (still-open) bucket — everything strictly before
    // this is a closed window safe to summarise.
    const cutoffExpr = `to_timestamp(floor(extract(epoch from now()) / ${span}) * ${span})`;
    const bucketExpr = `to_timestamp(floor(extract(epoch from created_at) / ${span}) * ${span})`;

    const ins = await client.query(`
      INSERT INTO funnel_rollups (bucket_start, bucket_hours, origin, source, sessions, converted, steps)
      SELECT ${bucketExpr} AS bucket_start,
             ${N} AS bucket_hours,
             COALESCE(origin,'') AS origin,
             COALESCE(source,'') AS source,
             count(*) AS sessions,
             count(*) FILTER (WHERE converted_at IS NOT NULL) AS converted,
             jsonb_build_object(
               ${STEP_JSONB}
             ) AS steps
        FROM sessions
       WHERE created_at < ${cutoffExpr}
       GROUP BY 1, 3, 4
      ON CONFLICT (bucket_start, bucket_hours, origin, source) DO UPDATE SET
        sessions  = funnel_rollups.sessions  + EXCLUDED.sessions,
        converted = funnel_rollups.converted + EXCLUDED.converted,
        steps     = (
          SELECT jsonb_object_agg(k, COALESCE((funnel_rollups.steps->>k)::int,0)
                                     + COALESCE((EXCLUDED.steps->>k)::int,0))
            FROM jsonb_object_keys(funnel_rollups.steps || EXCLUDED.steps) k
        ),
        rolled_at = now()
    `);

    const del = await client.query(`DELETE FROM sessions WHERE created_at < ${cutoffExpr}`);
    await client.query('COMMIT');
    if (del.rowCount) console.log(`[funnel-rollup] folded ${del.rowCount} session(s) into ${ins.rowCount} bucket(s) (${N}h)`);
    return del.rowCount || 0;
  } catch (err) {
    try { await client?.query('ROLLBACK'); } catch { /* ignore */ }
    console.error('[funnel-rollup] rollupClosedBuckets failed:', err.message);
    return 0;
  } finally {
    client?.release();
  }
}

/** Retention prune — drop rollup buckets older than `days` (default 30). */
export async function pruneRollups(days = 30) {
  const p = getPool();
  if (!p) return 0;
  try {
    await ensureSchema(p);
    const d = Math.max(1, Math.floor(Number(days) || 30));
    const res = await p.query(`DELETE FROM funnel_rollups WHERE bucket_start < now() - ($1 || ' days')::interval`, [String(d)]);
    return res.rowCount || 0;
  } catch (err) {
    console.error('[funnel-rollup-db] pruneRollups failed:', err.message);
    return 0;
  }
}

// Exported for the landing aggregator (same Postgres) so the query lives in one place.
export const ROLLUP_SQL = { STEP_COUNT_COLS, STEP_SUM_COLS };
