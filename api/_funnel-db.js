// ============================================================
//  Funnel DB — anonymous on-page engagement funnel (sessions)
//
//  Complements the leads CRM. A `session` is ONE row per visit,
//  minted at page load (before any phone), upserted as the visitor
//  progresses: saw product → saw price → chose variant → filled
//  info → submitted → converted, plus side engagements (WhatsApp,
//  story, voice reviews, feedback images). Each step stores its
//  FIRST-touch timestamp in a `steps` JSONB map, so a row stays
//  small and the funnel is a cheap GROUP-BY aggregate.
//
//  Best-effort, never on the order path. No DATABASE_URL → no-op.
// ============================================================

import { getPool } from './_pg.js';

let schemaReady = null;

// Ordered main funnel + side engagements. Client mirrors this in lib/funnel.ts.
// Keys are a fixed allowlist — only these are accepted / aggregated.
export const FUNNEL_STEPS = [
  'page_view', 'product_view', 'price_view', 'variant_select', 'bundle_select',
  'form_start', 'info_filled', 'submit_click', 'converted',
  'whatsapp_click', 'story_view', 'vocals_view', 'vocals_play', 'feedback_view', 'feedback_click',
];
const STEP_SET = new Set(FUNNEL_STEPS);


async function ensureSchema(p) {
  if (schemaReady) return schemaReady;
  schemaReady = p.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id   TEXT PRIMARY KEY,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      source       TEXT,
      origin       TEXT,
      url          TEXT,
      entry_url    TEXT,
      referrer     TEXT,
      device       TEXT,
      lead_id      TEXT,
      phone        TEXT,
      converted_at TIMESTAMPTZ,
      steps        JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS sessions_created_idx ON sessions (created_at DESC);
    CREATE INDEX IF NOT EXISTS sessions_origin_idx  ON sessions (origin);
    CREATE INDEX IF NOT EXISTS sessions_steps_idx   ON sessions USING GIN (steps);
  `).catch(err => { console.error('[funnel-db] schema init failed:', err.message); schemaReady = null; throw err; });
  return schemaReady;
}

const clean = (s, max = 300) => String(s ?? '').replace(/<[^>]*>/g, '').trim().slice(0, max);

/**
 * Record one funnel step for a session (first-touch wins). Idempotent on
 * session_id; safe to call many times. `step` must be in the allowlist.
 * Returns true on write. Best-effort.
 */
export async function trackSession(input = {}) {
  const p = getPool();
  if (!p) return false;
  const sessionId = clean(input.sessionId, 80);
  if (!sessionId) return false;
  const step = STEP_SET.has(input.step) ? input.step : null;

  const nowIso = new Date().toISOString();
  const source   = clean(input.source, 60);
  const origin   = clean(input.origin, 200);
  const url      = clean(input.url, 1000);
  const entryUrl = clean(input.entryUrl, 1000);
  const referrer = clean(input.referrer, 1000);
  const device   = clean(input.device, 40);
  const leadId   = clean(input.leadId, 80);
  const phone    = clean(input.phone, 20);
  const isConvert = step === 'converted';

  // Only add the step key if it isn't already present (first-touch timestamps).
  const stepPatch = step
    ? `CASE WHEN sessions.steps ? $2 THEN '{}'::jsonb ELSE jsonb_build_object($2::text, $3::text) END`
    : `'{}'::jsonb`;

  try {
    await ensureSchema(p);
    await p.query(
      `INSERT INTO sessions (session_id, source, origin, url, entry_url, referrer, device, lead_id, phone, converted_at, steps)
       VALUES ($1,$4,$5,$6,$7,$8,$9,$10,$11, ${isConvert ? '$3::timestamptz' : 'NULL'},
               ${step ? `jsonb_build_object($2::text, $3::text)` : `'{}'::jsonb`})
       ON CONFLICT (session_id) DO UPDATE SET
         updated_at   = now(),
         source       = COALESCE(NULLIF(sessions.source,''), EXCLUDED.source),
         origin       = COALESCE(NULLIF(sessions.origin,''), EXCLUDED.origin),
         url          = COALESCE(NULLIF(EXCLUDED.url,''), sessions.url),
         entry_url    = COALESCE(NULLIF(sessions.entry_url,''), EXCLUDED.entry_url),
         referrer     = COALESCE(NULLIF(sessions.referrer,''), EXCLUDED.referrer),
         device       = COALESCE(NULLIF(sessions.device,''), EXCLUDED.device),
         lead_id      = COALESCE(NULLIF(EXCLUDED.lead_id,''), sessions.lead_id),
         phone        = COALESCE(NULLIF(EXCLUDED.phone,''), sessions.phone),
         converted_at = ${isConvert ? 'COALESCE(sessions.converted_at, $3::timestamptz)' : 'sessions.converted_at'},
         steps        = sessions.steps || (${stepPatch})`,
      [sessionId, step, nowIso, source, origin, url, entryUrl, referrer, device, leadId, phone]
    );
    return true;
  } catch (err) {
    console.error('[funnel-db] trackSession failed:', err.message, `(sid ${sessionId})`);
    return false;
  }
}

/** Stamp a session converted once its order is created (server-side link). */
export async function markSessionConverted({ sessionId = '', leadId = '', phone = '' } = {}) {
  const sid = clean(sessionId, 80);
  if (!sid) return false;
  return trackSession({ sessionId: sid, step: 'converted', leadId, phone });
}

/** Funnel aggregate: total sessions + count per step (+ conversion rate). */
export async function funnelMetrics({ from, to } = {}) {
  const p = getPool();
  if (!p) return null;
  try {
    await ensureSchema(p);
    const cols = FUNNEL_STEPS
      .map((k, i) => `count(*) FILTER (WHERE steps ? $${i + 1}) AS "${k}"`)
      .join(', ');
    const params = [...FUNNEL_STEPS];
    let where = '';
    if (from && to) {
      params.push(from, to);
      where = `WHERE created_at >= $${params.length - 1} AND created_at <= $${params.length}`;
    }
    const { rows } = await p.query(
      `SELECT count(*)::int AS sessions,
              count(*) FILTER (WHERE converted_at IS NOT NULL)::int AS converted_orders,
              ${cols}
         FROM sessions ${where}`,
      params
    );
    const r = rows[0] || {};
    const steps = {};
    for (const k of FUNNEL_STEPS) steps[k] = Number(r[k]) || 0;
    const sessions = Number(r.sessions) || 0;
    return {
      sessions,
      steps,
      conversionRate: sessions > 0 ? Math.round((steps.converted / sessions) * 1000) / 10 : 0,
    };
  } catch (err) {
    console.error('[funnel-db] funnelMetrics failed:', err.message);
    return null;
  }
}

/** Retention prune — keep the table small (mirrors leads). */
export async function pruneSessions(days = 30) {
  const p = getPool();
  if (!p) return 0;
  try {
    await ensureSchema(p);
    const d = Math.max(1, Math.floor(Number(days) || 30));
    const res = await p.query(`DELETE FROM sessions WHERE created_at < now() - ($1 || ' days')::interval`, [String(d)]);
    return res.rowCount || 0;
  } catch (err) {
    console.error('[funnel-db] pruneSessions failed:', err.message);
    return 0;
  }
}
