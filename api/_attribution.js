// ============================================================
//  Traffic source attribution — last paid-click wins
//
//  Primary signal: `trafficSource` (hn_src cookie, 3-day TTL)
//    Set on every paid landing, always overwrites → true last-click.
//    Already resolved to a slug by the browser (tiktok / google /
//    meta / facebook / instagram), so no UA work needed here.
//
//  Fallback when hn_src is absent or expired:
//    1. ttclid → tiktok (paid)
//    2. gclid  → google (paid)
//    3. fbc    → meta/facebook/instagram via User-Agent
//    4. referrer domain → organic social
//    5. organic (direct / unknown)
//
//  _fbc is intentionally kept sticky (first Meta click) for CAPI
//  match quality — it is NOT used for attribution priority here.
// ============================================================

const FB_UA_RE = /FBAN|FBAV|FBIOS|FB_IAB|FBSV|FBDV|FBMD|FBOP/i;
const IG_UA_RE = /Instagram/i;

const VALID_SOURCES = new Set([
  'tiktok', 'google', 'meta', 'facebook', 'instagram'
]);

// ── Attribution gate ─────────────────────────────────────────────────────────
// Only hn_src (7-day TTL, last paid-click) counts for deciding which ad
// platform gets the event.  The longer-lived click-ID cookies (_fbc, _ttclid,
// 90 days) are intentionally NOT used here — they exist only for CAPI match
// quality, not for the fire/skip decision.
// Returns: 'tiktok' | 'meta' | 'google' | null (null = fire nothing)
export function resolveAdPlatform(trafficSource) {
  if (trafficSource === 'tiktok')                                                       return 'tiktok';
  if (trafficSource === 'meta' || trafficSource === 'facebook' || trafficSource === 'instagram') return 'meta';
  if (trafficSource === 'google')                                                       return 'google';
  return null; // expired, organic, or direct → fire nothing
}

// ── Ad-type classification ───────────────────────────────────────────────────
// Buckets every order/lead by WHAT KIND of ad (or none) produced it, so staff
// can triage accordingly: lead-form leads need a confirmation call with product
// choice, conversion-ad orders are already qualified, organic needs no ad ops.
//   lead_ad        — TikTok/Meta instant-form lead (set by the form pipelines)
//   conversion_ad  — paid click from a conversion campaign (site checkout)
//   reach_ad       — reach/awareness campaign (TikTok objective REACH)
//   boost_ad       — boosted post / traffic / engagement objective
//   organic_social — unpaid social referrer (…-organic sources)
//   organic        — direct / search / unknown
export function adTypeFromSource(source) {
  const s = String(source || '');
  if (!s || s === 'organic') return 'organic';
  if (s.endsWith('-organic')) return 'organic_social';
  if (['tiktok', 'meta', 'facebook', 'instagram', 'google'].includes(s)) return 'conversion_ad';
  if (s === 'tiktok_form' || s === 'meta_form') return 'lead_ad';
  return 'organic';
}

/** TikTok campaign objective_type → ad type bucket (form-lead pipeline). */
export function adTypeFromTikTokObjective(objective) {
  const o = String(objective || '').toUpperCase();
  if (!o) return 'lead_ad';                       // it came from a lead form
  if (o === 'LEAD_GENERATION') return 'lead_ad';
  if (o === 'REACH') return 'reach_ad';
  if (['TRAFFIC', 'ENGAGEMENT', 'VIDEO_VIEWS', 'COMMUNITY_INTERACTION', 'FOLLOWERS'].includes(o)) return 'boost_ad';
  if (['CONVERSIONS', 'WEB_CONVERSIONS', 'PRODUCT_SALES', 'APP_PROMOTION', 'VALUE_OPTIMIZATION'].includes(o)) return 'conversion_ad';
  return 'lead_ad';
}

export function detectSource({ trafficSource, fbc, ttclid, gclid, userAgent = '', referrer = '' }) {
  // ── Primary: hn_src last-click cookie (set by scripts.liquid) ──
  if (trafficSource && VALID_SOURCES.has(trafficSource)) {
    return trafficSource;
  }

  const ua  = String(userAgent);
  const ref = String(referrer).toLowerCase();

  // ── Fallback: raw click IDs (when hn_src is absent/expired) ──
  if (ttclid) return 'tiktok';
  if (gclid)  return 'google';

  if (fbc) {
    if (IG_UA_RE.test(ua)) return 'instagram';
    if (FB_UA_RE.test(ua)) return 'facebook';
    return 'meta';
  }

  // ── Organic social via referrer ──
  if (/instagram\.com/i.test(ref))              return 'instagram-organic';
  if (/facebook\.com|fb\.com|m\.me/i.test(ref)) return 'facebook-organic';
  if (/tiktok\.com/i.test(ref))                 return 'tiktok-organic';
  if (/google\./i.test(ref))                    return 'google-organic';
  if (/snapchat\.com/i.test(ref))               return 'snapchat-organic';
  if (/twitter\.com|x\.com/i.test(ref))         return 'twitter-organic';
  if (/youtube\.com/i.test(ref))                return 'youtube-organic';

  return 'organic';
}
