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
