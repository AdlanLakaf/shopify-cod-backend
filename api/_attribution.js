// ============================================================
//  Traffic source attribution
//  Returns a short slug used as a Shopify order tag (src-XXX)
//
//  Priority:
//    1. Paid TikTok  (ttclid)
//    2. Paid Google  (gclid)
//    3. Paid Meta — Instagram vs Facebook vs ambiguous meta
//       • Instagram in-app browser  → "instagram"
//       • Facebook in-app browser   → "facebook"
//       • Meta ad, opened in phone browser → "meta"
//    4. Organic social (no click-id, but referrer is social domain)
//    5. Direct / organic / unknown  → "organic"
// ============================================================

const FB_UA_RE  = /FBAN|FBAV|FBIOS|FB_IAB|FBSV|FBDV|FBMD|FBOP/i;
const IG_UA_RE  = /Instagram/i;

export function detectSource({ fbc, ttclid, gclid, userAgent = '', referrer = '' }) {
  const ua  = String(userAgent);
  const ref = String(referrer).toLowerCase();

  // ── Paid clicks ──────────────────────────────────────────
  if (ttclid) return 'tiktok';
  if (gclid)  return 'google';

  if (fbc) {
    if (IG_UA_RE.test(ua)) return 'instagram';
    if (FB_UA_RE.test(ua)) return 'facebook';
    // Meta ad clicked from external browser (can't tell FB vs IG)
    return 'meta';
  }

  // ── Organic social (no paid click-id, but recognisable referrer) ──
  if (/instagram\.com/i.test(ref))       return 'instagram-organic';
  if (/facebook\.com|fb\.com|m\.me/i.test(ref)) return 'facebook-organic';
  if (/tiktok\.com/i.test(ref))          return 'tiktok-organic';
  if (/google\./i.test(ref))             return 'google-organic';
  if (/snapchat\.com/i.test(ref))        return 'snapchat-organic';
  if (/twitter\.com|x\.com/i.test(ref))  return 'twitter-organic';
  if (/youtube\.com/i.test(ref))         return 'youtube-organic';

  return 'organic';
}
