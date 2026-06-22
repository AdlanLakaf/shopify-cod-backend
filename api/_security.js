// ============================================================
//  Security middleware — shared by all API functions
// ============================================================

import crypto from 'crypto';

// ── Logging — verbose only in TEST_MODE=true, errors always logged ────────────
export const TEST_MODE = process.env.TEST_MODE === 'true';
export const log = (...args) => { if (TEST_MODE) console.log(...args); };

// ── Shared fetch-with-timeout helper ─────────────────────────────────────────
export async function fetchWithTimeout(url, options = {}, timeoutMs = 10_000) {
  const ctrl  = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } catch (err) {
    if (err.name === 'AbortError') throw new Error(`Request timed out after ${timeoutMs}ms: ${url}`);
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

// ── Rate limit store ──────────────────────────────────────────────────────────
// Keyed by "<bucket>:<ip>" so each endpoint gets its own budget. Limits are
// deliberately high: Algerian mobile carriers use CGNAT, so one public IP is
// shared by thousands of real customers — a tight per-IP cap blocks buyers,
// not bots. (On Railway this Map lives in one persistent process shared by
// ALL routes and ALL customers, which made the old shared 30/5min cap fatal.)
const rateLimitStore = new Map();

const RATE_LIMIT_MAX    = 300;            // default per bucket+IP per window
const RATE_LIMIT_WINDOW = 5 * 60 * 1000;
const TIMESTAMP_TTL     = 5 * 60 * 1000;
// Largest legitimate body is a create-order POST: up to 5 line items + 5 display
// items + ~20 tracking fields + a full Arabic address/note (multi-byte UTF-8).
// That lands around ~4–5 KB, so the old 5 KB cap could clip a fat real order.
// 16 KB gives ~3× headroom for any genuine order while still rejecting the
// oversized payloads a flood/DoS would send.
const MAX_BODY_BYTES    = 16 * 1024;

// Purge expired entries every 10 minutes — prevents unbounded memory growth
setInterval(() => {
  const cutoff = Date.now() - RATE_LIMIT_WINDOW * 2;
  for (const [ip, entry] of rateLimitStore) {
    if (entry.windowStart < cutoff) rateLimitStore.delete(ip);
  }
}, 10 * 60 * 1000).unref(); // .unref() so this timer doesn't keep the process alive

// ── 1. CORS ───────────────────────────────────────────────────────────────────
export function setCorsHeaders(req, res, { anyOrigin = false } = {}) {
  const SHOP_DOMAIN    = process.env.SHOPIFY_STORE_DOMAIN;
  const SHOP_MYSHOPIFY = process.env.SHOPIFY_MYSHOPIFY_DOMAIN;
  const origin         = req.headers.origin || '';

  if (anyOrigin) {
    res.setHeader('Access-Control-Allow-Origin', '*');
  } else {
    // LANDING_ORIGINS: comma-separated full origins of the Next.js landing
    // deployments (e.g. "https://landing.example.com,https://x.up.railway.app")
    const landingOrigins = (process.env.LANDING_ORIGINS || '')
      .split(',')
      .map(s => s.trim().replace(/\/$/, ''))
      .filter(Boolean);

    const allowedOrigins = [
      SHOP_DOMAIN    ? `https://${SHOP_DOMAIN}`    : null,
      SHOP_MYSHOPIFY ? `https://${SHOP_MYSHOPIFY}` : null,
      ...landingOrigins,
    ].filter(Boolean);

    if (allowedOrigins.includes(origin)) {
      // Only echo the origin back when it is explicitly allowed
      res.setHeader('Access-Control-Allow-Origin', origin);
    }
    // When origin is not allowed, set nothing — browser blocks it
  }

  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Timestamp, X-Signature');
  res.setHeader('Vary', 'Origin');
}

// ── 2. Rate limiting ──────────────────────────────────────────────────────────
export function getClientIp(req) {
  return req.headers['x-forwarded-for']?.split(',')[0]?.trim()
    || req.socket?.remoteAddress
    || 'unknown';
}

// LOG-ONLY by default: CGNAT means one IP = thousands of real customers, so
// blocking by IP costs sales. We still count and log would-be blocks for
// visibility. Set RATE_LIMIT_ENFORCE=true in env to actually block (only do
// this during an active attack).
const RATE_LIMIT_ENFORCE = process.env.RATE_LIMIT_ENFORCE === 'true';

export function checkRateLimit(req, res, { bucket = 'global', max = RATE_LIMIT_MAX } = {}) {
  const key   = bucket + ':' + getClientIp(req);
  const now   = Date.now();
  const entry = rateLimitStore.get(key);

  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW) {
    rateLimitStore.set(key, { count: 1, windowStart: now });
    return null;
  }

  entry.count++;

  if (entry.count > max) {
    // Log once per 50 excess requests so a flood doesn't drown the logs
    if (entry.count % 50 === 1 || entry.count === max + 1) {
      console.warn(`[rate-limit] ${RATE_LIMIT_ENFORCE ? '429' : 'would-block (log-only)'} bucket=${bucket} ip=${getClientIp(req)} count=${entry.count}`);
    }
    if (RATE_LIMIT_ENFORCE) {
      const retryAfter = Math.ceil((RATE_LIMIT_WINDOW - (now - entry.windowStart)) / 1000);
      res.setHeader('Retry-After', retryAfter);
      return res.status(429).json({
        error: `Too many requests. Try again in ${Math.ceil(retryAfter / 60)} minutes.`,
        code:  'rate_limited'
      });
    }
  }

  return null;
}

// ── 3. Payload size ───────────────────────────────────────────────────────────
export function checkPayloadSize(req, res) {
  const contentLength = parseInt(req.headers['content-length'] || '0');
  if (contentLength > MAX_BODY_BYTES) {
    return res.status(413).json({ error: 'Payload too large' });
  }
  return null;
}

// ── 4. HMAC signature verification ───────────────────────────────────────────
export function verifyHmac(req, res) {
  const SECRET    = process.env.HMAC_SECRET;
  const timestamp = req.headers['x-timestamp'];
  const signature = req.headers['x-signature'];

  if (!SECRET) {
    // Fail closed — missing secret is a misconfiguration, not a pass
    console.error('HMAC_SECRET not set — rejecting request');
    return res.status(500).json({ error: 'Server misconfiguration' });
  }

  if (!timestamp || !signature) {
    return res.status(401).json({ error: 'Missing security headers' });
  }

  const ts  = parseInt(timestamp);
  const now = Date.now();
  // Reject if older than 5 min OR if more than 30 seconds in the future
  if (isNaN(ts) || now - ts > TIMESTAMP_TTL || ts - now > 30_000) {
    return res.status(401).json({ error: 'Request expired' });
  }

  const body     = JSON.stringify(req.body || {});
  const message  = `${timestamp}.${body}`;
  const expected = crypto.createHmac('sha256', SECRET).update(message).digest('hex');

  const sigBuffer      = Buffer.from(signature, 'hex');
  const expectedBuffer = Buffer.from(expected,  'hex');

  if (
    sigBuffer.length !== expectedBuffer.length ||
    !crypto.timingSafeEqual(sigBuffer, expectedBuffer)
  ) {
    console.warn('Invalid HMAC signature from IP:', req.headers['x-forwarded-for']?.split(',')[0] || 'unknown');
    return res.status(401).json({ error: 'Invalid signature' });
  }

  return null;
}

// ── 5. Turnstile bot verification — DISABLED ─────────────────────────────────
// export async function verifyTurnstile(req, res) {
//   const SECRET = process.env.TURNSTILE_SECRET;
//
//   if (!SECRET) {
//     console.warn('TURNSTILE_SECRET not set — skipping bot check');
//     return null;
//   }
//
//   const token = req.body?.turnstileToken;
//   if (!token) {
//     return res.status(403).json({ error: 'Missing bot verification token' });
//   }
//
//   const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || '';
//
//   try {
//     const verifyRes = await fetchWithTimeout(
//       'https://challenges.cloudflare.com/turnstile/v0/siteverify',
//       {
//         method:  'POST',
//         headers: { 'Content-Type': 'application/json' },
//         body:    JSON.stringify({ secret: SECRET, response: token, remoteip: ip })
//       },
//       8_000
//     );
//
//     const data = await verifyRes.json();
//
//     if (!data.success) {
//       console.warn('Turnstile failed:', data['error-codes'], 'IP:', ip);
//       return res.status(403).json({ error: 'Bot verification failed' });
//     }
//
//     return null;
//   } catch (err) {
//     // Fail open only on Cloudflare timeout — don't block real users if Cloudflare is down
//     console.error('Turnstile verification error (failing open):', err.message);
//     return null;
//   }
// }
export async function verifyTurnstile(_req, _res) { return null; } // TURNSTILE DISABLED — always passes

// ── 6. Combined security check ────────────────────────────────────────────────
export function runSecurityChecks(req, res, { skipHmac = false, anyOrigin = false, rateBucket = 'global', rateMax = RATE_LIMIT_MAX } = {}) {
  setCorsHeaders(req, res, { anyOrigin });

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return true;
  }

  const rateLimitResult = checkRateLimit(req, res, { bucket: rateBucket, max: rateMax });
  if (rateLimitResult) return true;

  if (req.method === 'POST') {
    const sizeResult = checkPayloadSize(req, res);
    if (sizeResult) return true;

    if (!skipHmac) {
      const hmacResult = verifyHmac(req, res);
      if (hmacResult) return true;
    }
  }

  return false;
}
