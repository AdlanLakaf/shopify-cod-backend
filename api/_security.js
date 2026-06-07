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
const rateLimitStore = new Map();

const RATE_LIMIT_MAX    = 30;
const RATE_LIMIT_WINDOW = 5 * 60 * 1000;
const TIMESTAMP_TTL     = 5 * 60 * 1000;
const MAX_BODY_BYTES    = 5 * 1024;

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
    const allowedOrigins = [
      SHOP_DOMAIN    ? `https://${SHOP_DOMAIN}`    : null,
      SHOP_MYSHOPIFY ? `https://${SHOP_MYSHOPIFY}` : null,
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
export function checkRateLimit(req, res) {
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
    || req.socket?.remoteAddress
    || 'unknown';

  const now   = Date.now();
  const entry = rateLimitStore.get(ip);

  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW) {
    rateLimitStore.set(ip, { count: 1, windowStart: now });
    return null;
  }

  if (entry.count >= RATE_LIMIT_MAX) {
    const retryAfter = Math.ceil((RATE_LIMIT_WINDOW - (now - entry.windowStart)) / 1000);
    res.setHeader('Retry-After', retryAfter);
    return res.status(429).json({
      error: `Too many requests. Try again in ${Math.ceil(retryAfter / 60)} minutes.`
    });
  }

  entry.count++;
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
export function runSecurityChecks(req, res, { skipHmac = false, anyOrigin = false } = {}) {
  setCorsHeaders(req, res, { anyOrigin });

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return true;
  }

  const rateLimitResult = checkRateLimit(req, res);
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
