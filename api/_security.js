// ============================================================
//  Security middleware — shared by all API functions
//  Provides: HMAC verification, timestamp check,
//            rate limiting, origin check, payload size limit
// ============================================================

import crypto from 'crypto';

// ── In-memory rate limit store ──
// Key: IP address  Value: { count, windowStart }
const rateLimitStore = new Map();

const RATE_LIMIT_MAX    = 30;     // max requests per window
const RATE_LIMIT_WINDOW = 10 * 60 * 1000; // 10 minutes in ms
const TIMESTAMP_TTL     = 5 * 60 * 1000;  // request expires after 5 minutes
const MAX_BODY_BYTES    = 5 * 1024;        // 5KB max payload

// ── 1. CORS — restrict to your Shopify store only ──
export function setCorsHeaders(req, res) {
  const SHOP_DOMAIN        = process.env.SHOPIFY_STORE_DOMAIN;   // e.g. handsnose.com
  const SHOP_MYSHOPIFY     = process.env.SHOPIFY_MYSHOPIFY_DOMAIN; // e.g. handsnose.myshopify.com
  const origin             = req.headers.origin || '';

  // All domains that are allowed to call this API
  const allowedOrigins = [
    SHOP_DOMAIN        ? `https://${SHOP_DOMAIN}`    : null,  // custom domain
    SHOP_MYSHOPIFY     ? `https://${SHOP_MYSHOPIFY}` : null,  // myshopify domain
  ].filter(Boolean);

  // Also allow any *.myshopify.com for theme preview URLs
  const isAllowed = allowedOrigins.includes(origin) ||
    origin.endsWith('.myshopify.com') ||
    origin.includes('shopify.com');

  res.setHeader(
    'Access-Control-Allow-Origin',
    isAllowed ? origin : (allowedOrigins[0] || '*')
  );

  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Timestamp, X-Signature');
  res.setHeader('Vary', 'Origin');
}

// ── 2. Rate limiting by IP ──
export function checkRateLimit(req, res) {
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
    || req.socket?.remoteAddress
    || 'unknown';

  const now  = Date.now();
  const entry = rateLimitStore.get(ip);

  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW) {
    // New window
    rateLimitStore.set(ip, { count: 1, windowStart: now });
    return null; // OK
  }

  if (entry.count >= RATE_LIMIT_MAX) {
    const retryAfter = Math.ceil((RATE_LIMIT_WINDOW - (now - entry.windowStart)) / 1000);
    res.setHeader('Retry-After', retryAfter);
    return res.status(429).json({
      error: `Too many requests. Try again in ${Math.ceil(retryAfter / 60)} minutes.`
    });
  }

  entry.count++;
  return null; // OK
}

// ── 3. Payload size check ──
export function checkPayloadSize(req, res) {
  const contentLength = parseInt(req.headers['content-length'] || '0');
  if (contentLength > MAX_BODY_BYTES) {
    return res.status(413).json({ error: 'Payload too large' });
  }
  return null; // OK
}

// ── 4. HMAC signature verification ──
// The Liquid form signs: SHA256(timestamp + "." + body_json, HMAC_SECRET)
// Backend verifies this signature before processing anything
export function verifyHmac(req, res) {
  const SECRET    = process.env.HMAC_SECRET;
  const timestamp = req.headers['x-timestamp'];
  const signature = req.headers['x-signature'];

  if (!SECRET) {
    // If secret not configured, skip (dev mode) but log warning
    console.warn('⚠️  HMAC_SECRET not set — skipping signature verification');
    return null;
  }

  if (!timestamp || !signature) {
    return res.status(401).json({ error: 'Missing security headers' });
  }

  // ── Timestamp check — reject requests older than 5 minutes ──
  const ts = parseInt(timestamp);
  if (isNaN(ts) || Date.now() - ts > TIMESTAMP_TTL) {
    return res.status(401).json({ error: 'Request expired' });
  }

  // ── HMAC check ──
  const body    = JSON.stringify(req.body || {});
  const message = `${timestamp}.${body}`;
  const expected = crypto
    .createHmac('sha256', SECRET)
    .update(message)
    .digest('hex');

  // Timing-safe comparison prevents timing attacks
  const sigBuffer      = Buffer.from(signature, 'hex');
  const expectedBuffer = Buffer.from(expected,  'hex');

  if (
    sigBuffer.length !== expectedBuffer.length ||
    !crypto.timingSafeEqual(sigBuffer, expectedBuffer)
  ) {
    console.warn('⚠️  Invalid HMAC signature from IP:', req.headers['x-forwarded-for']);
    return res.status(401).json({ error: 'Invalid signature' });
  }

  return null; // OK
}

// ── 5. Run all security checks in order ──
// Returns a response if blocked, null if all checks pass
export function runSecurityChecks(req, res, { skipHmac = false } = {}) {
  setCorsHeaders(req, res);

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return true; // signals caller to return early
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

  return false; // all checks passed
}
