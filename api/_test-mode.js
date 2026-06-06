// Validates the staff token and extracts test mode options from the request body.
// Returns null if the token is missing or invalid (treat as normal production request).
export function getTestMode(body) {
  const STAFF_TOKEN = process.env.STAFF_TOKEN;
  if (!STAFF_TOKEN || !body.staffToken || body.staffToken !== STAFF_TOKEN) return null;

  const allow = (val, options, fallback) => options.includes(val) ? val : fallback;

  return {
    orderMode:      allow(body.testOrderMode,  ['mock', 'draft', 'full'],          'mock'),
    metaMode:       allow(body.testMetaMode,   ['skip', 'test', 'real'],           'skip'),
    tiktokMode:     allow(body.testTiktokMode, ['skip', 'test', 'real'],           'skip'),
    ga4Mode:        allow(body.testGa4Mode,    ['skip', 'real'],                   'skip'),
    metaTestCode:   String(body.metaTestCode   || '').slice(0, 30),
    tiktokTestCode: String(body.tiktokTestCode || '').slice(0, 30),
  };
}
