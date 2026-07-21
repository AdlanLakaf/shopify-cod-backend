// ============================================================
//  Per-shop backup storage on a Railway Volume.
//
//  Each shop uploads ONE dump per day of its local ERP database.
//  Files land under BACKUP_DIR/shop_<id>/<YYYY-MM-DD>.dump.gz .
//
//  IMPORTANT: BACKUP_DIR must be a mounted Railway VOLUME. The
//  normal container filesystem is ephemeral — every deploy wipes
//  it — so without a volume, backups do not survive. Default path
//  is /data/backups; mount the volume at /data.
//
//  Retention: a hard total cap (BACKUP_MAX_BYTES, default 1 GB)
//  across all shops. When a new upload finalizes, the oldest
//  dumps are deleted until the total is back under the cap, so
//  storage can never run away. One dump per shop per day (a repeat
//  same-day upload overwrites).
//
//  Upload is chunked so a large dump never has to fit in one
//  request body: parts append to a .part file; `done` renames it
//  into place and runs retention.
// ============================================================

import fs from 'fs';
import path from 'path';

const DIR = process.env.BACKUP_DIR || '/data/backups';
const MAX_BYTES = Math.max(Number(process.env.BACKUP_MAX_BYTES) || 1024 * 1024 * 1024, 1024 * 1024);

const safeDay = d => (/^\d{4}-\d{2}-\d{2}$/.test(String(d)) ? String(d) : null);
const shopDir = shopId => path.join(DIR, `shop_${Number(shopId)}`);

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

/**
 * Append one chunk of a shop's daily dump. Parts must arrive in order
 * (part 0 first); part 0 truncates any half-written previous attempt.
 * When `done`, the temp file is atomically renamed into place and
 * retention runs. Returns { ok, bytes } or { ok:false, error }.
 */
export function appendBackupChunk({ shopId, day, part, dataB64, done }) {
  const d = safeDay(day);
  if (!Number(shopId) || !d) return { ok: false, error: 'shopId and a valid day are required' };
  try {
    const dir = shopDir(shopId);
    ensureDir(dir);
    const tmp = path.join(dir, `${d}.dump.gz.part`);
    const final = path.join(dir, `${d}.dump.gz`);

    if (dataB64) {
      const buf = Buffer.from(String(dataB64), 'base64');
      // part 0 starts a fresh file; later parts append.
      if (Number(part) === 0) fs.writeFileSync(tmp, buf);
      else fs.appendFileSync(tmp, buf);
    }

    if (done) {
      if (fs.existsSync(tmp)) fs.renameSync(tmp, final);      // atomic replace
      const evicted = enforceRetention();
      const bytes = fs.existsSync(final) ? fs.statSync(final).size : 0;
      return { ok: true, bytes, evicted, totalBytes: totalSize() };
    }
    return { ok: true, bytes: fs.existsSync(tmp) ? fs.statSync(tmp).size : 0 };
  } catch (err) {
    console.error('[backup] appendBackupChunk failed:', err.message);
    return { ok: false, error: err.message };
  }
}

/** Every stored dump, newest first: { shopId, day, bytes, mtime }. */
export function listBackups() {
  const out = [];
  try {
    if (!fs.existsSync(DIR)) return out;
    for (const shop of fs.readdirSync(DIR)) {
      const m = /^shop_(\d+)$/.exec(shop);
      if (!m) continue;
      const dir = path.join(DIR, shop);
      for (const f of fs.readdirSync(dir)) {
        const dm = /^(\d{4}-\d{2}-\d{2})\.dump\.gz$/.exec(f);
        if (!dm) continue;
        const st = fs.statSync(path.join(dir, f));
        out.push({ shopId: Number(m[1]), day: dm[1], bytes: st.size, mtime: st.mtimeMs });
      }
    }
  } catch (err) {
    console.error('[backup] listBackups failed:', err.message);
  }
  return out.sort((a, b) => b.mtime - a.mtime);
}

/** Absolute path of one dump, for download — or null if it doesn't exist. */
export function backupPath(shopId, day) {
  const d = safeDay(day);
  if (!Number(shopId) || !d) return null;
  const f = path.join(shopDir(shopId), `${d}.dump.gz`);
  return fs.existsSync(f) ? f : null;
}

function totalSize() {
  return listBackups().reduce((s, b) => s + b.bytes, 0);
}

/** Delete oldest dumps until the total is under the cap. Returns count removed. */
export function enforceRetention() {
  const all = listBackups().sort((a, b) => a.mtime - b.mtime);   // oldest first
  let total = all.reduce((s, b) => s + b.bytes, 0);
  let removed = 0;
  for (const b of all) {
    if (total <= MAX_BYTES) break;
    try {
      fs.unlinkSync(path.join(shopDir(b.shopId), `${b.day}.dump.gz`));
      total -= b.bytes;
      removed++;
      console.log(`[backup] evicted shop ${b.shopId} ${b.day} (${b.bytes} bytes) to stay under cap`);
    } catch { /* already gone */ }
  }
  return removed;
}

export const BACKUP_INFO = { dir: DIR, maxBytes: MAX_BYTES };
