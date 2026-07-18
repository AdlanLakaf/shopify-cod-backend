// ============================================================
//  Shared Postgres pool — ONE pool for the whole process.
//
//  Every _*-db.js module imports { getPool } from here. They used
//  to own a pool each (5 pools × max 2–4 = up to 17 connections
//  against one small Railway Postgres); now the whole process
//  shares this single pool. max 8 replaces the old combined total.
//
//  With no DATABASE_URL set, getPool() returns null and callers
//  are expected to no-op (same contract as the legacy modules).
// ============================================================

import pg from 'pg';

let pool = null;

export function getPool() {
  const url = process.env.DATABASE_URL;
  if (!url) return null;
  if (pool) return pool;

  // Railway's internal network (*.railway.internal) speaks plain TCP; the
  // public proxy host needs SSL. Allow an explicit override either way.
  const wantSsl =
    process.env.DATABASE_SSL === 'true' ||
    (!/railway\.internal/.test(url) && /\b(sslmode=require|proxy\.rlwy\.net|\.railway\.app)\b/.test(url));

  pool = new pg.Pool({
    connectionString: url,
    ssl: wantSsl ? { rejectUnauthorized: false } : undefined,
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });
  pool.on('error', err => console.error('[pg] idle client error:', err.message));
  return pool;
}
