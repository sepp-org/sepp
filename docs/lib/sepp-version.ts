import { readFileSync } from 'node:fs';
import { join } from 'node:path';

// Read the Sepp crate version from the root Cargo.toml at build time.
// `docs/` sits at the repo root, so go up one and pick the first `version = "..."`
// under the `[package]` block.
function readSeppVersion(): string {
  try {
    const toml = readFileSync(join(process.cwd(), '..', 'Cargo.toml'), 'utf8');
    const match = toml.match(/^\s*version\s*=\s*"([^"]+)"/m);
    return match?.[1] ?? '0.0.0';
  } catch {
    return '0.0.0';
  }
}

export const SEPP_VERSION = readSeppVersion();
