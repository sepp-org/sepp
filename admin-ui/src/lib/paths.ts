// True when `path` is `p` or nested under it, for env_pinned / restart_only
// lists where an entry like "server" pins everything in that table.
export function matchesPath(list: string[], path: string): boolean {
  return list.some((p) => p === path || path.startsWith(p + '.'))
}
