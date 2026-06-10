<script setup lang="ts">
import { computed } from 'vue'
import type { JsonValue } from '../api/types'
import JsonView from './JsonView.vue'

const props = defineProps<{
  encoding: string
  sizeBytes: number
  dataB64?: string
  downloadName?: string
}>()

const HEX_CAP = 4096

function b64ToBytes(b64: string) {
  try {
    const bin = atob(b64)
    const out = new Uint8Array(bin.length)
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
    return out
  } catch {
    return null
  }
}

function utf8(bytes: Uint8Array): string | null {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes)
  } catch {
    return null
  }
}

function hexDump(b: Uint8Array): string[] {
  const lines: string[] = []
  const n = Math.min(b.length, HEX_CAP)
  for (let off = 0; off < n; off += 16) {
    const row = b.subarray(off, Math.min(off + 16, n))
    const hex = Array.from(row, (x) => x.toString(16).padStart(2, '0')).join(' ')
    const ascii = Array.from(row, (x) =>
      x >= 0x20 && x < 0x7f ? String.fromCharCode(x) : '.',
    ).join('')
    lines.push(`${off.toString(16).padStart(8, '0')}  ${hex.padEnd(47)}  ${ascii}`)
  }
  return lines
}

const bytes = computed(() => (props.dataB64 !== undefined ? b64ToBytes(props.dataB64) : null))

type Rendered =
  | { kind: 'json'; json: JsonValue }
  | { kind: 'text'; text: string }
  | { kind: 'hex'; lines: string[]; capped: boolean }
  | null

const rendered = computed<Rendered>(() => {
  const b = bytes.value
  if (!b) return null
  const enc = props.encoding.toLowerCase()
  if (enc.includes('json')) {
    const t = utf8(b)
    if (t !== null) {
      try {
        return { kind: 'json', json: JSON.parse(t) as JsonValue }
      } catch {
        return { kind: 'text', text: t }
      }
    }
  } else if (enc === '' || enc.includes('text') || enc.includes('utf') || enc.includes('plain')) {
    const t = utf8(b)
    if (t !== null) return { kind: 'text', text: t }
  }
  return { kind: 'hex', lines: hexDump(b), capped: b.length > HEX_CAP }
})

function download() {
  const b = bytes.value
  if (!b) return
  const url = URL.createObjectURL(new Blob([b]))
  const a = document.createElement('a')
  a.href = url
  a.download = props.downloadName ?? 'payload.bin'
  a.click()
  URL.revokeObjectURL(url)
}
</script>

<template>
  <p v-if="sizeBytes === 0" class="text-sm text-ink-400">
    {{ encoding ? `Empty payload (${encoding}).` : 'No payload.' }}
  </p>
  <div v-else class="flex flex-col gap-2">
    <div class="flex items-center gap-3 text-xs text-ink-400">
      <span class="font-mono">{{ encoding || 'no encoding' }}</span>
      <span>{{ sizeBytes }} bytes</span>
      <button v-if="bytes" type="button" class="text-ink-300 hover:text-ink-100" @click="download">
        Download
      </button>
    </div>

    <JsonView v-if="rendered && rendered.kind === 'json'" :value="rendered.json" />
    <pre
      v-else-if="rendered && rendered.kind === 'text'"
      class="overflow-x-auto rounded border border-ink-800 bg-ink-950 p-3 font-mono text-xs leading-relaxed text-ink-200"
    >{{ rendered.text }}</pre>
    <template v-else-if="rendered && rendered.kind === 'hex'">
      <pre
        class="overflow-x-auto rounded border border-ink-800 bg-ink-950 p-3 font-mono text-xs leading-relaxed text-ink-200"
      >{{ rendered.lines.join('\n') }}</pre>
      <p v-if="rendered.capped" class="text-xs text-ink-500">
        Showing first {{ HEX_CAP }} of {{ sizeBytes }} bytes; download for the rest.
      </p>
    </template>
    <p v-else-if="dataB64 === undefined" class="text-sm text-ink-400">Payload not inlined.</p>
    <p v-else class="text-sm text-red-400">Failed to decode payload.</p>
  </div>
</template>
