function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;')
}

// One pass: object keys (string followed by a colon), strings, literals,
// numbers. The closing quote is optional so a string still mid-typing
// highlights instead of flickering; newlines bound it like JSON does.
const TOKEN =
  /("(?:\\.|[^"\\\n])*"?)(\s*:)|("(?:\\.|[^"\\\n])*"?)|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g

// Escaped HTML with tok-* spans (colors in style.css). Safe for v-html: all
// input text passes through esc().
export function jsonToHtml(text: string): string {
  let out = ''
  let last = 0
  for (const m of text.matchAll(TOKEN)) {
    const at = m.index ?? 0
    out += esc(text.slice(last, at))
    if (m[1] !== undefined) out += `<span class="tok-key">${esc(m[1])}</span>${esc(m[2] ?? '')}`
    else if (m[3] !== undefined) out += `<span class="tok-str">${esc(m[3])}</span>`
    else if (m[4] !== undefined) out += `<span class="tok-lit">${esc(m[4])}</span>`
    else if (m[5] !== undefined) out += `<span class="tok-num">${esc(m[5])}</span>`
    last = at + m[0].length
  }
  return out + esc(text.slice(last))
}
