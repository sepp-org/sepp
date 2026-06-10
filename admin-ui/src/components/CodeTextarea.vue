<script setup lang="ts">
import {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
  completionKeymap,
} from '@codemirror/autocomplete'
import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands'
import { json, jsonParseLinter } from '@codemirror/lang-json'
import {
  HighlightStyle,
  bracketMatching,
  indentOnInput,
  syntaxHighlighting,
} from '@codemirror/language'
import { lintGutter, linter } from '@codemirror/lint'
import { Compartment, EditorState, type Extension } from '@codemirror/state'
import { EditorView, keymap, lineNumbers, placeholder as cmPlaceholder } from '@codemirror/view'
import { tags } from '@lezer/highlight'
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = withDefaults(
  defineProps<{
    modelValue: string
    highlight?: 'json' | 'none'
    placeholder?: string
  }>(),
  { highlight: 'none', placeholder: '' },
)

const emit = defineEmits<{ 'update:modelValue': [value: string] }>()

const host = ref<HTMLDivElement | null>(null)
let view: EditorView | null = null
const lang = new Compartment()
const hint = new Compartment()

// Same token colors as lib/jsonHighlight.ts (the read-only JsonView), so the
// editor and the payload inspectors look like one system.
const palette = HighlightStyle.define([
  { tag: tags.propertyName, color: '#f0976b' },
  { tag: tags.string, color: '#a9c79b' },
  { tag: tags.number, color: '#8fb8e0' },
  { tag: [tags.bool, tags.null], color: '#d9b66f' },
  { tag: [tags.punctuation, tags.brace, tags.squareBracket], color: '#ab9c8c' },
])

const theme = EditorView.theme(
  {
    '&': {
      height: '176px',
      backgroundColor: 'var(--color-ink-950)',
      border: '1px solid var(--color-ink-700)',
      borderRadius: '0.25rem',
      fontSize: '0.875rem',
    },
    '&.cm-focused': { outline: 'none', borderColor: 'var(--color-accent)' },
    '.cm-scroller': {
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
      lineHeight: '1.5',
    },
    '.cm-content': { caretColor: 'var(--color-ink-100)' },
    '.cm-cursor': { borderLeftColor: 'var(--color-ink-100)' },
    '.cm-gutters': {
      backgroundColor: 'var(--color-ink-900)',
      color: 'var(--color-ink-500)',
      border: 'none',
      borderRight: '1px solid var(--color-ink-800)',
    },
    '.cm-activeLine': { backgroundColor: 'rgba(42, 36, 30, 0.45)' },
    '.cm-activeLineGutter': {
      backgroundColor: 'rgba(42, 36, 30, 0.7)',
      color: 'var(--color-ink-300)',
    },
    '&.cm-focused .cm-selectionBackground, .cm-selectionBackground': {
      backgroundColor: 'rgba(236, 106, 46, 0.25) !important',
    },
    '.cm-placeholder': { color: 'var(--color-ink-600)' },
    '.cm-tooltip': {
      backgroundColor: 'var(--color-ink-800)',
      border: '1px solid var(--color-ink-700)',
      color: 'var(--color-ink-100)',
    },
    '.cm-tooltip.cm-tooltip-autocomplete > ul > li[aria-selected]': {
      backgroundColor: 'var(--color-ink-700)',
      color: 'var(--color-ink-100)',
    },
    '.cm-lintRange-error': {
      backgroundImage: 'none',
      textDecoration: 'underline wavy #f87171 1px',
    },
  },
  { dark: true },
)

// An empty payload is valid (it means "no payload"), so do not flag it.
const jsonLint = jsonParseLinter()
const lintNonEmpty = linter((v) => (v.state.doc.toString().trim() === '' ? [] : jsonLint(v)))

function langExtensions(mode: 'json' | 'none'): Extension {
  return mode === 'json'
    ? [json(), lintNonEmpty, lintGutter(), bracketMatching(), closeBrackets(), autocompletion()]
    : []
}

onMounted(() => {
  if (!host.value) return
  view = new EditorView({
    parent: host.value,
    state: EditorState.create({
      doc: props.modelValue,
      extensions: [
        lineNumbers(),
        history(),
        indentOnInput(),
        EditorView.lineWrapping,
        keymap.of([
          ...closeBracketsKeymap,
          ...defaultKeymap,
          ...historyKeymap,
          ...completionKeymap,
          indentWithTab,
        ]),
        syntaxHighlighting(palette),
        theme,
        lang.of(langExtensions(props.highlight)),
        hint.of(cmPlaceholder(props.placeholder)),
        EditorView.updateListener.of((u) => {
          if (u.docChanged) emit('update:modelValue', u.state.doc.toString())
        }),
      ],
    }),
  })
})

watch(
  () => props.highlight,
  (mode) => view?.dispatch({ effects: lang.reconfigure(langExtensions(mode)) }),
)

watch(
  () => props.placeholder,
  (p) => view?.dispatch({ effects: hint.reconfigure(cmPlaceholder(p)) }),
)

watch(
  () => props.modelValue,
  (v) => {
    if (view && v !== view.state.doc.toString()) {
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: v } })
    }
  },
)

onBeforeUnmount(() => {
  view?.destroy()
  view = null
})
</script>

<template>
  <div ref="host"></div>
</template>
