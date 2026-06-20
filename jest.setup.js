// jsdom doesn't expose structuredClone, which @dagrejs/dagre uses (the "Tidy
// layout" feature in src/layout.ts). Real browsers have it, so this is a
// test-environment-only gap. v8 serialize/deserialize matches its
// structured-clone semantics (handles nested objects and cycles). Guarded, so
// it's a no-op where the environment already provides it.
const v8 = require('v8');

if (typeof globalThis.structuredClone === 'undefined') {
  globalThis.structuredClone = value => v8.deserialize(v8.serialize(value));
}
