`bytes` now crosses the wire as a `Uint8Array` instead of an `ArrayBuffer`, and
a JavaScript `ArrayBuffer` decodes to `bytearray` instead of `bytes`.
