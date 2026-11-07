# SSEGA.5 decoder hardening — 2026-11-07

Usable legacy decoder maintenance, separate from experimental Fetch v2 work.

1. Delivery correctness review: permissive Node base64 decoding plus Flechette's
   schema fallback could interpret malformed bytes as an empty successful event.
   Reject malformed/oversized base64, missing modern stream framing, missing schema
   and payloads above16MiB before allowing callbacks to acknowledge an event.
   Independent Arrow regressions cover truncation, absent schema, invalid encoding,
   oversize and a valid zero-row schema. Existing exact numeric tests remain green.
2. Compatibility review: server events already use a complete modern Arrow IPC
   stream with continuation marker and end-of-stream marker. Valid empty schemas
   remain accepted. The public decoder explicitly accepts IPC streams; this change
   does not qualify IPC files, compressed messages or every Arrow type. Flechette
   stays2.7.1 (npm latest stable rechecked2026-11-07); no library replacement needed.
3. Evidence/security review:15 unit/interop cases pass on macOS. Real TLS transport
   and all three actual browser engines pass for the legacy client and candidate
   Fetch client. The decoder check is transport framing, not a complete hostile
   FlatBuffers verifier. Candidate server native/security tests remain pending;
   no SSE GA or npm publication claim follows from this maintenance merge.
