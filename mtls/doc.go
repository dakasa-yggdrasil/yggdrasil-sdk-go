// Package mtls loads a *tls.Config from a PKCS#12 bundle so adapter
// binaries can present a client certificate to upstream APIs (and
// optionally pin the same cert chain on an inbound listener) without
// each adapter reimplementing the load-and-decode dance.
//
// Three sources are supported:
//
//   - SourceFile     — read a P12 from a filesystem path
//     (typically a mounted Kubernetes Secret).
//   - SourceBase64   — decode a base64 string from env (useful for
//     single-tenant deployments that ship the cert
//     via plain Secret data).
//   - SourceDisabled — return (nil, nil); used by feature-flag-gated
//     mock modes.
//
// LoadFromEnv is the conventional entrypoint for adapter main()s:
// pass a prefix (e.g. "EFI"), and the package reads
// {PREFIX}_MTLS_ENABLED, {PREFIX}_CERTIFICATE, and
// {PREFIX}_CERTIFICATE_BASE64 to decide what to do.
package mtls
