Add origin-scoped provider HTTP connections

This component is shared across provider services. Error construction and translation live in error.rs; validation lives in validation.rs.

HTTP response errors retain `Error::http_status()` and `Error::retry_after()`. The latter is the original textual header (delta-seconds or HTTP-date), including malformed values for caller inspection. Missing/non-text headers yield None. Callers own parsing, pacing and retry decisions; no automatic request replay is added.
