### Added
- Add per-worker slowdown on consecutive retryable HTTP errors from the external endpoint to avoid hammering a degraded service.
- Send non-retryable HTTP errors to an optional poison queue for manual inspection or reprocessing.
