Stop nesting retry wrappers in delivery lifecycle settlement: the RetryAfter
and ACK paths wrapped the already-retrying `extend_lease()`/`acknowledge()`
client methods in a second retry layer, multiplying retry attempts and
logging duplicate `visibility.retry_attempt` events per request.
