Make `SerializationError` inherit from `FatalError` instead of `RuntimeError`, so serialization failures fail steps without retrying.
