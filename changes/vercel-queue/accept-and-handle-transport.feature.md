`accept_and_handle()` now takes a `transport`, so a caller that knows the wire
format of a delivery can supply it instead of relying on the codec inferred
from the subscriber's payload annotation — which cannot express formats such as
CBOR. The `Transport` protocol is exported to go with it.
