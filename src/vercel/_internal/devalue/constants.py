"""Constants matching the JavaScript devalue library wire format."""

UNDEFINED = -1
HOLE = -2
NAN = -3
POSITIVE_INFINITY = -4
NEGATIVE_INFINITY = -5
NEGATIVE_ZERO = -6
SPARSE = -7

# The largest valid value for a JavaScript array's `length` property,
# and the largest valid array index (one less than the max length).
MAX_ARRAY_LEN = 2**32 - 1
MAX_ARRAY_INDEX = MAX_ARRAY_LEN - 1

# The largest exactly-representable integer in a JS `number`.  Python ints
# beyond this range are serialized as `["BigInt", …]` so that JS `parse`
# recovers the exact value instead of a rounded float.
MAX_SAFE_INTEGER = 2**53 - 1

# JS hydrates a sparse array into a dictionary-mode array, so a declared
# length costs nothing until the slots are populated.  Python lists are
# dense, so `[Undefined] * length` allocates eagerly and a tiny payload such
# as `[[-7,4294967295]]` would demand tens of gigabytes.  Reject declared
# lengths above this bound rather than trying to honour them.
MAX_SPARSE_ARRAY_LENGTH = 2**22
