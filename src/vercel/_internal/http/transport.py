"""Transitional aliases for HTTP transports now owned by internal core."""

from vercel.internal.core.http.transport import (
    AsyncTransport,
    BaseTransport,
    BytesBody,
    HeaderTypes,
    JSONBody,
    PrimitiveData,
    QueryParamTypes,
    RawBody,
    ReadResponsePolicy,
    RequestBody,
    StreamingRequest,
    StreamingResponse,
    SyncTransport,
    TransportOptions,
    extract_structured_error,
)

__all__ = [
    "AsyncTransport",
    "BaseTransport",
    "BytesBody",
    "HeaderTypes",
    "JSONBody",
    "PrimitiveData",
    "QueryParamTypes",
    "RawBody",
    "ReadResponsePolicy",
    "RequestBody",
    "StreamingRequest",
    "StreamingResponse",
    "SyncTransport",
    "TransportOptions",
    "extract_structured_error",
]
