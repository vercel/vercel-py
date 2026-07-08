from __future__ import annotations

import platform
import sys

from ..version import __version__ as _ver

CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_NDJSON = "application/x-ndjson"
CONTENT_TYPE_OCTET_STREAM = "application/octet-stream"
CONTENT_TYPE_TEXT = "text/plain; charset=utf-8"
CONTENT_TYPE_MULTIPART_MIXED = "multipart/mixed"

HEADER_ACCEPT = "Accept"
HEADER_AUTHORIZATION = "Authorization"
HEADER_CONTENT_TYPE = "Content-Type"
HEADER_RETRY_AFTER = "Retry-After"
HEADER_USER_AGENT = "User-Agent"

VQS_NAME_PATTERN = r"^[A-Za-z0-9_-]+$"

DEFAULT_RETRY_AFTER_SECONDS = 60

VQS_HEADER_DELAY_SECONDS = "Vqs-Delay-Seconds"
VQS_HEADER_CLIENT_TS = "Vqs-Client-Ts"
VQS_HEADER_DELIVERY_COUNT = "Vqs-Delivery-Count"
VQS_HEADER_DEPLOYMENT_ID = "Vqs-Deployment-Id"
VQS_HEADER_EXPIRES_AT = "Vqs-Expires-At"
VQS_HEADER_IDEMPOTENCY_KEY = "Vqs-Idempotency-Key"
VQS_HEADER_MAX_MESSAGES = "Vqs-Max-Messages"
VQS_HEADER_MESSAGE_ID = "Vqs-Message-Id"
VQS_HEADER_RECEIPT_HANDLE = "Vqs-Receipt-Handle"
VQS_HEADER_RETENTION_SECONDS = "Vqs-Retention-Seconds"
VQS_HEADER_TIMESTAMP = "Vqs-Timestamp"
VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS = "Vqs-Visibility-Timeout-Seconds"

CLOUD_EVENT_TYPE_V2BETA = "com.vercel.queue.v2beta"
CLOUD_EVENT_HEADER_TYPE = "ce-type"
CLOUD_EVENT_HEADER_VQS_CONSUMER_GROUP = "ce-vqsconsumergroup"
CLOUD_EVENT_HEADER_VQS_CREATED_AT = "ce-vqscreatedat"
CLOUD_EVENT_HEADER_VQS_DELIVERY_COUNT = "ce-vqsdeliverycount"
CLOUD_EVENT_HEADER_VQS_EXPIRES_AT = "ce-vqsexpiresat"
CLOUD_EVENT_HEADER_VQS_MESSAGE_ID = "ce-vqsmessageid"
CLOUD_EVENT_HEADER_VQS_TOPIC = "ce-vqsqueuename"
CLOUD_EVENT_HEADER_VQS_RECEIPT_HANDLE = "ce-vqsreceipthandle"
CLOUD_EVENT_HEADER_VQS_REGION = "ce-vqsregion"
CLOUD_EVENT_HEADER_VQS_VISIBILITY_DEADLINE = "ce-vqsvisibilitydeadline"

PLATFORM = platform.uname()
USER_AGENT = f"vercel/queue/{_ver} (Python/{sys.version}; {PLATFORM.system}/{PLATFORM.machine})"
