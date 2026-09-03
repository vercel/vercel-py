"""Test-only encoder for constructing plain workflow payloads."""

from vercel.workflow._internal import serialization

PLAIN_ENCODER = serialization.PayloadEncoder()
