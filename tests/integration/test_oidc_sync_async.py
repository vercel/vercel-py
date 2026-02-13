"""Integration tests for Vercel OIDC module.

Tests token retrieval from context, environment, and JWT payload decoding.
"""

import base64
import json

import pytest


class TestOidcTokenFromContext:
    """Test OIDC token retrieval from request context."""

    def test_get_token_from_header_context(self, mock_env_clear):
        """Test getting OIDC token from request headers."""
        from vercel.cache.context import set_headers
        from vercel.oidc import get_vercel_oidc_token_sync

        # Set headers via context
        set_headers({"x-vercel-oidc-token": "test_oidc_token_from_header"})

        try:
            token = get_vercel_oidc_token_sync()
            assert token == "test_oidc_token_from_header"
        finally:
            # Clear headers
            set_headers(None)

    def test_get_token_from_env_variable(self, mock_env_clear, monkeypatch):
        """Test getting OIDC token from environment variable."""
        from vercel.oidc import get_vercel_oidc_token_sync

        monkeypatch.setenv("VERCEL_OIDC_TOKEN", "test_oidc_token_from_env")

        token = get_vercel_oidc_token_sync()
        assert token == "test_oidc_token_from_env"

    def test_missing_token_raises_error(self, mock_env_clear):
        """Test that missing token raises VercelOidcTokenError."""
        from vercel.oidc import VercelOidcTokenError, get_vercel_oidc_token_sync

        with pytest.raises(VercelOidcTokenError) as exc_info:
            get_vercel_oidc_token_sync()

        assert "x-vercel-oidc-token" in str(exc_info.value)

    def test_header_takes_precedence_over_env(self, mock_env_clear, monkeypatch):
        """Test that header takes precedence over environment variable."""
        from vercel.cache.context import set_headers
        from vercel.oidc import get_vercel_oidc_token_sync

        monkeypatch.setenv("VERCEL_OIDC_TOKEN", "env_token")
        set_headers({"x-vercel-oidc-token": "header_token"})

        try:
            token = get_vercel_oidc_token_sync()
            assert token == "header_token"
        finally:
            set_headers(None)


class TestDecodeOidcPayload:
    """Test JWT payload decoding."""

    def test_decode_valid_jwt_payload(self, mock_env_clear):
        """Test decoding a valid JWT token payload."""
        from vercel.oidc import decode_oidc_payload

        # Create a valid JWT-like token with a base64url encoded payload
        payload = {
            "sub": "test_subject",
            "aud": "vercel",
            "iss": "https://oidc.vercel.com",
            "exp": 9999999999,
            "project_id": "prj_test123",
            "owner_id": "team_test456",
        }
        # Base64url encode the payload
        payload_json = json.dumps(payload)
        payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode().rstrip("=")

        # Create a mock JWT (header.payload.signature)
        mock_token = f"header.{payload_b64}.signature"

        decoded = decode_oidc_payload(mock_token)

        assert decoded["sub"] == "test_subject"
        assert decoded["aud"] == "vercel"
        assert decoded["iss"] == "https://oidc.vercel.com"
        assert decoded["project_id"] == "prj_test123"
        assert decoded["owner_id"] == "team_test456"

    def test_decode_payload_with_padding(self, mock_env_clear):
        """Test decoding payload that requires base64 padding."""
        from vercel.oidc import decode_oidc_payload

        payload = {"short": "data"}
        payload_json = json.dumps(payload)
        payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode().rstrip("=")

        mock_token = f"header.{payload_b64}.signature"

        decoded = decode_oidc_payload(mock_token)
        assert decoded["short"] == "data"


class TestGetCredentials:
    """Test get_credentials function."""

    def test_get_credentials_with_explicit_values(self, mock_env_clear):
        """Test getting credentials with explicitly provided values."""
        from vercel.oidc import Credentials, get_credentials

        creds = get_credentials(
            token="explicit_token",
            project_id="prj_explicit",
            team_id="team_explicit",
        )

        assert isinstance(creds, Credentials)
        assert creds.token == "explicit_token"
        assert creds.project_id == "prj_explicit"
        assert creds.team_id == "team_explicit"

    def test_get_credentials_from_env(self, mock_env_clear, monkeypatch):
        """Test getting credentials from environment variables."""
        from vercel.oidc import get_credentials

        monkeypatch.setenv("VERCEL_TOKEN", "env_token")
        monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_from_env")
        monkeypatch.setenv("VERCEL_TEAM_ID", "team_from_env")

        creds = get_credentials()

        assert creds.token == "env_token"
        assert creds.project_id == "prj_from_env"
        assert creds.team_id == "team_from_env"

    def test_get_credentials_missing_raises_error(self, mock_env_clear):
        """Test that missing credentials raises RuntimeError."""
        from vercel.oidc import get_credentials

        with pytest.raises(RuntimeError) as exc_info:
            get_credentials()

        assert "Missing credentials" in str(exc_info.value)

    def test_get_credentials_partial_explicit_with_env(self, mock_env_clear, monkeypatch):
        """Test partial explicit credentials filled from env."""
        from vercel.oidc import get_credentials

        monkeypatch.setenv("VERCEL_TOKEN", "env_token")
        monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_from_env")
        monkeypatch.setenv("VERCEL_TEAM_ID", "team_from_env")

        # Only provide token explicitly
        creds = get_credentials(token="explicit_only_token")

        # Should use explicit token but env for project_id and team_id
        assert creds.token == "explicit_only_token"
        assert creds.project_id == "prj_from_env"
        assert creds.team_id == "team_from_env"

    def test_get_credentials_from_oidc_token(self, mock_env_clear, monkeypatch):
        """Test getting credentials from OIDC token payload."""
        from vercel.oidc import get_credentials

        # Create a valid OIDC token with embedded project and team info
        payload = {
            "project_id": "prj_oidc_embedded",
            "owner_id": "team_oidc_embedded",
            "exp": 9999999999,
        }
        payload_json = json.dumps(payload)
        payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode().rstrip("=")
        oidc_token = f"header.{payload_b64}.signature"

        monkeypatch.setenv("VERCEL_OIDC_TOKEN", oidc_token)

        creds = get_credentials()

        assert creds.token == oidc_token
        assert creds.project_id == "prj_oidc_embedded"
        assert creds.team_id == "team_oidc_embedded"

    def test_get_credentials_oidc_with_env_project_team(self, mock_env_clear, monkeypatch):
        """Test OIDC token with project/team from env vars."""
        from vercel.oidc import get_credentials

        # OIDC token without embedded project/team
        payload = {"sub": "test", "exp": 9999999999}
        payload_json = json.dumps(payload)
        payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode().rstrip("=")
        oidc_token = f"header.{payload_b64}.signature"

        monkeypatch.setenv("VERCEL_OIDC_TOKEN", oidc_token)
        monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_env_override")
        monkeypatch.setenv("VERCEL_TEAM_ID", "team_env_override")

        creds = get_credentials()

        assert creds.token == oidc_token
        assert creds.project_id == "prj_env_override"
        assert creds.team_id == "team_env_override"


class TestCredentialsDataclass:
    """Test Credentials dataclass."""

    def test_credentials_fields(self, mock_env_clear):
        """Test Credentials dataclass has expected fields."""
        from vercel.oidc import Credentials

        creds = Credentials(
            token="test_token",
            project_id="prj_test",
            team_id="team_test",
        )

        assert creds.token == "test_token"
        assert creds.project_id == "prj_test"
        assert creds.team_id == "team_test"

    def test_credentials_equality(self, mock_env_clear):
        """Test Credentials equality comparison."""
        from vercel.oidc import Credentials

        creds1 = Credentials(token="tok", project_id="prj", team_id="team")
        creds2 = Credentials(token="tok", project_id="prj", team_id="team")
        creds3 = Credentials(token="different", project_id="prj", team_id="team")

        assert creds1 == creds2
        assert creds1 != creds3


class TestVercelOidcTokenError:
    """Test VercelOidcTokenError exception."""

    def test_error_message(self, mock_env_clear):
        """Test error message formatting."""
        from vercel.oidc import VercelOidcTokenError

        error = VercelOidcTokenError("Test error message")
        assert str(error) == "Test error message"
        assert error.cause is None

    def test_error_with_cause(self, mock_env_clear):
        """Test error with cause exception."""
        from vercel.oidc import VercelOidcTokenError

        cause = ValueError("Original error")
        error = VercelOidcTokenError("Wrapped error", cause)

        assert "Wrapped error" in str(error)
        assert "Original error" in str(error)
        assert error.cause is cause
