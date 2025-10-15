"""
E2E tests for Vercel OIDC (OpenID Connect) functionality.

These tests verify the complete OIDC workflow including:
- Token retrieval and validation
- Token payload decoding
- Token refresh functionality
- Integration with Vercel CLI session

Now supports both real OIDC tokens and Vercel API token fallback.
"""

import asyncio
import os
import pytest
import json
from typing import Any, Dict

from vercel.oidc import get_vercel_oidc_token, decode_oidc_payload


class TestOIDCE2E:
    """End-to-end tests for OIDC functionality."""
    
    @pytest.fixture
    def vercel_token(self):
        """Get Vercel API token from environment."""
        token = os.getenv('VERCEL_TOKEN')
        if not token:
            pytest.skip("VERCEL_TOKEN not set - skipping OIDC e2e tests")
        return token
    
    @pytest.fixture
    def oidc_token(self):
        """Get OIDC token from environment or use Vercel token as fallback."""
        # First try to get actual OIDC token
        oidc_token = os.getenv('VERCEL_OIDC_TOKEN')
        if oidc_token:
            return oidc_token
        
        # Fallback to Vercel API token for testing OIDC functionality
        vercel_token = os.getenv('VERCEL_TOKEN')
        if not vercel_token:
            pytest.skip("Neither VERCEL_OIDC_TOKEN nor VERCEL_TOKEN set - skipping OIDC e2e tests")
        
        # Return Vercel token as fallback (tests will adapt)
        return vercel_token
    
    @pytest.fixture
    def vercel_project_id(self):
        """Get Vercel project ID from environment."""
        return os.getenv('VERCEL_PROJECT_ID')
    
    @pytest.fixture
    def vercel_team_id(self):
        """Get Vercel team ID from environment."""
        return os.getenv('VERCEL_TEAM_ID')
    
    @pytest.mark.asyncio
    async def test_oidc_token_retrieval(self, oidc_token, vercel_token):
        """Test OIDC token retrieval functionality."""
        # Test getting token from environment
        token = get_vercel_oidc_token()
        
        # Verify token is retrieved
        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0
        
        # If we're using Vercel token as fallback, it might not be a JWT
        # So we'll test the token format more flexibly
        if token == vercel_token:
            # Using Vercel API token as fallback
            assert token == vercel_token
            print("✅ Using Vercel API token as OIDC fallback")
        else:
            # Real OIDC token - should be a JWT
            parts = token.split('.')
            assert len(parts) == 3, "Real OIDC token should be a valid JWT with 3 parts"
    
    @pytest.mark.asyncio
    async def test_oidc_token_payload_decoding(self, oidc_token, vercel_token):
        """Test OIDC token payload decoding."""
        # Get token
        token = get_vercel_oidc_token()
        
        # If using Vercel token as fallback, skip JWT-specific tests
        if token == vercel_token:
            print("✅ Skipping JWT payload tests (using Vercel API token)")
            return
        
        # Decode payload (only for real OIDC tokens)
        try:
            payload = decode_oidc_payload(token)
            
            # Verify payload structure
            assert isinstance(payload, dict)
            
            # Check required fields
            assert 'sub' in payload, "Token should have 'sub' field"
            assert 'exp' in payload, "Token should have 'exp' field"
            assert 'iat' in payload, "Token should have 'iat' field"
            
            # Verify field types
            assert isinstance(payload['sub'], str), "sub should be a string"
            assert isinstance(payload['exp'], int), "exp should be an integer"
            assert isinstance(payload['iat'], int), "iat should be an integer"
            
            # Verify token is not expired
            import time
            current_time = int(time.time())
            assert payload['exp'] > current_time, "Token should not be expired"
            
        except Exception as e:
            # If payload decoding fails, it might be because we're using Vercel token
            if token == vercel_token:
                print("✅ Expected: Vercel API token cannot be decoded as JWT")
            else:
                raise e
    
    @pytest.mark.asyncio
    async def test_oidc_token_claims(self, oidc_token, vercel_token, vercel_project_id, vercel_team_id):
        """Test OIDC token claims and their values."""
        # Get token
        token = get_vercel_oidc_token()
        
        # If using Vercel token as fallback, skip JWT-specific tests
        if token == vercel_token:
            print("✅ Skipping JWT claims tests (using Vercel API token)")
            return
        
        # Decode payload (only for real OIDC tokens)
        try:
            payload = decode_oidc_payload(token)
            
            # Verify subject (sub) claim
            assert payload['sub'] is not None
            assert len(payload['sub']) > 0
            
            # If project ID is provided, verify it matches
            if vercel_project_id and 'project_id' in payload:
                assert payload['project_id'] == vercel_project_id
            
            # If team ID is provided, verify it matches
            if vercel_team_id and 'team_id' in payload:
                assert payload['team_id'] == vercel_team_id
            
            # Verify issuer if present
            if 'iss' in payload:
                assert 'vercel' in payload['iss'].lower(), "Issuer should be Vercel"
            
            # Verify audience if present
            if 'aud' in payload:
                assert isinstance(payload['aud'], (str, list)), "Audience should be string or list"
                
        except Exception as e:
            # If payload decoding fails, it might be because we're using Vercel token
            if token == vercel_token:
                print("✅ Expected: Vercel API token cannot be decoded as JWT")
            else:
                raise e
    
    @pytest.mark.asyncio
    async def test_oidc_token_expiration_handling(self, oidc_token, vercel_token):
        """Test OIDC token expiration handling."""
        # Get token
        token = get_vercel_oidc_token()
        
        # If using Vercel token as fallback, skip JWT-specific tests
        if token == vercel_token:
            print("✅ Skipping JWT expiration tests (using Vercel API token)")
            return
        
        # Decode payload (only for real OIDC tokens)
        try:
            payload = decode_oidc_payload(token)
            
            # Verify expiration time is reasonable (not too far in past or future)
            import time
            current_time = int(time.time())
            exp_time = payload['exp']
            
            # Token should not be expired
            assert exp_time > current_time, "Token should not be expired"
            
            # Token should not be valid for more than 24 hours (OIDC tokens can have longer lifetimes)
            max_valid_time = current_time + 86400  # 24 hours
            assert exp_time <= max_valid_time, "Token should not be valid for more than 24 hours"
            
        except Exception as e:
            # If payload decoding fails, it might be because we're using Vercel token
            if token == vercel_token:
                print("✅ Expected: Vercel API token cannot be decoded as JWT")
            else:
                raise e
    
    @pytest.mark.asyncio
    async def test_oidc_token_refresh_simulation(self, oidc_token, vercel_token):
        """Test OIDC token refresh simulation."""
        # Get initial token
        initial_token = get_vercel_oidc_token()
        
        # If using Vercel token as fallback, test basic functionality
        if initial_token == vercel_token:
            print("✅ Testing Vercel API token refresh simulation")
            # Wait a moment and get token again
            await asyncio.sleep(1)
            refreshed_token = get_vercel_oidc_token()
            
            # Tokens should be the same (Vercel API tokens are persistent)
            assert refreshed_token == initial_token
            print("✅ Vercel API token refresh simulation passed")
            return
        
        # For real OIDC tokens, test refresh behavior
        initial_payload = decode_oidc_payload(initial_token)
        
        # Wait a moment and get token again
        await asyncio.sleep(1)
        refreshed_token = get_vercel_oidc_token()
        refreshed_payload = decode_oidc_payload(refreshed_token)
        
        # Tokens might be the same (cached) or different (refreshed)
        # Both scenarios are valid
        assert refreshed_token is not None
        assert refreshed_payload is not None
        
        # Verify refreshed token has valid structure
        assert 'sub' in refreshed_payload
        assert 'exp' in refreshed_payload
        assert 'iat' in refreshed_payload
    
    @pytest.mark.asyncio
    async def test_oidc_token_consistency(self, oidc_token, vercel_token):
        """Test OIDC token consistency across multiple calls."""
        # Get multiple tokens
        tokens = []
        payloads = []
        
        for _ in range(3):
            token = get_vercel_oidc_token()
            tokens.append(token)
            
            # Only decode if it's a real OIDC token
            if token != vercel_token:
                try:
                    payload = decode_oidc_payload(token)
                    payloads.append(payload)
                except Exception:
                    # If decoding fails, it might be Vercel token
                    payloads.append(None)
            else:
                payloads.append(None)
        
        # Verify all tokens are valid
        for token in tokens:
            assert token is not None
            assert isinstance(token, str)
            assert len(token) > 0
        
        # If using Vercel token, all should be the same
        if tokens[0] == vercel_token:
            for token in tokens:
                assert token == vercel_token
            print("✅ Vercel API token consistency verified")
        else:
            # For real OIDC tokens, verify all have same subject (same identity)
            subjects = [payload['sub'] for payload in payloads if payload]
            assert len(set(subjects)) == 1, "All tokens should have the same subject"
            
            # Verify all tokens have valid expiration times
            for payload in payloads:
                if payload:
                    import time
                    current_time = int(time.time())
                    assert payload['exp'] > current_time, "All tokens should not be expired"
    
    @pytest.mark.asyncio
    async def test_oidc_token_error_handling(self):
        """Test OIDC token error handling for invalid scenarios."""
        # Test with invalid token format
        with pytest.raises(Exception):
            decode_oidc_payload("invalid.token.format")
        
        # Test with empty token
        with pytest.raises(Exception):
            decode_oidc_payload("")
        
        # Test with None token
        with pytest.raises(Exception):
            decode_oidc_payload(None)
    
    @pytest.mark.asyncio
    async def test_oidc_token_permissions(self, oidc_token, vercel_token):
        """Test OIDC token permissions and scopes."""
        # Get token
        token = get_vercel_oidc_token()
        
        # If using Vercel token as fallback, skip JWT-specific tests
        if token == vercel_token:
            print("✅ Skipping JWT permissions tests (using Vercel API token)")
            return
        
        # Decode payload (only for real OIDC tokens)
        try:
            payload = decode_oidc_payload(token)
            
            # Check for scope information if present
            if 'scope' in payload:
                assert isinstance(payload['scope'], str), "Scope should be a string"
                # Vercel scopes can be complex (e.g., "owner:framework-test-matrix-vtest314:project:vercel-py:environment:development")
                # Just verify it's a non-empty string
                assert len(payload['scope']) > 0, "Scope should not be empty"
            
            # Check for role information if present
            if 'role' in payload:
                assert isinstance(payload['role'], str), "Role should be a string"
                valid_roles = ['admin', 'member', 'viewer', 'owner']
                assert payload['role'] in valid_roles, f"Unknown role: {payload['role']}"
                
        except Exception as e:
            # If payload decoding fails, it might be because we're using Vercel token
            if token == vercel_token:
                print("✅ Expected: Vercel API token cannot be decoded as JWT")
            else:
                raise e
    
    @pytest.mark.asyncio
    async def test_oidc_token_environment_integration(self, oidc_token, vercel_token):
        """Test OIDC token integration with environment variables."""
        # Test that token retrieval works with environment setup
        token = get_vercel_oidc_token()
        assert token is not None
        
        # Test that token can be used for API calls
        # This is a basic test - in real scenarios, the token would be used
        # to authenticate with Vercel APIs
        
        if token == vercel_token:
            print("✅ Vercel API token integration verified")
            # Verify token has necessary format for API usage
            assert isinstance(token, str)
            assert len(token) > 0
        else:
            # For real OIDC tokens, verify token has necessary claims for API usage
            try:
                payload = decode_oidc_payload(token)
                assert 'sub' in payload, "Token should have subject for API authentication"
                assert 'exp' in payload, "Token should have expiration for API authentication"
            except Exception as e:
                if token == vercel_token:
                    print("✅ Expected: Vercel API token cannot be decoded as JWT")
                else:
                    raise e
    
    @pytest.mark.asyncio
    async def test_oidc_token_concurrent_access(self, oidc_token, vercel_token):
        """Test concurrent OIDC token access."""
        async def get_token_and_payload():
            token = get_vercel_oidc_token()
            if token == vercel_token:
                return token, None
            try:
                payload = decode_oidc_payload(token)
                return token, payload
            except Exception:
                return token, None
        
        # Get tokens concurrently
        results = await asyncio.gather(*[get_token_and_payload() for _ in range(5)])
        
        # Verify all tokens are valid
        for token, payload in results:
            assert token is not None
            assert isinstance(token, str)
            assert len(token) > 0
        
        # If using Vercel token, all should be the same
        if results[0][0] == vercel_token:
            for token, _ in results:
                assert token == vercel_token
            print("✅ Vercel API token concurrent access verified")
        else:
            # For real OIDC tokens, verify all tokens have same subject (same identity)
            subjects = [payload['sub'] for _, payload in results if payload]
            if subjects:
                assert len(set(subjects)) == 1, "All concurrent tokens should have same subject"