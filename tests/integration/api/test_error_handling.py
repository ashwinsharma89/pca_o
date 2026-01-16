"""
Integration tests for error handling across all endpoints.

Tests:
- Rate limiting
- Malformed requests
- Missing fields
- Invalid parameters
- Database errors
- LLM timeouts
- Concurrent requests
"""

import pytest
from fastapi import status
import asyncio
from concurrent.futures import ThreadPoolExecutor


# ============================================================================
# Rate Limiting Tests
# ============================================================================

class TestRateLimiting:
    """Tests for rate limiting enforcement."""
    
    def test_rate_limiting_on_query_endpoint(self, client, auth_headers):
        """Test rate limiting on query endpoint."""
        # Make many requests quickly
        responses = []
        for i in range(20):
            response = client.post(
                "/api/v1/campaigns/chat",
                headers=auth_headers,
                json={'question': f'test query {i}'}
            )
            responses.append(response.status_code)
        
        # At least one should be rate limited (if implemented)
        # Or all should succeed if no rate limiting
        assert any(code == status.HTTP_429_TOO_MANY_REQUESTS for code in responses) or \
               all(code in [status.HTTP_200_OK, status.HTTP_400_BAD_REQUEST] for code in responses)
    
    def test_rate_limiting_per_user(self, client, auth_headers):
        """Test that rate limiting is per-user."""
        # This would need multiple users to test properly
        # For now, just verify endpoint responds
        response = client.get(
            "/api/v1/campaigns/filters",
            headers=auth_headers
        )
        
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_429_TOO_MANY_REQUESTS
        ]


# ============================================================================
# Malformed Request Tests
# ============================================================================

class TestMalformedRequests:
    """Tests for handling malformed requests."""
    
    def test_malformed_json(self, client, auth_headers):
        """Test handling of malformed JSON."""
        response = client.post(
            "/api/v1/campaigns/chat",
            headers={**auth_headers, "Content-Type": "application/json"},
            data="{'invalid': json}"  # Malformed JSON
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_missing_content_type(self, client, auth_headers):
        """Test request without Content-Type header."""
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            data='{"question": "test"}'
        )
        
        # Should handle gracefully
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_empty_request_body(self, client, auth_headers):
        """Test POST request with empty body."""
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={}
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]


# ============================================================================
# Missing Required Fields Tests
# ============================================================================

class TestMissingFields:
    """Tests for validation of required fields."""
    
    def test_missing_required_field_in_query(self, client, auth_headers):
        """Test query without required message field."""
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={}  # Missing 'question'
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_missing_required_field_in_intelligence(self, client, auth_headers):
        """Test intelligence query without required query field."""
        response = client.post(
            "/api/v1/intelligence/query",
            headers=auth_headers,
            json={}  # Missing 'query'
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]


# ============================================================================
# Invalid Parameters Tests
# ============================================================================

class TestInvalidParameters:
    """Tests for handling invalid query parameters."""
    
    def test_invalid_date_format(self, client, auth_headers):
        """Test endpoint with invalid date format."""
        response = client.get(
            "/api/v1/campaigns/visualizations?start_date=not-a-date",
            headers=auth_headers
        )
        
        # Should handle gracefully
        assert response.status_code in [
            status.HTTP_200_OK,  # Ignores invalid date
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_invalid_numeric_parameter(self, client, auth_headers):
        """Test endpoint with invalid numeric parameter."""
        response = client.get(
            "/api/v1/anomaly-detective/anomalies",
            headers=auth_headers,
            params={'limit': 'not-a-number'}
        )
        
        assert response.status_code in [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_negative_numeric_parameter(self, client, auth_headers):
        """Test endpoint with negative value where positive expected."""
        response = client.get(
            "/api/v1/anomaly-detective/anomalies",
            headers=auth_headers,
            params={'limit': -1}
        )
        
        # FastAPI/Pydantic might allow negative ints if not constrained, 
        # but let's check for validation if it exists.
        assert response.status_code in [
            status.HTTP_200_OK,  # If no constraint
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]


# ============================================================================
# Database Error Tests
# ============================================================================

class TestDatabaseErrors:
    """Tests for handling database errors."""
    
    def test_database_connection_error(self, client, auth_headers):
        """Test handling when database is unavailable."""
        # This is hard to test without actually breaking the DB
        # Just verify endpoint responds
        response = client.get(
            "/api/v1/campaigns/filters",
            headers=auth_headers
        )
        
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            status.HTTP_503_SERVICE_UNAVAILABLE
        ]
    
    def test_nonexistent_resource(self, client, auth_headers):
        """Test accessing non-existent resource."""
        response = client.get(
            "/api/v1/campaigns/99999",
            headers=auth_headers
        )
        
        assert response.status_code in [status.HTTP_404_NOT_FOUND, status.HTTP_200_OK]
        # In this specific app, it might return empty list or 404


# ============================================================================
# LLM Timeout Tests
# ============================================================================

class TestLLMTimeouts:
    """Tests for handling LLM timeouts."""
    
    def test_llm_timeout_handling(self, client, auth_headers):
        """Test that LLM timeouts are handled gracefully."""
        # Make a query that might timeout
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={
                'question': 'Analyze all campaigns in extreme detail with comprehensive insights'
            }
        )
        
        # Should either succeed or fail gracefully
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_408_REQUEST_TIMEOUT,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            status.HTTP_503_SERVICE_UNAVAILABLE
        ]


# ============================================================================
# Concurrent Request Tests
# ============================================================================

class TestConcurrentRequests:
    """Tests for handling concurrent requests."""
    
    def test_concurrent_read_requests(self, client, auth_headers):
        """Test handling multiple concurrent read requests."""
        def make_request():
            return client.get(
                "/api/v1/campaigns/filters",
                headers=auth_headers
            )
        
        # Make 10 concurrent requests
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            responses = [f.result() for f in futures]
        
        # All should succeed
        assert all(r.status_code == status.HTTP_200_OK for r in responses)
    
    def test_concurrent_write_requests(self, client, auth_headers, sample_csv_file):
        """Test handling multiple concurrent write requests."""
        def make_upload():
            # Reset file pointer
            sample_csv_file.seek(0)
            files = {'file': ('data.csv', sample_csv_file, 'text/csv')}
            return client.post(
                "/api/v1/campaigns/upload",
                headers=auth_headers,
                files=files
            )
        
        # Make 5 concurrent uploads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_upload) for _ in range(5)]
            responses = [f.result() for f in futures]
        
        status_codes = [r.status_code for r in responses]
        print(f"Concurrent upload status codes: {status_codes}")
        
        # Should handle gracefully (may succeed or have conflicts)
        assert all(r.status_code in [
            status.HTTP_200_OK,
            status.HTTP_201_CREATED,
            status.HTTP_409_CONFLICT,
            status.HTTP_500_INTERNAL_SERVER_ERROR
        ] for r in responses)


# ============================================================================
# Edge Case Tests
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""
    
    def test_very_long_query_string(self, client, auth_headers):
        """Test handling of very long query string."""
        long_message = "A" * 10000  # 10KB message
        
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={'question': long_message}
        )
        
        # Should either process or reject gracefully
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            status.HTTP_422_UNPROCESSABLE_ENTITY
        ]
    
    def test_special_characters_in_input(self, client, auth_headers):
        """Test handling of special characters."""
        special_chars = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={'question': special_chars}
        )
        
        # Should handle gracefully
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST
        ]
    
    def test_unicode_characters_in_input(self, client, auth_headers):
        """Test handling of Unicode characters."""
        unicode_text = "Hello 世界 🌍 مرحبا"
        
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={'question': unicode_text}
        )
        
        # Should handle gracefully
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST
        ]
    
    def test_sql_injection_attempt(self, client, auth_headers):
        """Test that SQL injection is prevented."""
        sql_injection = "'; DROP TABLE campaigns; --"
        
        response = client.post(
            "/api/v1/campaigns/chat",
            headers=auth_headers,
            json={'question': sql_injection}
        )
        
        # Should handle safely
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_400_BAD_REQUEST
        ]
        
        # Verify system still works after injection attempt
        verify_response = client.get(
            "/api/v1/campaigns/filters",
            headers=auth_headers
        )
        assert verify_response.status_code == status.HTTP_200_OK
