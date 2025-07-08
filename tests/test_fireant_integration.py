#!/usr/bin/env python3
"""
Integration tests for Fireant API client
Tests job submission, status checking, and result download
"""

import pytest
import requests_mock
import json
from unittest.mock import patch, MagicMock
from orchestrator.resources.external_api import FireantAPIClient, JobType
from orchestrator.utils.config import FIREANT_API_URL, FIREANT_API_KEY


class TestFireantAPIClient:
    """Test the Fireant API client functionality"""

    def setup_method(self):
        """Setup client for each test"""
        self.client = FireantAPIClient(
            base_url=FIREANT_API_URL,
            api_key=FIREANT_API_KEY
        )

    def test_job_type_enum_values(self):
        """Test that JobType enum has expected values"""
        assert JobType.GEO.value == "geo"
        assert JobType.RING.value == "ring"
        assert JobType.BOX.value == "box"
        
        # Test that we can create JobType from string
        assert JobType("geo") == JobType.GEO
        assert JobType("ring") == JobType.RING
        assert JobType("box") == JobType.BOX

    def test_submit_job_geo_type_mocked(self):
        """Test submitting a geo job with mocked response"""
        with requests_mock.Mocker() as m:
            # Mock job data
            job_data = {
                "geos": [
                    {"lat": 40.7128, "lon": -74.0060, "value": 1000000},
                    {"lat": 34.0522, "lon": -118.2437, "value": 500000}
                ],
                "as_at_date": "2024-01-01",
                "fx_date": "2024-01-01"
            }
            
            # Mock response
            mock_response = {"job_id": "geo_job_12345"}
            m.post(
                f"{FIREANT_API_URL}/job_exposure_geo_agg",
                json=mock_response,
                status_code=200
            )
            
            # Test the method
            job_id = self.client.submit_job(job_data, "geo")
            
            # Assertions
            assert job_id == "geo_job_12345"
            
            # Verify the request
            assert m.call_count == 1
            request = m.last_request
            assert request.method == "POST"
            assert request.url == f"{FIREANT_API_URL}/job_exposure_geo_agg"
            assert request.json() == job_data
            assert "Authorization" in request.headers
            assert request.headers["Authorization"] == f"Bearer {FIREANT_API_KEY}"

    def test_submit_job_ring_type_mocked(self):
        """Test submitting a ring job with mocked response"""
        with requests_mock.Mocker() as m:
            # Mock job data
            job_data = {
                "rings": [
                    {"center_lat": 40.7128, "center_lon": -74.0060, "radius": 1000, "value": 1000000}
                ],
                "as_at_date": "2024-01-01",
                "fx_date": "2024-01-01"
            }
            
            # Mock response
            mock_response = {"job_id": "ring_job_12345"}
            m.post(
                f"{FIREANT_API_URL}/job_exposure_ring_agg",
                json=mock_response,
                status_code=200
            )
            
            # Test the method
            job_id = self.client.submit_job(job_data, "ring")
            
            # Assertions
            assert job_id == "ring_job_12345"
            assert m.call_count == 1
            request = m.last_request
            assert request.url == f"{FIREANT_API_URL}/job_exposure_ring_agg"

    def test_submit_job_box_type_mocked(self):
        """Test submitting a box job with mocked response"""
        with requests_mock.Mocker() as m:
            # Mock job data
            job_data = {
                "boxes": [
                    {"min_lat": 40.0, "max_lat": 41.0, "min_lon": -75.0, "max_lon": -74.0, "value": 1000000}
                ],
                "as_at_date": "2024-01-01",
                "fx_date": "2024-01-01"
            }
            
            # Mock response
            mock_response = {"job_id": "box_job_12345"}
            m.post(
                f"{FIREANT_API_URL}/job_exposure_box_agg",
                json=mock_response,
                status_code=200
            )
            
            # Test the method
            job_id = self.client.submit_job(job_data, "box")
            
            # Assertions
            assert job_id == "box_job_12345"
            assert m.call_count == 1
            request = m.last_request
            assert request.url == f"{FIREANT_API_URL}/job_exposure_box_agg"

    def test_submit_job_invalid_type(self):
        """Test submitting job with invalid type raises error"""
        job_data = {"test": "data"}
        
        with pytest.raises(ValueError, match="Invalid job type 'invalid'"):
            self.client.submit_job(job_data, "invalid")

    def test_submit_job_case_insensitive(self):
        """Test that job type is case insensitive"""
        with requests_mock.Mocker() as m:
            job_data = {"geos": []}
            mock_response = {"job_id": "test_job"}
            
            m.post(
                f"{FIREANT_API_URL}/job_exposure_geo_agg",
                json=mock_response,
                status_code=200
            )
            
            # Test uppercase
            job_id = self.client.submit_job(job_data, "GEO")
            assert job_id == "test_job"

    def test_check_job_status_running(self):
        """Test checking job status - running"""
        with requests_mock.Mocker() as m:
            job_id = "test_job_12345"
            mock_response = {
                "data": {
                    "status": "Running",
                    "progress": 0.5,
                    "message": "Processing data..."
                }
            }
            
            m.get(
                f"{FIREANT_API_URL}/get_job_status/{job_id}?is_get_results=false&is_newline=true",
                json=mock_response,
                status_code=200
            )
            
            # Test the method
            status = self.client.check_job_status(job_id)
            
            # Assertions
            assert status == mock_response
            assert status["data"]["status"] == "Running"
            
            # Verify the request
            assert m.call_count == 1
            request = m.last_request
            assert request.method == "GET"
            assert "Authorization" in request.headers
            assert request.headers["Authorization"] == f"Bearer {FIREANT_API_KEY}"

    def test_check_job_status_finished(self):
        """Test checking job status - finished"""
        with requests_mock.Mocker() as m:
            job_id = "test_job_12345"
            mock_response = {
                "data": {
                    "status": "Finished",
                    "progress": 1.0,
                    "message": "Job completed successfully",
                    "results_url": f"/jobs/results/{job_id}"
                }
            }
            
            m.get(
                f"{FIREANT_API_URL}/get_job_status/{job_id}?is_get_results=false&is_newline=true",
                json=mock_response,
                status_code=200
            )
            
            # Test the method
            status = self.client.check_job_status(job_id)
            
            # Assertions
            assert status == mock_response
            assert status["data"]["status"] == "Finished"
            assert status["data"]["progress"] == 1.0

    def test_check_job_status_failed(self):
        """Test checking job status - failed"""
        with requests_mock.Mocker() as m:
            job_id = "test_job_12345"
            mock_response = {
                "data": {
                    "status": "Failed",
                    "progress": 0.3,
                    "message": "Job failed due to invalid data",
                    "error": "Invalid coordinates provided"
                }
            }
            
            m.get(
                f"{FIREANT_API_URL}/get_job_status/{job_id}?is_get_results=false&is_newline=true",
                json=mock_response,
                status_code=200
            )
            
            # Test the method
            status = self.client.check_job_status(job_id)
            
            # Assertions
            assert status == mock_response
            assert status["data"]["status"] == "Failed"
            assert "error" in status["data"]

    def test_download_results_success(self):
        """Test downloading job results successfully"""
        with requests_mock.Mocker() as m:
            job_id = "test_job_12345"
            mock_results = {
                "results": [
                    {"location": "New York", "exposure": 1000000},
                    {"location": "Los Angeles", "exposure": 500000}
                ],
                "metadata": {
                    "total_locations": 2,
                    "processing_time": "45.2s"
                }
            }
            
            # Mock the results endpoint
            m.get(
                f"{FIREANT_API_URL}/jobs/results/{job_id}",
                json=mock_results,
                status_code=200
            )
            
            # Test the method
            response = self.client.download_results(job_id)
            
            # Assertions
            assert isinstance(response, requests.Response)
            results_data = response.json()
            assert results_data == mock_results
            assert len(results_data["results"]) == 2
            
            # Verify the request
            assert m.call_count == 1
            request = m.last_request
            assert request.method == "GET"
            assert request.url == f"{FIREANT_API_URL}/jobs/results/{job_id}"
            assert "Authorization" in request.headers

    def test_download_results_not_found(self):
        """Test downloading results for non-existent job"""
        with requests_mock.Mocker() as m:
            job_id = "non_existent_job"
            
            m.get(
                f"{FIREANT_API_URL}/jobs/results/{job_id}",
                status_code=404,
                text="Job not found"
            )
            
            # Test the method - should raise exception
            with pytest.raises(requests.exceptions.HTTPError):
                self.client.download_results(job_id)

    def test_api_timeout_handling(self):
        """Test that API client handles timeouts correctly"""
        with requests_mock.Mocker() as m:
            # Mock timeout
            m.post(
                f"{FIREANT_API_URL}/job_exposure_geo_agg",
                exc=requests.exceptions.Timeout
            )
            
            with pytest.raises(requests.exceptions.Timeout):
                self.client.submit_job({"geos": []}, "geo")

    def test_api_connection_error_handling(self):
        """Test that API client handles connection errors correctly"""
        with requests_mock.Mocker() as m:
            # Mock connection error
            m.post(
                f"{FIREANT_API_URL}/job_exposure_geo_agg",
                exc=requests.exceptions.ConnectionError
            )
            
            with pytest.raises(requests.exceptions.ConnectionError):
                self.client.submit_job({"geos": []}, "geo")

    def test_api_http_error_handling(self):
        """Test that API client handles HTTP errors correctly"""
        with requests_mock.Mocker() as m:
            # Mock HTTP error
            m.post(
                f"{FIREANT_API_URL}/job_exposure_geo_agg",
                status_code=500,
                text="Internal Server Error"
            )
            
            with pytest.raises(requests.exceptions.HTTPError):
                self.client.submit_job({"geos": []}, "geo")

    @pytest.mark.integration
    def test_submit_job_real_api_geo(self):
        """
        Integration test - submit actual geo job to Fireant API
        
        Run with: pytest -m integration tests/test_fireant_integration.py::TestFireantAPIClient::test_submit_job_real_api_geo
        
        Requires:
        - FIREANT_API_URL and FIREANT_API_KEY to be set correctly
        - Fireant API to be accessible
        """
        try:
            # Test data for geo job
            job_data = {
                "geos": [
                    {"lat": 40.7128, "lon": -74.0060, "value": 1000000},  # New York
                    {"lat": 34.0522, "lon": -118.2437, "value": 500000}   # Los Angeles
                ],
                "as_at_date": "2024-01-01",
                "fx_date": "2024-01-01"
            }
            
            # Submit job
            job_id = self.client.submit_job(job_data, "geo")
            
            # Assertions
            assert isinstance(job_id, str)
            assert len(job_id) > 0
            print(f"✅ Successfully submitted geo job: {job_id}")
            
            return job_id  # Return for potential cleanup
            
        except Exception as e:
            pytest.fail(f"Real Fireant API test failed: {e}")

    @pytest.mark.integration
    def test_check_job_status_real_api(self):
        """
        Integration test - check actual job status on Fireant API
        
        This test requires a valid job_id from a previous submission
        """
        try:
            # First submit a job to get a valid job_id
            job_data = {
                "geos": [{"lat": 40.7128, "lon": -74.0060, "value": 1000000}],
                "as_at_date": "2024-01-01",
                "fx_date": "2024-01-01"
            }
            
            job_id = self.client.submit_job(job_data, "geo")
            print(f"✅ Submitted job for status check: {job_id}")
            
            # Check status
            status = self.client.check_job_status(job_id)
            
            # Assertions
            assert isinstance(status, dict)
            assert "data" in status
            assert "status" in status["data"]
            
            job_status = status["data"]["status"]
            assert job_status in ["Running", "Finished", "Failed", "Queued"]
            
            print(f"✅ Job status: {job_status}")
            
        except Exception as e:
            pytest.fail(f"Real Fireant API status check failed: {e}")

    @pytest.mark.integration
    def test_full_job_workflow_real_api(self):
        """
        Integration test - full workflow from submission to completion
        
        This test may take a while as it waits for job completion
        """
        import time
        
        try:
            # Submit job
            job_data = {
                "geos": [{"lat": 40.7128, "lon": -74.0060, "value": 1000000}],
                "as_at_date": "2024-01-01",
                "fx_date": "2024-01-01"
            }
            
            job_id = self.client.submit_job(job_data, "geo")
            print(f"✅ Submitted job: {job_id}")
            
            # Poll for completion (with timeout)
            max_attempts = 10
            poll_interval = 5  # seconds
            
            for attempt in range(max_attempts):
                status = self.client.check_job_status(job_id)
                job_status = status["data"]["status"]
                
                print(f"Attempt {attempt + 1}: Job status = {job_status}")
                
                if job_status == "Finished":
                    print("✅ Job completed successfully")
                    
                    # Try to download results
                    try:
                        response = self.client.download_results(job_id)
                        results = response.json()
                        print(f"✅ Downloaded results: {len(results.get('results', []))} items")
                        break
                    except Exception as e:
                        print(f"⚠️  Could not download results: {e}")
                        break
                        
                elif job_status == "Failed":
                    print(f"❌ Job failed: {status['data'].get('error', 'Unknown error')}")
                    break
                    
                elif job_status in ["Running", "Queued"]:
                    if attempt < max_attempts - 1:
                        time.sleep(poll_interval)
                    else:
                        print(f"⚠️  Job still {job_status} after {max_attempts} attempts")
                        break
                else:
                    print(f"⚠️  Unknown job status: {job_status}")
                    break
                    
        except Exception as e:
            pytest.fail(f"Full workflow test failed: {e}")


class TestFireantAPIClientConfiguration:
    """Test configuration and initialization of Fireant API client"""

    def test_client_initialization_with_custom_timeout(self):
        """Test client initialization with custom timeout"""
        client = FireantAPIClient(
            base_url="https://test-api.com",
            api_key="test-key",
            timeout=60
        )
        
        assert client.base_url == "https://test-api.com"
        assert client.api_key == "test-key"
        assert client.timeout == 60

    def test_client_initialization_default_timeout(self):
        """Test client initialization with default timeout"""
        client = FireantAPIClient(
            base_url="https://test-api.com",
            api_key="test-key"
        )
        
        assert client.timeout == 30  # Default timeout

    def test_client_configuration_from_environment(self):
        """Test that client can be configured from environment variables"""
        # This test uses the actual environment configuration
        client = FireantAPIClient(
            base_url=FIREANT_API_URL,
            api_key=FIREANT_API_KEY
        )
        
        assert client.base_url == FIREANT_API_URL
        assert client.api_key == FIREANT_API_KEY
        assert isinstance(client.timeout, int)
        assert client.timeout > 0 