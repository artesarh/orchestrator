import pytest
import os
import time
from orchestrator.resources.api_client import DjangoAPIClient


class TestDjangoAPIClientIntegration:
    """Integration tests that can run against different environments"""

    @pytest.fixture
    def test_client(self):
        """Client configured for test environment"""
        test_url = os.getenv("DJANGO_TEST_API_URL", "http://localhost:8001")  # Different port for test
        test_token = os.getenv("DJANGO_TEST_JWT_TOKEN", "test-jwt-token")
        
        return DjangoAPIClient(
            base_url=test_url,
            api_token=test_token
        )

    @pytest.fixture  
    def staging_client(self):
        """Client configured for staging environment"""
        staging_url = os.getenv("DJANGO_STAGING_API_URL")
        staging_token = os.getenv("DJANGO_STAGING_JWT_TOKEN")
        
        if not staging_token:
            pytest.skip("DJANGO_STAGING_JWT_TOKEN not set")
            
        return DjangoAPIClient(
            base_url=staging_url,
            api_token=staging_token
        )

    @pytest.mark.integration
    @pytest.mark.local
    def test_create_job_local_api(self, test_client):
        """Test job creation against local test API"""
        job_data = {
            "report": 1,
            "fireant_jobid": f"test_job_{int(time.time())}",
            "status": "submitted"
        }
        
        try:
            response = test_client.create_job(job_data)
            
            assert "id" in response
            assert response["report"] == 1
            assert response["status"] == "submitted"
            
            # Clean up - delete the test job
            job_id = response["id"]
            test_client.delete_job(job_id)
            print(f"✓ Created and cleaned up test job {job_id} successfully")
            
        except Exception as e:
            pytest.fail(f"Local API test failed: {e}")

    @pytest.mark.integration  
    @pytest.mark.staging
    def test_create_job_staging_api(self, staging_client):
        """Test job creation against staging API"""
        job_data = {
            "report": 1,
            "fireant_jobid": f"staging_test_{int(time.time())}",
            "status": "submitted"
        }
        
        try:
            response = staging_client.create_job(job_data)
            
            assert "id" in response
            assert response["report"] == 1
            assert response["status"] == "submitted"
            
            print(f"✓ Created staging job {response['id']} successfully")
            
        except Exception as e:
            pytest.fail(f"Staging API test failed: {e}")

    @pytest.mark.integration
    @pytest.mark.local
    def test_update_job_local_api(self, test_client):
        """Test job update against local test API"""
        # First create a job
        job_data = {
            "report": 1,
            "fireant_jobid": f"update_test_{int(time.time())}",
            "status": "submitted"
        }
        
        created_job = test_client.create_job(job_data)
        job_id = created_job["id"]
        
        # Then update it
        update_data = {
            "status": "completed",
            "results_url": "https://test-storage.com/results.json"
        }
        
        try:
            response = test_client.update_job(job_id, update_data)
            
            assert response["id"] == job_id
            assert response["status"] == "completed"
            assert response["results_url"] == "https://test-storage.com/results.json"
            
            print(f"✓ Updated job {job_id} successfully")
            
        except Exception as e:
            pytest.fail(f"Local API update test failed: {e}") 