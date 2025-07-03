import pytest
import requests_mock
from orchestrator.resources.api_client import DjangoAPIClient
from orchestrator.utils.config import DJANGO_API_URL, DJANGO_JWT_TOKEN


class TestDjangoAPIClient:
    """Test suite for DjangoAPIClient"""

    def setup_method(self):
        """Setup client for each test"""
        self.client = DjangoAPIClient(
            base_url=DJANGO_API_URL,
            api_token=DJANGO_JWT_TOKEN
        )

    def test_get_report_with_modifier_mocked(self):
        """Unit test with mocked response"""
        with requests_mock.Mocker() as m:
            # Mock the expected response
            mock_response = {
                "data": {
                    "report": {"id": 1, "name": "Test Report"},
                    "modifiers": {"modifier_id": 1},
                    "eventgroup": {"events": []}
                },
                "meta": {"report_id": 1}
            }
            
            # Setup mock with expected content-length
            mock_content = str(mock_response).encode('utf-8')
            m.get(
                f"{DJANGO_API_URL}/api/reports/1/modifiers/1/all",
                json=mock_response,
                status_code=200,
                headers={'Content-Length': str(len(mock_content))}
            )
            
            # Call the method
            response = self.client.get_report_with_modifier(1, 1)
            
            # Assertions
            assert response == mock_response
            assert response["meta"]["report_id"] == 1

    def test_get_all_reports_mocked(self):
        """Unit test for get_all_reports with mocked response"""
        with requests_mock.Mocker() as m:
            mock_response = {
                "data": [
                    {"id": 1, "name": "Report 1", "cron": "0 9 * * *"},
                    {"id": 2, "name": "Report 2", "cron": "0 18 * * *"},
                    {"id": 3, "name": "Report 3", "cron": None}
                ],
                "meta": {"total": 3}
            }
            
            m.get(
                f"{DJANGO_API_URL}/api/reports/",
                json=mock_response,
                status_code=200
            )
            
            # Call the method
            response = self.client.get_all_reports()
            
            # Assertions
            assert response == mock_response
            assert len(response["data"]) == 3
            assert response["meta"]["total"] == 3
            # Verify some reports have cron schedules
            reports_with_cron = [r for r in response["data"] if r["cron"]]
            assert len(reports_with_cron) == 2

    def test_create_job_mocked(self):
        """Unit test for create_job with mocked response - NO real API call"""
        with requests_mock.Mocker() as m:
            # Input data
            job_data = {
                "report": 1,
                "fireant_jobid": "ext_job_123",
                "status": "submitted"
            }
            
            # Expected response
            mock_response = {
                "id": 42,
                "report": 1,
                "fireant_jobid": "ext_job_123",
                "status": "submitted",
                "created_at": "2024-01-01T12:00:00Z"
            }
            
            # Mock POST request
            m.post(
                f"{DJANGO_API_URL}/api/jobs/",
                json=mock_response,
                status_code=201
            )
            
            # Call the method
            response = self.client.create_job(job_data)
            
            # Assertions
            assert response == mock_response
            assert response["id"] == 42
            assert response["report"] == 1
            assert response["fireant_jobid"] == "ext_job_123"
            assert response["status"] == "submitted"
            
            # Verify the request was made correctly
            assert m.call_count == 1
            request = m.last_request
            assert request.method == "POST"
            assert request.url == f"{DJANGO_API_URL}/api/jobs/"
            assert request.json() == job_data
            assert "Authorization" in request.headers
            assert request.headers["Authorization"] == f"Bearer {DJANGO_JWT_TOKEN}"

    def test_update_job_mocked(self):
        """Unit test for update_job with mocked response - NO real API call"""
        with requests_mock.Mocker() as m:
            # Input data
            job_id = 42
            update_data = {
                "status": "completed",
                "results_url": "https://storage.azure.com/results/job_123.json"
            }
            
            # Expected response
            mock_response = {
                "id": 42,
                "report": 1,
                "fireant_jobid": "ext_job_123",
                "status": "completed",
                "results_url": "https://storage.azure.com/results/job_123.json",
                "updated_at": "2024-01-01T14:00:00Z"
            }
            
            # Mock PATCH request
            m.patch(
                f"{DJANGO_API_URL}/api/jobs/{job_id}/",
                json=mock_response,
                status_code=200
            )
            
            # Call the method
            response = self.client.update_job(job_id, update_data)
            
            # Assertions
            assert response == mock_response
            assert response["id"] == 42
            assert response["status"] == "completed"
            assert response["results_url"] == "https://storage.azure.com/results/job_123.json"
            
            # Verify the request was made correctly
            assert m.call_count == 1
            request = m.last_request
            assert request.method == "PATCH"
            assert request.url == f"{DJANGO_API_URL}/api/jobs/{job_id}/"
            assert request.json() == update_data
            assert "Authorization" in request.headers
            assert request.headers["Authorization"] == f"Bearer {DJANGO_JWT_TOKEN}"

    @pytest.mark.integration
    def test_get_report_with_modifier_real_api(self):
        """
        Integration test with real API call.
        
        Run with: pytest -m integration tests/test_api_client.py::TestDjangoAPIClient::test_get_report_with_modifier_real_api
        
        Requires:
        - DJANGO_API_URL and DJANGO_JWT_TOKEN to be set correctly in .env
        - The Django API to be running and accessible
        - Report with ID 1 and modifier with ID 1 to exist
        """
        try:
            # Make real API call
            response = self.client.get_report_with_modifier(1, 1)
            
            # Basic assertions
            assert isinstance(response, dict), "Response should be a dictionary"
            assert "meta" in response, "Response should contain 'meta' key"
            assert response["meta"]["report_id"] == 1, f"Expected report_id=1, got {response['meta']['report_id']}"
            
            print(f"✓ API call successful")
            print(f"✓ Response contains meta.report_id = {response['meta']['report_id']}")
            
        except Exception as e:
            pytest.fail(f"Real API test failed: {e}")

    @pytest.mark.integration
    def test_get_all_reports_real_api(self):
        """
        Integration test for get_all_reports with real API call.
        
        Run with: pytest -m integration tests/test_api_client.py::TestDjangoAPIClient::test_get_all_reports_real_api
        """
        try:
            # Make real API call
            response = self.client.get_all_reports()
            
            # Basic assertions
            assert isinstance(response, dict), "Response should be a dictionary"
            assert "data" in response, "Response should contain 'data' key"
            assert isinstance(response["data"], list), "Response data should be a list"
            
            print(f"✓ get_all_reports API call successful")
            print(f"✓ Found {len(response['data'])} reports")
            
            # Check if any reports have cron schedules
            reports_with_cron = [r for r in response["data"] if r.get("cron")]
            print(f"✓ Reports with cron schedules: {len(reports_with_cron)}")
            
        except Exception as e:
            pytest.fail(f"Real API test for get_all_reports failed: {e}")

    @pytest.mark.integration  
    def test_get_report_with_modifier_response_details(self):
        """
        Integration test that checks specific response details like content-length.
        
        This test makes a real HTTP request to verify the exact response characteristics.
        """
        import requests
        
        # Make direct request to get response details
        url = f"{DJANGO_API_URL}/api/reports/1/modifiers/1/all"
        headers = {
            "Authorization": f"Bearer {DJANGO_JWT_TOKEN}",
            "Content-Type": "application/json",
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            # Status check
            assert response.status_code == 200, f"Expected 200, got {response.status_code}"
            print(f"✓ HTTP Status: {response.status_code}")
            
            # Content-length check
            content_length = response.headers.get('Content-Length')
            if content_length:
                assert int(content_length) == 1594, f"Expected content-length=1594, got {content_length}"
                print(f"✓ Content-Length: {content_length}")
            else:
                print("⚠ Content-Length header not present")
            
            # JSON content check
            json_response = response.json()
            assert json_response["meta"]["report_id"] == 1
            print(f"✓ meta.report_id = {json_response['meta']['report_id']}")
            
        except Exception as e:
            pytest.fail(f"Response details test failed: {e}")


# Helper to run integration tests only when explicitly requested
def test_unit_only():
    """This test always passes - use to verify test setup works"""
    client = DjangoAPIClient(base_url="http://test.com", api_token="test-token")
    assert client.base_url == "http://test.com"
    assert client.api_token == "test-token" 