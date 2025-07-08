#!/usr/bin/env python3
"""
Tests for Fabric storage resource
Tests Azure Data Lake storage integration for Fabric lakehouse
"""

import pytest
import json
from unittest.mock import patch, MagicMock, mock_open
from orchestrator.resources.fabric_storage import FabricStorageResource
from orchestrator.utils.config import (
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, 
    AZURE_ACCOUNT_NAME, FABRIC_WORKSPACE_ID
)


class TestFabricStorageResource:
    """Test the Fabric storage resource functionality"""

    def setup_method(self):
        """Setup storage resource for each test"""
        self.storage = FabricStorageResource(
            tenant_id=AZURE_TENANT_ID,
            client_id=AZURE_CLIENT_ID,
            client_secret=AZURE_CLIENT_SECRET,
            account_name=AZURE_ACCOUNT_NAME,
            workspace_id=FABRIC_WORKSPACE_ID,
            data_path="test_results"
        )

    def test_storage_initialization(self):
        """Test storage resource initialization with correct parameters"""
        assert self.storage.tenant_id == AZURE_TENANT_ID
        assert self.storage.client_id == AZURE_CLIENT_ID
        assert self.storage.client_secret == AZURE_CLIENT_SECRET
        assert self.storage.account_name == AZURE_ACCOUNT_NAME
        assert self.storage.workspace_id == FABRIC_WORKSPACE_ID
        assert self.storage.data_path == "test_results"

    def test_storage_initialization_default_path(self):
        """Test storage resource initialization with default data path"""
        storage = FabricStorageResource(
            tenant_id=AZURE_TENANT_ID,
            client_id=AZURE_CLIENT_ID,
            client_secret=AZURE_CLIENT_SECRET,
            account_name=AZURE_ACCOUNT_NAME,
            workspace_id=FABRIC_WORKSPACE_ID
        )
        
        assert storage.data_path == "report_results"  # Default path

    @patch('orchestrator.resources.fabric_storage.ClientSecretCredential')
    @patch('orchestrator.resources.fabric_storage.DataLakeServiceClient')
    def test_get_service_client(self, mock_service_client, mock_credential):
        """Test _get_service_client method creates correct client"""
        # Mock the credential and service client
        mock_cred_instance = MagicMock()
        mock_credential.return_value = mock_cred_instance
        
        mock_client_instance = MagicMock()
        mock_service_client.return_value = mock_client_instance
        
        # Call the method
        client = self.storage._get_service_client()
        
        # Verify credential creation
        mock_credential.assert_called_once_with(
            tenant_id=AZURE_TENANT_ID,
            client_id=AZURE_CLIENT_ID,
            client_secret=AZURE_CLIENT_SECRET
        )
        
        # Verify service client creation
        expected_url = f"https://{AZURE_ACCOUNT_NAME}.dfs.fabric.microsoft.com"
        mock_service_client.assert_called_once_with(expected_url, credential=mock_cred_instance)
        
        assert client == mock_client_instance

    @patch('orchestrator.resources.fabric_storage.get_dagster_logger')
    @patch.object(FabricStorageResource, '_get_service_client')
    def test_upload_json_success(self, mock_get_client, mock_logger):
        """Test successful JSON upload to Fabric lakehouse"""
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock the Azure clients
        mock_service_client = MagicMock()
        mock_file_system_client = MagicMock()
        mock_dir_client = MagicMock()
        mock_file_client = MagicMock()
        
        mock_get_client.return_value = mock_service_client
        mock_service_client.get_file_system_client.return_value = mock_file_system_client
        mock_file_system_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.get_file_client.return_value = mock_file_client
        
        # Test data
        test_data = {
            "results": [
                {"location": "New York", "exposure": 1000000},
                {"location": "Los Angeles", "exposure": 500000}
            ],
            "metadata": {"total_locations": 2}
        }
        file_name = "test_report_123.json"
        
        # Call the method
        result_path = self.storage.upload_json(test_data, file_name)
        
        # Verify the result
        assert result_path == f"test_results/{file_name}"
        
        # Verify service client calls
        mock_service_client.get_file_system_client.assert_called_once_with(FABRIC_WORKSPACE_ID)
        mock_file_system_client.get_directory_client.assert_called_once_with("test_results")
        mock_dir_client.get_file_client.assert_called_once_with(file_name)
        
        # Verify file upload
        expected_json_bytes = json.dumps(test_data, indent=2).encode('utf-8')
        mock_file_client.upload_data.assert_called_once_with(expected_json_bytes, overwrite=True)
        
        # Verify logging
        mock_logger_instance.info.assert_called_with(
            f'File {file_name} uploaded to test_results successfully'
        )

    @patch('orchestrator.resources.fabric_storage.get_dagster_logger')
    @patch.object(FabricStorageResource, '_get_service_client')
    def test_upload_json_directory_creation(self, mock_get_client, mock_logger):
        """Test that directory creation is attempted and handled gracefully"""
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock the Azure clients
        mock_service_client = MagicMock()
        mock_file_system_client = MagicMock()
        mock_dir_client = MagicMock()
        mock_file_client = MagicMock()
        
        # Simulate directory creation error (already exists)
        mock_dir_client.create_directory.side_effect = Exception("Directory already exists")
        
        mock_get_client.return_value = mock_service_client
        mock_service_client.get_file_system_client.return_value = mock_file_system_client
        mock_file_system_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.get_file_client.return_value = mock_file_client
        
        # Test data
        test_data = {"test": "data"}
        file_name = "test_file.json"
        
        # Call the method - should not raise exception
        result_path = self.storage.upload_json(test_data, file_name)
        
        # Verify the result
        assert result_path == f"test_results/{file_name}"
        
        # Verify directory creation was attempted
        mock_dir_client.create_directory.assert_called_once()
        
        # Verify debug log for directory error
        mock_logger_instance.debug.assert_called_with(
            "Directory exists or error: Directory already exists"
        )

    @patch('orchestrator.resources.fabric_storage.get_dagster_logger')
    @patch.object(FabricStorageResource, '_get_service_client')
    def test_upload_json_failure(self, mock_get_client, mock_logger):
        """Test JSON upload failure handling"""
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock service client to raise an exception
        mock_get_client.side_effect = Exception("Authentication failed")
        
        # Test data
        test_data = {"test": "data"}
        file_name = "test_file.json"
        
        # Call the method - should raise exception
        with pytest.raises(Exception, match="Authentication failed"):
            self.storage.upload_json(test_data, file_name)
        
        # Verify error logging
        mock_logger_instance.error.assert_called_with(
            f"Failed to upload {file_name}: Authentication failed"
        )

    @patch('orchestrator.resources.fabric_storage.get_dagster_logger')
    @patch.object(FabricStorageResource, '_get_service_client')
    def test_upload_json_complex_data(self, mock_get_client, mock_logger):
        """Test uploading complex JSON data structures"""
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock the Azure clients
        mock_service_client = MagicMock()
        mock_file_system_client = MagicMock()
        mock_dir_client = MagicMock()
        mock_file_client = MagicMock()
        
        mock_get_client.return_value = mock_service_client
        mock_service_client.get_file_system_client.return_value = mock_file_system_client
        mock_file_system_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.get_file_client.return_value = mock_file_client
        
        # Complex test data
        test_data = {
            "report_id": 123,
            "timestamp": "2024-01-01T12:00:00Z",
            "results": [
                {
                    "location": {
                        "name": "New York",
                        "coordinates": {"lat": 40.7128, "lon": -74.0060}
                    },
                    "exposure": 1000000,
                    "details": {
                        "risk_factors": ["earthquake", "flood"],
                        "confidence": 0.95
                    }
                }
            ],
            "metadata": {
                "processing_time": 45.2,
                "api_version": "v2.1",
                "parameters": {
                    "as_at_date": "2024-01-01",
                    "fx_date": "2024-01-01"
                }
            }
        }
        file_name = "complex_report_123.json"
        
        # Call the method
        result_path = self.storage.upload_json(test_data, file_name)
        
        # Verify the result
        assert result_path == f"test_results/{file_name}"
        
        # Verify the JSON was properly serialized
        call_args = mock_file_client.upload_data.call_args[0]
        uploaded_bytes = call_args[0]
        uploaded_data = json.loads(uploaded_bytes.decode('utf-8'))
        
        assert uploaded_data == test_data
        assert uploaded_data["report_id"] == 123
        assert len(uploaded_data["results"]) == 1
        assert uploaded_data["results"][0]["location"]["name"] == "New York"

    @patch('orchestrator.resources.fabric_storage.get_dagster_logger')
    @patch.object(FabricStorageResource, '_get_service_client')
    def test_upload_json_with_special_characters(self, mock_get_client, mock_logger):
        """Test uploading JSON with special characters and unicode"""
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock the Azure clients
        mock_service_client = MagicMock()
        mock_file_system_client = MagicMock()
        mock_dir_client = MagicMock()
        mock_file_client = MagicMock()
        
        mock_get_client.return_value = mock_service_client
        mock_service_client.get_file_system_client.return_value = mock_file_system_client
        mock_file_system_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.get_file_client.return_value = mock_file_client
        
        # Test data with special characters
        test_data = {
            "location": "São Paulo",
            "description": "Test with émojis: 🌍 and symbols: €£¥",
            "special_chars": "Quotes: \"double\" and 'single'",
            "unicode": "Çhinese: 中文, Àrabic: العربية",
            "numbers": [1, 2.5, -3, 1e10]
        }
        file_name = "special_chars_report.json"
        
        # Call the method
        result_path = self.storage.upload_json(test_data, file_name)
        
        # Verify the result
        assert result_path == f"test_results/{file_name}"
        
        # Verify the JSON was properly encoded as UTF-8
        call_args = mock_file_client.upload_data.call_args[0]
        uploaded_bytes = call_args[0]
        
        # Should be valid UTF-8 bytes
        uploaded_text = uploaded_bytes.decode('utf-8')
        uploaded_data = json.loads(uploaded_text)
        
        assert uploaded_data == test_data
        assert uploaded_data["location"] == "São Paulo"
        assert "🌍" in uploaded_data["description"]

    def test_upload_json_file_name_validation(self):
        """Test various file name formats"""
        with patch.object(self.storage, '_get_service_client'):
            with patch('orchestrator.resources.fabric_storage.get_dagster_logger'):
                # Mock the full chain
                mock_service_client = MagicMock()
                mock_file_system_client = MagicMock()
                mock_dir_client = MagicMock()
                mock_file_client = MagicMock()
                
                self.storage._get_service_client.return_value = mock_service_client
                mock_service_client.get_file_system_client.return_value = mock_file_system_client
                mock_file_system_client.get_directory_client.return_value = mock_dir_client
                mock_dir_client.get_file_client.return_value = mock_file_client
                
                # Test various file name formats
                test_cases = [
                    "simple.json",
                    "report_123.json",
                    "report_2024-01-01_12-30-45.json",
                    "complex_report_with_underscores_and_numbers_123.json",
                    "report-with-hyphens.json"
                ]
                
                for file_name in test_cases:
                    result_path = self.storage.upload_json({"test": "data"}, file_name)
                    assert result_path == f"test_results/{file_name}"
                    mock_dir_client.get_file_client.assert_called_with(file_name)


class TestFabricStorageIntegration:
    """Integration tests for Fabric storage (require real Azure credentials)"""

    @pytest.mark.integration
    def test_real_azure_authentication(self):
        """
        Integration test - test real Azure authentication
        
        Run with: pytest -m integration tests/test_fabric_storage.py::TestFabricStorageIntegration::test_real_azure_authentication
        
        Requires:
        - Valid Azure credentials in environment
        - Fabric workspace to be accessible
        """
        try:
            storage = FabricStorageResource(
                tenant_id=AZURE_TENANT_ID,
                client_id=AZURE_CLIENT_ID,
                client_secret=AZURE_CLIENT_SECRET,
                account_name=AZURE_ACCOUNT_NAME,
                workspace_id=FABRIC_WORKSPACE_ID,
                data_path="test_integration"
            )
            
            # Try to get service client - this will test authentication
            client = storage._get_service_client()
            
            # Basic validation
            assert client is not None
            print("✅ Successfully authenticated with Azure Data Lake")
            
        except Exception as e:
            pytest.fail(f"Real Azure authentication failed: {e}")

    @pytest.mark.integration
    def test_real_json_upload(self):
        """
        Integration test - upload actual JSON to Fabric lakehouse
        
        This test uploads a small test file and should clean up after itself
        """
        import time
        
        try:
            storage = FabricStorageResource(
                tenant_id=AZURE_TENANT_ID,
                client_id=AZURE_CLIENT_ID,
                client_secret=AZURE_CLIENT_SECRET,
                account_name=AZURE_ACCOUNT_NAME,
                workspace_id=FABRIC_WORKSPACE_ID,
                data_path="test_integration"
            )
            
            # Test data
            test_data = {
                "test_upload": True,
                "timestamp": time.time(),
                "message": "This is a test upload from the integration test",
                "data": [1, 2, 3, 4, 5]
            }
            
            # Generate unique file name
            file_name = f"integration_test_{int(time.time())}.json"
            
            # Upload the file
            result_path = storage.upload_json(test_data, file_name)
            
            # Verify result
            assert result_path == f"test_integration/{file_name}"
            print(f"✅ Successfully uploaded test file: {result_path}")
            
            # Note: In a real scenario, you might want to verify the file exists
            # and then clean it up, but that would require additional Azure SDK calls
            
        except Exception as e:
            pytest.fail(f"Real JSON upload failed: {e}")


class TestFabricStorageErrorHandling:
    """Test error handling scenarios for Fabric storage"""

    def test_invalid_json_data(self):
        """Test handling of non-serializable data"""
        with patch.object(FabricStorageResource, '_get_service_client'):
            storage = FabricStorageResource(
                tenant_id="test",
                client_id="test",
                client_secret="test",
                account_name="test",
                workspace_id="test"
            )
            
            # Data that cannot be JSON serialized
            class NonSerializable:
                pass
            
            invalid_data = {"object": NonSerializable()}
            
            with pytest.raises(TypeError):
                storage.upload_json(invalid_data, "test.json")

    @patch('orchestrator.resources.fabric_storage.get_dagster_logger')
    @patch.object(FabricStorageResource, '_get_service_client')
    def test_network_timeout_handling(self, mock_get_client, mock_logger):
        """Test handling of network timeouts"""
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock network timeout
        from azure.core.exceptions import ServiceRequestTimeoutError
        mock_get_client.side_effect = ServiceRequestTimeoutError("Request timed out")
        
        storage = FabricStorageResource(
            tenant_id="test",
            client_id="test", 
            client_secret="test",
            account_name="test",
            workspace_id="test"
        )
        
        with pytest.raises(ServiceRequestTimeoutError):
            storage.upload_json({"test": "data"}, "test.json")

    @patch('orchestrator.resources.fabric_storage.get_dagster_logger')
    @patch.object(FabricStorageResource, '_get_service_client')
    def test_authentication_error_handling(self, mock_get_client, mock_logger):
        """Test handling of authentication errors"""
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock authentication error
        from azure.core.exceptions import ClientAuthenticationError
        mock_get_client.side_effect = ClientAuthenticationError("Authentication failed")
        
        storage = FabricStorageResource(
            tenant_id="test",
            client_id="test",
            client_secret="test", 
            account_name="test",
            workspace_id="test"
        )
        
        with pytest.raises(ClientAuthenticationError):
            storage.upload_json({"test": "data"}, "test.json") 