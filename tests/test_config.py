import pytest
import os
from unittest.mock import patch
from orchestrator.utils.config import (
    get_environment, Environment, get_required_env, get_optional_env, 
    get_optional_int, get_django_api_url, get_external_api_url, get_log_level
)


class TestConfig:
    """Test the configuration system"""

    def setup_method(self):
        """Clear environment before each test"""
        # Clear all relevant environment variables
        env_vars_to_clear = [
            "APP_ENV", "DJANGO_API_URL", "DJANGO_JWT_TOKEN", "EXTERNAL_API_URL", 
            "EXTERNAL_API_KEY", "AZURE_CONNECTION_STRING", "AZURE_CONTAINER_NAME",
            "AZURE_ACCOUNT_NAME", "AZURE_ACCOUNT_KEY", "FABRIC_WORKSPACE_ID",
            "CRON_SENSOR_INTERVAL_SECONDS", "DISCOVERY_SENSOR_INTERVAL_SECONDS"
        ]
        for var in env_vars_to_clear:
            if var in os.environ:
                del os.environ[var]

    def test_environment_detection_default(self):
        """Test default environment detection"""
        env = get_environment()
        assert env == Environment.DEVELOPMENT

    def test_environment_detection_production(self):
        """Test production environment detection"""
        os.environ["APP_ENV"] = "prod"
        env = get_environment()
        assert env == Environment.PRODUCTION

    def test_environment_detection_testing(self):
        """Test testing environment detection"""
        os.environ["APP_ENV"] = "test"
        env = get_environment()
        assert env == Environment.TESTING

    def test_environment_detection_invalid(self):
        """Test invalid environment falls back to development"""
        os.environ["APP_ENV"] = "invalid"
        env = get_environment()
        assert env == Environment.DEVELOPMENT

    def test_required_environment_variables_missing(self):
        """Test that missing required variables cause ValueError"""
        # Don't set any required variables
        with pytest.raises(ValueError) as exc_info:
            get_required_env("DJANGO_JWT_TOKEN")
        
        error_msg = str(exc_info.value)
        assert "DJANGO_JWT_TOKEN" in error_msg

    def test_required_environment_variables_present(self):
        """Test that all required variables work correctly"""
        # Set all required variables
        os.environ.update({
            "DJANGO_API_URL": "https://api.example.com",
            "DJANGO_JWT_TOKEN": "test-jwt-token",
            "EXTERNAL_API_URL": "https://external.example.com",
            "EXTERNAL_API_KEY": "test-external-key",
            "AZURE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net",
            "AZURE_CONTAINER_NAME": "test-container",
            "AZURE_ACCOUNT_NAME": "testaccount",
            "AZURE_ACCOUNT_KEY": "test-key",
            "FABRIC_WORKSPACE_ID": "test-workspace-id"
        })
        
        assert get_required_env("DJANGO_JWT_TOKEN") == "test-jwt-token"
        assert get_required_env("EXTERNAL_API_KEY") == "test-external-key"
        assert get_required_env("AZURE_CONNECTION_STRING") == "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net"

    def test_optional_environment_variables_defaults(self):
        assert get_required_env("EXTERNAL_API_KEY") == "test-external-key"
        assert get_required_env("AZURE_CONNECTION_STRING") == "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net"

 