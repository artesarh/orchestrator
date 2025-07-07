#!/usr/bin/env python3
"""Simple test to verify the new config system works"""

import os
from orchestrator.utils.config import Config, Environment, ConfigError, reset_config

def test_config_basic():
    """Test basic config functionality"""
    # Set required environment variables
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
    
    # Reset config for clean test
    reset_config()
    
    # Test config creation
    config = Config()
    print(f"✅ Config created successfully")
    print(f"   Environment: {config.environment.value}")
    print(f"   Django URL: {config.django_api_url}")
    print(f"   External URL: {config.external_api_url}")
    print(f"   Log Level: {config.log_level}")
    
    # Test environment detection
    assert config.is_development is True
    assert config.django_api_url == "https://api.example.com"
    assert config.external_api_url == "https://external.example.com"
    
    print("✅ All tests passed!")

if __name__ == "__main__":
    test_config_basic() 