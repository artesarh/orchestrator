import os
from typing import Any, Optional

def get_env_int(key: str, default: int) -> int:
    """Get environment variable as integer with default fallback"""
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default

def get_env_str(key: str, default: str) -> str:
    """Get environment variable as string with default fallback"""
    return os.getenv(key, default)

def get_env_bool(key: str, default: bool) -> bool:
    """Get environment variable as boolean with default fallback"""
    value = os.getenv(key, str(default)).lower()
    return value in ('true', '1', 'yes', 'on')

# Sensor configuration
CRON_SENSOR_INTERVAL = get_env_int("CRON_SENSOR_INTERVAL_SECONDS", 60)
DISCOVERY_SENSOR_INTERVAL = get_env_int("DISCOVERY_SENSOR_INTERVAL_SECONDS", 86400)

# API configuration  
DJANGO_API_URL = get_env_str("DJANGO_API_URL", "http://localhost:8000")
DJANGO_JWT_TOKEN = get_env_str("DJANGO_JWT_TOKEN", "your-jwt-token")
EXTERNAL_API_URL = get_env_str("EXTERNAL_API_URL", "https://external-service.com/api")
EXTERNAL_API_KEY = get_env_str("EXTERNAL_API_KEY", "your-external-api-key")

# Azure configuration
AZURE_CONNECTION_STRING = get_env_str("AZURE_CONNECTION_STRING", "your-azure-connection-string")
AZURE_CONTAINER_NAME = get_env_str("AZURE_CONTAINER_NAME", "your-container-name") 