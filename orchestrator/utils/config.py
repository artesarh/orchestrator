"""
Dagster-native configuration using environment variables
"""

import os
from typing import Dict, Any
from enum import Enum
from pathlib import Path


class Environment(Enum):
    """Supported environments"""
    DEVELOPMENT = "dev"
    PRODUCTION = "prod"
    TESTING = "test"


def load_environment_files():
    """Load environment-specific .env files"""
    # Get the root directory (where .env files are located)
    root_dir = Path(__file__).parent.parent.parent
    
    # Always try to load .env first (base config)
    env_file = root_dir / ".env"
    if env_file.exists():
        _load_env_file(env_file)
    
    # Get current environment and load environment-specific file
    env_str = os.getenv("APP_ENV", "dev").lower()
    env_specific_file = root_dir / f".env.{env_str}"
    
    if env_specific_file.exists():
        _load_env_file(env_specific_file)
        print(f"Loaded environment config from: {env_specific_file}")
    else:
        print(f"No environment-specific config found: {env_specific_file}")


def _load_env_file(file_path: Path):
    """Load environment variables from a .env file"""
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        # Remove quotes if present
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        
                        # Only set if not already set (environment takes precedence)
                        if key not in os.environ:
                            os.environ[key] = value
    except Exception as e:
        print(f"Warning: Could not load {file_path}: {e}")


# Load environment files at import time
load_environment_files()


def get_environment() -> Environment:
    """Get current environment"""
    env_str = os.getenv("APP_ENV", "dev").lower()
    try:
        return Environment(env_str)
    except ValueError:
        return Environment.DEVELOPMENT


def get_required_env(key: str) -> str:
    """Get required environment variable"""
    value = os.getenv(key)
    if not value:
        raise ValueError(f"Required environment variable '{key}' is not set")
    return value


def get_optional_env(key: str, default: str) -> str:
    """Get optional environment variable with default"""
    return os.getenv(key, default)


def get_optional_int(key: str, default: int) -> int:
    """Get optional integer environment variable"""
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


# Environment-specific defaults
def get_django_api_url() -> str:
    """Get Django API URL with environment-specific defaults"""
    env = get_environment()
    if env == Environment.DEVELOPMENT:
        return get_optional_env("DJANGO_API_URL", "http://localhost:8000")
    elif env == Environment.PRODUCTION:
        return get_optional_env("DJANGO_API_URL", "https://api.production.com")
    elif env == Environment.TESTING:
        return get_optional_env("DJANGO_API_URL", "http://localhost:8001")
    return get_required_env("DJANGO_API_URL")


def get_external_api_url() -> str:
    """Get external API URL with environment-specific defaults"""
    env = get_environment()
    if env == Environment.DEVELOPMENT:
        return get_optional_env("EXTERNAL_API_URL", "https://dev-external-api.com")
    elif env == Environment.PRODUCTION:
        return get_optional_env("EXTERNAL_API_URL", "https://prod-external-api.com")
    elif env == Environment.TESTING:
        return get_optional_env("EXTERNAL_API_URL", "https://test-external-api.com")
    return get_required_env("EXTERNAL_API_URL")


def get_log_level() -> str:
    """Get log level with environment-specific defaults"""
    env = get_environment()
    if env == Environment.DEVELOPMENT:
        return get_optional_env("LOG_LEVEL", "DEBUG")
    elif env == Environment.PRODUCTION:
        return get_optional_env("LOG_LEVEL", "WARNING")
    elif env == Environment.TESTING:
        return get_optional_env("LOG_LEVEL", "DEBUG")
    return get_optional_env("LOG_LEVEL", "INFO")


# Required variables (no defaults) - these will fail fast if not set
DJANGO_JWT_TOKEN = get_required_env("DJANGO_JWT_TOKEN")
EXTERNAL_API_KEY = get_required_env("EXTERNAL_API_KEY")
AZURE_CONNECTION_STRING = get_required_env("AZURE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = get_required_env("AZURE_CONTAINER_NAME")
AZURE_ACCOUNT_NAME = get_required_env("AZURE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = get_required_env("AZURE_ACCOUNT_KEY")
FABRIC_WORKSPACE_ID = get_required_env("FABRIC_WORKSPACE_ID")

# Optional variables with environment-specific defaults
DJANGO_API_URL = get_django_api_url()
EXTERNAL_API_URL = get_external_api_url()
LOG_LEVEL = get_log_level()
CRON_SENSOR_INTERVAL = get_optional_int("CRON_SENSOR_INTERVAL_SECONDS", 60)
DISCOVERY_SENSOR_INTERVAL = get_optional_int("DISCOVERY_SENSOR_INTERVAL_SECONDS", 86400)
MAX_RETRIES = get_optional_int("MAX_RETRIES", 3 if get_environment() == Environment.DEVELOPMENT else 5)
TIMEOUT_SECONDS = get_optional_int("TIMEOUT_SECONDS", 5 if get_environment() == Environment.TESTING else 30) 