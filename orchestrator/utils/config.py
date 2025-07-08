"""
Configuration system for the orchestrator.
Uses Dagster's EnvVar for environment management.
"""

import os
from enum import Enum
from typing import Any
from dagster import EnvVar


class Environment(Enum):
    DEVELOPMENT = "dev"
    PRODUCTION = "prod"


class SchedulingApproach(Enum):
    SENSOR = "sensor"
    SCHEDULE = "schedule"
    PIPELINE = "pipeline"


def get_env() -> Environment:
    """Get current environment from APP_ENV"""
    env = EnvVar("APP_ENV").get_value()
    if not env or env.lower() not in ["dev", "prod"]:
        raise ValueError("APP_ENV must be either 'dev' or 'prod'")
    return Environment.DEVELOPMENT if env.lower() == "dev" else Environment.PRODUCTION


def get_scheduling_approach() -> SchedulingApproach:
    """Get scheduling approach from environment with fallback to schedule"""
    try:
        approach = EnvVar("SCHEDULING_APPROACH").get_value()
        if approach:
            approach_lower = approach.lower()
            if approach_lower == "sensor":
                return SchedulingApproach.SENSOR
            elif approach_lower == "pipeline":
                return SchedulingApproach.PIPELINE
            else:
                return SchedulingApproach.SCHEDULE
        return SchedulingApproach.SCHEDULE
    except Exception:
        return SchedulingApproach.SCHEDULE


# Load environment based on APP_ENV
env = get_env()
if env == Environment.DEVELOPMENT:
    os.environ["DAGSTER_ENV"] = "dev"
    from dotenv import load_dotenv

    load_dotenv(".env.dev")
else:  # PRODUCTION
    os.environ["DAGSTER_ENV"] = "prod"
    from dotenv import load_dotenv

    load_dotenv(".env")


# Main configuration dictionary using Dagster's EnvVar
CONFIG = {
    # Django API settings
    "DJANGO_API_URL": EnvVar("DJANGO_API_URL").get_value(),
    "DJANGO_JWT_TOKEN": EnvVar("DJANGO_JWT_TOKEN").get_value(),
    # Fireant API settings
    "FIREANT_API_URL": EnvVar("FIREANT_API_URL").get_value(),
    "FIREANT_API_KEY": EnvVar("FIREANT_API_KEY").get_value(),
    # Azure/Fabric settings
    "AZURE_TENANT_ID": EnvVar("AZURE_TENANT_ID").get_value(),
    "AZURE_CLIENT_ID": EnvVar("AZURE_CLIENT_ID").get_value(),
    "AZURE_CLIENT_SECRET": EnvVar("AZURE_CLIENT_SECRET").get_value(),
    "AZURE_ACCOUNT_NAME": EnvVar("AZURE_ACCOUNT_NAME").get_value(),
    "FABRIC_WORKSPACE_ID": EnvVar("FABRIC_WORKSPACE_ID").get_value(),
    # Logging
    "LOG_LEVEL": EnvVar("LOG_LEVEL").get_value(),
    # Scheduling settings
    "SCHEDULING_APPROACH": EnvVar("SCHEDULING_APPROACH").get_value(),
    "CRON_SENSOR_INTERVAL_SECONDS": EnvVar("CRON_SENSOR_INTERVAL_SECONDS").get_value(),
    "DISCOVERY_SENSOR_INTERVAL_SECONDS": EnvVar(
        "DISCOVERY_SENSOR_INTERVAL_SECONDS"
    ).get_value(),
    "SCHEDULE_CRON_EXPRESSION": EnvVar("SCHEDULE_CRON_EXPRESSION").get_value(),
    "SCHEDULE_INTERVAL_SECONDS": EnvVar("SCHEDULE_INTERVAL_SECONDS").get_value(),
    # Polling settings
    "MAX_POLL_ATTEMPTS": EnvVar("MAX_POLL_ATTEMPTS").get_value(),
    "POLL_INTERVAL_SECONDS": EnvVar("POLL_INTERVAL_SECONDS").get_value(),
    # Retry settings
    "MAX_RETRIES": EnvVar("MAX_RETRIES").get_value(),
    "TIMEOUT_SECONDS": EnvVar("TIMEOUT_SECONDS").get_value(),
}


# Helper functions to access config
def get_config(key: str, default: Any = None) -> Any:
    """Get a config value with optional default"""
    return CONFIG.get(key, default)


def require_config(key: str) -> Any:
    """Get a required config value"""
    if key not in CONFIG:
        raise KeyError(f"Required config key '{key}' not found")
    return CONFIG[key]


# Export commonly used values
DJANGO_API_URL = CONFIG["DJANGO_API_URL"]
DJANGO_JWT_TOKEN = CONFIG["DJANGO_JWT_TOKEN"]
FIREANT_API_URL = CONFIG["FIREANT_API_URL"]
FIREANT_API_KEY = CONFIG["FIREANT_API_KEY"]
AZURE_TENANT_ID = CONFIG["AZURE_TENANT_ID"]
AZURE_CLIENT_ID = CONFIG["AZURE_CLIENT_ID"]
AZURE_CLIENT_SECRET = CONFIG["AZURE_CLIENT_SECRET"]
AZURE_ACCOUNT_NAME = CONFIG["AZURE_ACCOUNT_NAME"]
FABRIC_WORKSPACE_ID = CONFIG["FABRIC_WORKSPACE_ID"]
MAX_POLL_ATTEMPTS = CONFIG["MAX_POLL_ATTEMPTS"]
POLL_INTERVAL_SECONDS = CONFIG["POLL_INTERVAL_SECONDS"]
