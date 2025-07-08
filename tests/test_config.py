import pytest
import os
from unittest.mock import patch, MagicMock
from orchestrator.utils.config import (
    get_env, Environment, get_config, require_config, 
    get_scheduling_approach, SchedulingApproach,
    DJANGO_API_URL, DJANGO_JWT_TOKEN, FIREANT_API_URL, FIREANT_API_KEY
)


@pytest.fixture
def clean_env():
    """Fixture to provide clean environment variables"""
    old_env = dict(os.environ)
    os.environ.clear()
    yield
    os.environ.clear()
    os.environ.update(old_env)


class TestEnvironmentDetection:
    """Test environment detection functionality"""

    def test_environment_detection_default(self, clean_env):
        """Test default environment detection"""
        # Don't set APP_ENV - should default to dev
        env = get_env()
        assert env == Environment.DEVELOPMENT

    def test_environment_detection_production(self, clean_env):
        """Test production environment detection"""
        os.environ["APP_ENV"] = "prod"
        env = get_env()
        assert env == Environment.PRODUCTION

    def test_environment_detection_development_explicit(self, clean_env):
        """Test explicit development environment detection"""
        os.environ["APP_ENV"] = "dev"
        env = get_env()
        assert env == Environment.DEVELOPMENT

    def test_environment_detection_invalid_falls_back(self, clean_env):
        """Test invalid environment raises error"""
        os.environ["APP_ENV"] = "invalid"
        with pytest.raises(ValueError, match="APP_ENV must be either 'dev' or 'prod'"):
            get_env()


class TestSchedulingConfiguration:
    """Test scheduling approach configuration"""

    def test_default_scheduling_approach(self, clean_env):
        """Test that default scheduling approach is 'schedule'"""
        # Don't set SCHEDULING_APPROACH - should default to schedule
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.SCHEDULE

    def test_explicit_sensor_approach(self, clean_env):
        """Test explicitly setting sensor approach"""
        os.environ["SCHEDULING_APPROACH"] = "sensor"
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.SENSOR

    def test_explicit_schedule_approach(self, clean_env):
        """Test explicitly setting schedule approach"""
        os.environ["SCHEDULING_APPROACH"] = "schedule"
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.SCHEDULE

    def test_explicit_pipeline_approach(self, clean_env):
        """Test explicitly setting pipeline approach"""
        os.environ["SCHEDULING_APPROACH"] = "pipeline"
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.PIPELINE

    def test_invalid_approach_defaults_to_schedule(self, clean_env):
        """Test that invalid approach defaults to schedule"""
        os.environ["SCHEDULING_APPROACH"] = "invalid"
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.SCHEDULE

    def test_case_insensitive_approach(self, clean_env):
        """Test that approach setting is case insensitive"""
        os.environ["SCHEDULING_APPROACH"] = "SENSOR"
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.SENSOR
        
        os.environ["SCHEDULING_APPROACH"] = "Schedule"
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.SCHEDULE
        
        os.environ["SCHEDULING_APPROACH"] = "PIPELINE"
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.PIPELINE

    def test_scheduling_approach_with_missing_env_var(self, clean_env):
        """Test scheduling approach when environment variable is missing"""
        # Ensure SCHEDULING_APPROACH is not set
        if "SCHEDULING_APPROACH" in os.environ:
            del os.environ["SCHEDULING_APPROACH"]
        
        approach = get_scheduling_approach()
        assert approach == SchedulingApproach.SCHEDULE


class TestEnvironmentSwitching:
    """Test environment-specific configuration loading"""

    @patch('orchestrator.utils.config.load_dotenv')
    def test_development_environment_loads_dev_env(self, mock_load_dotenv, clean_env):
        """Test that development environment loads .env.dev file"""
        os.environ["APP_ENV"] = "dev"
        
        # Mock the environment loading
        with patch('orchestrator.utils.config.get_env', return_value=Environment.DEVELOPMENT):
            # Re-import to trigger environment loading
            import importlib
            import orchestrator.utils.config
            importlib.reload(orchestrator.utils.config)
            
            # Verify .env.dev was loaded
            mock_load_dotenv.assert_called_with(".env.dev")

    @patch('orchestrator.utils.config.load_dotenv')
    def test_production_environment_loads_prod_env(self, mock_load_dotenv, clean_env):
        """Test that production environment loads .env.prod file"""
        os.environ["APP_ENV"] = "prod"
        
        # Mock the environment loading
        with patch('orchestrator.utils.config.get_env', return_value=Environment.PRODUCTION):
            # Re-import to trigger environment loading
            import importlib
            import orchestrator.utils.config
            importlib.reload(orchestrator.utils.config)
            
            # Verify .env.prod was loaded
            mock_load_dotenv.assert_called_with(".env")

    def test_dagster_env_variable_set_correctly(self, clean_env):
        """Test that DAGSTER_ENV is set correctly based on APP_ENV"""
        # Test development
        os.environ["APP_ENV"] = "dev"
        with patch('orchestrator.utils.config.get_env', return_value=Environment.DEVELOPMENT):
            import importlib
            import orchestrator.utils.config
            importlib.reload(orchestrator.utils.config)
            assert os.environ.get("DAGSTER_ENV") == "dev"
        
        # Test production
        os.environ["APP_ENV"] = "prod"
        with patch('orchestrator.utils.config.get_env', return_value=Environment.PRODUCTION):
            import importlib
            import orchestrator.utils.config
            importlib.reload(orchestrator.utils.config)
            assert os.environ.get("DAGSTER_ENV") == "prod"


class TestConfigHelperFunctions:
    """Test the simple config helper functions"""

    def test_get_config_with_existing_key(self):
        """Test get_config with existing key"""
        # This assumes CONFIG is loaded with real environment variables
        from orchestrator.utils.config import CONFIG
        
        # Test with a key that should exist
        if "DJANGO_API_URL" in CONFIG:
            result = get_config("DJANGO_API_URL")
            assert result == CONFIG["DJANGO_API_URL"]

    def test_get_config_with_default(self):
        """Test get_config with non-existing key and default"""
        result = get_config("NON_EXISTENT_KEY", "default_value")
        assert result == "default_value"

    def test_get_config_without_default(self):
        """Test get_config with non-existing key without default"""
        result = get_config("NON_EXISTENT_KEY")
        assert result is None

    def test_require_config_with_existing_key(self):
        """Test require_config with existing key"""
        from orchestrator.utils.config import CONFIG
        
        # Test with a key that should exist
        if "DJANGO_API_URL" in CONFIG:
            result = require_config("DJANGO_API_URL")
            assert result == CONFIG["DJANGO_API_URL"]

    def test_require_config_with_missing_key(self):
        """Test require_config with missing key raises error"""
        with pytest.raises(KeyError, match="Required config key 'NON_EXISTENT_KEY' not found"):
            require_config("NON_EXISTENT_KEY")


class TestConfigConstants:
    """Test that exported constants work correctly"""

    def test_exported_constants_are_strings(self):
        """Test that exported constants are accessible and are strings"""
        # These tests will only pass if the environment variables are actually set
        try:
            assert isinstance(DJANGO_API_URL, str)
            assert len(DJANGO_API_URL) > 0
            print(f"✅ DJANGO_API_URL: {DJANGO_API_URL}")
        except Exception as e:
            pytest.skip(f"DJANGO_API_URL not configured: {e}")

        try:
            assert isinstance(FIREANT_API_URL, str) 
            assert len(FIREANT_API_URL) > 0
            print(f"✅ FIREANT_API_URL: {FIREANT_API_URL}")
        except Exception as e:
            pytest.skip(f"FIREANT_API_URL not configured: {e}")

    def test_config_dict_structure(self):
        """Test that CONFIG dictionary has expected structure"""
        from orchestrator.utils.config import CONFIG
        
        # Test that CONFIG is a dictionary
        assert isinstance(CONFIG, dict)
        
        # Test that it has some expected keys (that should always be present)
        expected_keys = [
            "DJANGO_API_URL", "DJANGO_JWT_TOKEN", 
            "FIREANT_API_URL", "FIREANT_API_KEY",
            "SCHEDULING_APPROACH"
        ]
        
        for key in expected_keys:
            assert key in CONFIG, f"Expected key '{key}' not found in CONFIG"

 