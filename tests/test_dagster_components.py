import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from freezegun import freeze_time

from dagster import (
    build_sensor_context,
    build_asset_context,
    AssetMaterialization,
    MetadataValue,
    RunRequest,
    SkipReason,
    SensorResult,
    instance_for_test,
)

from orchestrator.sensors.report_sensor import report_cron_sensor, daily_report_discovery_sensor
from orchestrator.assets.report_discovery import report_cron_schedules, ReportDiscoveryConfig
from orchestrator.assets.report_processing import report_data, ReportProcessingConfig


class TestReportCronSensor:
    """Test the cron-based sensor that triggers report jobs"""

    def test_sensor_skips_when_no_asset_materialization(self):
        """Test sensor returns SkipReason when no report schedules discovered"""
        with instance_for_test() as instance:
            context = build_sensor_context(instance=instance)
            
            result = report_cron_sensor(context)
            
            assert isinstance(result, SkipReason)
            assert "No report schedules discovered yet" in str(result)

    def test_sensor_skips_when_no_cron_schedules_in_metadata(self):
        """Test sensor handles asset with no cron metadata"""
        with instance_for_test() as instance:
            # Create asset materialization without cron_schedules metadata
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={"other_metadata": MetadataValue.text("some value")}
            )
            
            # Add the materialization to the instance
            instance.report_runless_asset_event(materialization)
            
            context = build_sensor_context(instance=instance)
            result = report_cron_sensor(context)
            
            assert isinstance(result, SkipReason)
            assert "No cron schedules found in asset metadata" in str(result)

    @freeze_time("2024-01-01 12:00:00")
    def test_sensor_triggers_jobs_for_due_cron_schedules(self):
        """Test sensor creates RunRequests for reports that should run"""
        with instance_for_test() as instance:
            # Mock cron schedules - one that should run, one that shouldn't
            cron_schedules = {
                "1": {
                    "cron": "0 12 * * *",  # Daily at 12:00 - should trigger now
                    "name": "Daily Report",
                },
                "2": {
                    "cron": "0 18 * * *",  # Daily at 18:00 - should not trigger
                    "name": "Evening Report",
                }
            }
            
            # Create asset materialization with cron schedules
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={
                    "cron_schedules": MetadataValue.json(cron_schedules),
                    "total_reports": MetadataValue.int(2)
                }
            )
            
            instance.report_runless_asset_event(materialization)
            
            context = build_sensor_context(instance=instance)
            
            with patch('orchestrator.sensors.report_sensor.EXTERNAL_API_URL', 'https://test-api.com'):
                with patch('orchestrator.sensors.report_sensor.EXTERNAL_API_KEY', 'test-key'):
                    result = report_cron_sensor(context)
            
            # Should return SensorResult with run requests
            assert isinstance(result, SensorResult)
            assert len(result.run_requests) == 1
            
            run_request = result.run_requests[0]
            assert isinstance(run_request, RunRequest)
            assert run_request.partition_key.startswith("report_1_")
            assert run_request.tags["report_id"] == "1"
            assert run_request.tags["report_name"] == "Daily Report"
            assert run_request.tags["cron_schedule"] == "0 12 * * *"

    @freeze_time("2024-01-01 12:30:00")  # 30 minutes past noon
    def test_sensor_timing_window_logic(self):
        """Test sensor catches jobs that should have run in the timing window"""
        with instance_for_test() as instance:
            # Cron that should have triggered at 12:00 (30 minutes ago)
            cron_schedules = {
                "1": {
                    "cron": "0 12 * * *",  # Should have run 30 minutes ago
                    "name": "Noon Report",
                }
            }
            
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={"cron_schedules": MetadataValue.json(cron_schedules)}
            )
            
            instance.report_runless_asset_event(materialization)
            context = build_sensor_context(instance=instance)
            
            with patch('orchestrator.sensors.report_sensor.CRON_SENSOR_INTERVAL', 3600):  # 1 hour window
                with patch('orchestrator.sensors.report_sensor.EXTERNAL_API_URL', 'https://test-api.com'):
                    with patch('orchestrator.sensors.report_sensor.EXTERNAL_API_KEY', 'test-key'):
                        result = report_cron_sensor(context)
            
            # Should still trigger because we're within the timing window
            assert isinstance(result, SensorResult)
            assert len(result.run_requests) == 1

    def test_sensor_handles_invalid_cron_gracefully(self):
        """Test sensor handles invalid cron expressions"""
        with instance_for_test() as instance:
            # Invalid cron expression
            cron_schedules = {
                "1": {
                    "cron": "invalid cron",
                    "name": "Broken Report",
                }
            }
            
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={"cron_schedules": MetadataValue.json(cron_schedules)}
            )
            
            instance.report_runless_asset_event(materialization)
            context = build_sensor_context(instance=instance)
            
            # Should not crash, should handle gracefully
            result = report_cron_sensor(context)
            
            # Should skip because no valid cron schedules
            assert isinstance(result, SkipReason) or (isinstance(result, SensorResult) and len(result.run_requests) == 0)

    def test_sensor_error_handling(self):
        """Test sensor handles unexpected errors gracefully"""
        with instance_for_test() as instance:
            context = build_sensor_context(instance=instance)
            
            # Mock the instance to raise an error
            with patch.object(context.instance, 'get_latest_materialization_record', side_effect=Exception("Database error")):
                result = report_cron_sensor(context)
            
            assert isinstance(result, SkipReason)
            assert "Error in sensor" in str(result)


class TestDailyReportDiscoverySensor:
    """Test the discovery sensor that triggers report discovery"""

    def test_discovery_sensor_returns_run_request(self):
        """Test discovery sensor always returns a RunRequest"""
        with instance_for_test() as instance:
            context = build_sensor_context(instance=instance)
            
            result = daily_report_discovery_sensor(context)
            
            assert isinstance(result, RunRequest)


class TestReportDiscoveryAsset:
    """Test the report discovery asset"""

    @pytest.fixture
    def mock_api_client(self):
        """Mock API client for testing"""
        mock_client = Mock()
        mock_client.get_all_reports.return_value = {
            "data": [
                {"id": 1, "name": "Report 1", "cron": "0 9 * * *"},
                {"id": 2, "name": "Report 2", "cron": "0 18 * * *"},
                {"id": 3, "name": "Report 3", "cron": None},  # No cron
                {"id": 4, "name": "Report 4", "cron": "invalid cron"},  # Invalid cron
            ]
        }
        return mock_client

    @freeze_time("2024-01-01 12:00:00")
    def test_report_discovery_asset(self, mock_api_client):
        """Test report discovery processes reports correctly"""
        context = build_asset_context()
        config = ReportDiscoveryConfig(check_new_reports=True)
        
        result = report_cron_schedules(context, config, mock_api_client)
        
        # Verify the result structure
        assert "cron_schedules" in result
        assert "valid_report_ids" in result
        assert "discovered_at" in result
        
        # Should have 2 valid cron schedules (reports 1 and 2)
        assert len(result["valid_report_ids"]) == 2
        assert "1" in result["cron_schedules"]
        assert "2" in result["cron_schedules"]
        
        # Invalid and missing cron schedules should be excluded
        assert "3" not in result["cron_schedules"]  # No cron
        assert "4" not in result["cron_schedules"]  # Invalid cron
        
        # Check metadata was added to context
        # (In a real test, you'd verify context.add_output_metadata was called)


class TestReportProcessingAsset:
    """Test the report processing assets"""

    @pytest.fixture
    def mock_api_client(self):
        """Mock API client for testing"""
        mock_client = Mock()
        mock_client.get_report_with_modifier.return_value = {
            "data": {
                "report": {"id": 1, "name": "Test Report"},
                "modifiers": {"modifier_id": 1},
                "eventgroup": {"events": []}
            },
            "meta": {"report_id": 1}
        }
        return mock_client

    def test_report_data_asset(self, mock_api_client):
        """Test report data asset fetches and transforms data"""
        context = build_asset_context()
        config = ReportProcessingConfig(
            report_id=1,
            modifier_id=1,
            external_api_url="https://test-api.com",
            external_api_key="test-key"
        )
        
        with patch('orchestrator.assets.report_processing.transform_report_schema') as mock_transform:
            mock_transform.return_value = {"transformed": "data"}
            
            result = report_data(context, config, mock_api_client)
            
            # Verify API was called correctly
            mock_api_client.get_report_with_modifier.assert_called_once_with(1, 1)
            
            # Verify transformation was applied
            mock_transform.assert_called_once()
            
            # Verify result
            assert result == {"transformed": "data"}


class TestJobIntegration:
    """Test job execution with mocked dependencies"""

    def test_job_config_structure(self):
        """Test that job config has the expected structure"""
        from orchestrator.jobs.report_pipeline import process_report_job
        
        # Verify job has the expected config structure
        job_def = process_report_job
        assert job_def.name == "process_report_job"
        
        # Check that it has partitions
        assert job_def.partitions_def is not None
        assert job_def.partitions_def.name == "reports"

    @pytest.mark.slow
    def test_job_execution_with_mocks(self):
        """Test job execution with all external dependencies mocked"""
        from orchestrator.jobs.report_pipeline import process_report_job
        from orchestrator.definitions import defs
        
        # This would be a full integration test that mocks all external APIs
        # and runs the job to completion. It's marked as slow because it
        # would actually execute the full pipeline.
        
        # For now, just verify the job definition is valid
        assert process_report_job is not None
        assert defs is not None


# Helper functions for test data
def create_test_cron_schedules():
    """Helper to create test cron schedule data"""
    return {
        "1": {"cron": "0 9 * * *", "name": "Morning Report"},
        "2": {"cron": "0 17 * * *", "name": "Evening Report"},
        "3": {"cron": "0 12 * * 1", "name": "Monday Noon Report"},
    }


def create_test_materialization(cron_schedules):
    """Helper to create test asset materialization"""
    return AssetMaterialization(
        asset_key="report_cron_schedules",
        metadata={
            "cron_schedules": MetadataValue.json(cron_schedules),
            "total_reports": MetadataValue.int(len(cron_schedules)),
            "discovery_time": MetadataValue.text(datetime.now().isoformat())
        }
    ) 