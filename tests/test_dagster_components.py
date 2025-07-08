import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from freezegun import freeze_time

from dagster import (
    build_sensor_context,
    build_schedule_context,
    build_asset_context,
    AssetMaterialization,
    MetadataValue,
    RunRequest,
    SkipReason,
    SensorResult,
    instance_for_test,
)

from orchestrator.sensors.report_sensor import report_cron_sensor, daily_report_discovery_sensor
from orchestrator.schedules.dynamic_schedules import unified_report_schedule
from orchestrator.assets.report_discovery import report_cron_schedules, ReportDiscoveryConfig
from orchestrator.assets.report_processing import report_data, ReportProcessingConfig
from orchestrator.utils.config import SchedulingApproach


@pytest.fixture
def mock_cron_schedules():
    """Helper fixture to create test cron schedules"""
    return {
        "1": {
            "cron": "0 12 * * *",  # Daily at 12:00
            "name": "Daily Report",
        },
        "2": {
            "cron": "0 18 * * *",  # Daily at 18:00
            "name": "Evening Report",
        },
        "3": {
            "cron": "0 0 * * 0",  # Weekly on Sunday at midnight
            "name": "Weekly Report",
        }
    }


@pytest.fixture
def mock_asset_materialization(mock_cron_schedules):
    """Helper fixture to create test asset materialization"""
    return AssetMaterialization(
        asset_key="report_cron_schedules",
        metadata={
            "cron_schedules": MetadataValue.json(mock_cron_schedules),
            "total_reports": MetadataValue.int(len(mock_cron_schedules)),
            "discovery_time": MetadataValue.text(datetime.now().isoformat())
        }
    )


class TestUnifiedReportSchedule:
    """Test the unified schedule that handles all reports"""

    def test_schedule_skips_when_no_asset_materialization(self):
        """Test schedule returns SkipReason when no report schedules discovered"""
        with instance_for_test() as instance:
            context = build_schedule_context(instance=instance)
            result = unified_report_schedule(context)
            assert isinstance(result, SkipReason)
            assert "No report schedules discovered yet" in str(result)

    def test_schedule_skips_when_no_cron_schedules(self):
        """Test schedule handles asset with no cron metadata"""
        with instance_for_test() as instance:
            # Create asset materialization without cron_schedules metadata
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={"other_metadata": MetadataValue.text("some value")}
            )
            instance.report_runless_asset_event(materialization)
            
            context = build_schedule_context(instance=instance)
            result = unified_report_schedule(context)
            
            assert isinstance(result, SkipReason)
            assert "No cron schedules found" in str(result)

    @freeze_time("2024-01-01 12:00:00")
    def test_schedule_creates_run_requests_for_due_reports(self, mock_asset_materialization):
        """Test schedule creates RunRequests for reports that should run"""
        with instance_for_test() as instance:
            instance.report_runless_asset_event(mock_asset_materialization)
            context = build_schedule_context(instance=instance)
            
            with patch('orchestrator.schedules.dynamic_schedules.EXTERNAL_API_URL', 'https://test-api.com'):
                with patch('orchestrator.schedules.dynamic_schedules.EXTERNAL_API_KEY', 'test-key'):
                    result = unified_report_schedule(context)
            
            # Should return list of RunRequests
            assert isinstance(result, list)
            assert len(result) == 1  # Only report 1 should run at 12:00
            
            run_request = result[0]
            assert isinstance(run_request, RunRequest)
            assert run_request.partition_key.startswith("report_1_")
            assert run_request.tags["report_id"] == "1"
            assert run_request.tags["report_name"] == "Daily Report"
            assert run_request.tags["cron_schedule"] == "0 12 * * *"

    @freeze_time("2024-01-01 18:00:00")
    def test_schedule_handles_multiple_reports_same_time(self, mock_asset_materialization):
        """Test schedule handles multiple reports due at the same time"""
        with instance_for_test() as instance:
            # Add another report at 18:00
            cron_schedules = mock_asset_materialization.metadata["cron_schedules"].value
            cron_schedules["4"] = {
                "cron": "0 18 * * *",  # Also at 18:00
                "name": "Another Evening Report"
            }
            
            instance.report_runless_asset_event(mock_asset_materialization)
            context = build_schedule_context(instance=instance)
            
            with patch('orchestrator.schedules.dynamic_schedules.EXTERNAL_API_URL', 'https://test-api.com'):
                with patch('orchestrator.schedules.dynamic_schedules.EXTERNAL_API_KEY', 'test-key'):
                    result = unified_report_schedule(context)
            
            assert isinstance(result, list)
            assert len(result) == 2  # Both evening reports should run
            
            report_ids = {r.tags["report_id"] for r in result}
            assert report_ids == {"2", "4"}

    def test_schedule_error_handling(self):
        """Test schedule handles unexpected errors gracefully"""
        with instance_for_test() as instance:
            context = build_schedule_context(instance=instance)
            
            # Mock the instance to raise an error
            with patch.object(context.instance, 'get_event_records', side_effect=Exception("Database error")):
                result = unified_report_schedule(context)
            
            assert isinstance(result, SkipReason)
            assert "Error in schedule" in str(result)


@pytest.mark.legacy
class TestReportCronSensor:
    """Test the legacy cron-based sensor that triggers report jobs"""

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
                {
                    "url": "http://localhost:8000/api/reports/1/",
                    "name": "Report 1",
                    "cron": "0 9 * * *",
                    "event_group": 1
                },
                {
                    "url": "http://localhost:8000/api/reports/2/",
                    "name": "Report 2",
                    "cron": "0 18 * * *",
                    "event_group": 2
                },
                {
                    "url": "http://localhost:8000/api/reports/3/",
                    "name": "Report 3",
                    "cron": None,  # No cron
                    "event_group": 3
                },
                {
                    "url": "http://localhost:8000/api/reports/4/",
                    "name": "Report 4",
                    "cron": "invalid cron",  # Invalid cron
                    "event_group": 4
                },
                {
                    "url": "malformed/url",  # Invalid URL format
                    "name": "Bad Report",
                    "cron": "0 12 * * *",
                    "event_group": 5
                }
            ]
        }
        return mock_client

    @freeze_time("2024-01-01 12:00:00")
    def test_report_discovery_metadata_structure(self, mock_api_client):
        """Test that report discovery produces correctly structured metadata"""
        context = build_asset_context()
        config = ReportDiscoveryConfig(check_new_reports=True)
        
        # Capture metadata added to context
        added_metadata = {}
        def mock_add_output_metadata(metadata):
            added_metadata.update(metadata)
        context.add_output_metadata = mock_add_output_metadata
        
        result = report_cron_schedules(context, config, mock_api_client)
        
        # Check result structure
        assert "cron_schedules" in result
        assert "valid_report_ids" in result
        assert "discovered_at" in result
        
        # Verify metadata was added with correct structure
        assert "cron_schedules" in added_metadata
        assert "total_reports" in added_metadata
        assert "discovery_time" in added_metadata
        
        # Verify metadata types
        assert isinstance(added_metadata["cron_schedules"], MetadataValue)
        assert isinstance(added_metadata["total_reports"], MetadataValue)
        assert isinstance(added_metadata["discovery_time"], MetadataValue)
        
        # Verify cron_schedules content
        cron_data = added_metadata["cron_schedules"].value
        assert isinstance(cron_data, dict)
        assert "1" in cron_data  # Report 1 should be included
        assert "2" in cron_data  # Report 2 should be included
        assert "3" not in cron_data  # Report 3 had no cron
        assert "4" not in cron_data  # Report 4 had invalid cron
        assert "5" not in cron_data  # Report 5 had invalid URL
        
        # Verify schedule entries
        for report_id, schedule_info in cron_data.items():
            assert "cron" in schedule_info
            assert "name" in schedule_info
            assert isinstance(schedule_info["cron"], str)
            assert isinstance(schedule_info["name"], str)

    def test_report_discovery_handles_url_based_ids(self, mock_api_client):
        """Test that report IDs are correctly extracted from URLs"""
        context = build_asset_context()
        config = ReportDiscoveryConfig(check_new_reports=True)
        
        # Add a report with a complex URL
        mock_api_client.get_all_reports.return_value["data"].append({
            "url": "https://api.example.com/v2/api/reports/42/?format=json",
            "name": "Complex URL Report",
            "cron": "0 12 * * *",
            "event_group": 6
        })
        
        result = report_cron_schedules(context, config, mock_api_client)
        
        # Verify the ID was correctly extracted
        assert "42" in result["cron_schedules"]
        schedule_info = result["cron_schedules"]["42"]
        assert schedule_info["name"] == "Complex URL Report"
        assert schedule_info["cron"] == "0 12 * * *"

    def test_report_discovery_invalid_report_structure(self, mock_api_client):
        """Test handling of malformed report data"""
        context = build_asset_context()
        config = ReportDiscoveryConfig(check_new_reports=True)
        
        # Test various malformed data scenarios
        test_cases = [
            {},  # Empty response
            {"data": []},  # Empty data list
            {"data": [{}]},  # Empty report object
            {"data": [{"url": None, "name": "Bad Report"}]},  # Missing URL
            {"data": [{"url": "/reports/abc/", "name": "Non-numeric ID"}]},  # Non-numeric ID
        ]
        
        for test_case in test_cases:
            mock_api_client.get_all_reports.return_value = test_case
            result = report_cron_schedules(context, config, mock_api_client)
            
            # Should handle gracefully and return empty but valid structure
            assert "cron_schedules" in result
            assert "valid_report_ids" in result
            assert isinstance(result["cron_schedules"], dict)
            assert isinstance(result["valid_report_ids"], list)

    def test_report_discovery_cron_validation(self, mock_api_client):
        """Test validation of cron expressions"""
        context = build_asset_context()
        config = ReportDiscoveryConfig(check_new_reports=True)
        
        # Test various cron expressions
        test_cases = [
            ("0 9 * * *", True),  # Valid: Every day at 9 AM
            ("*/15 * * * *", True),  # Valid: Every 15 minutes
            ("0 0 * * SUN", True),  # Valid: Weekly on Sunday
            ("invalid", False),  # Invalid format
            ("0 24 * * *", False),  # Invalid hour
            ("0 0 0 * *", False),  # Invalid day
            ("* * * *", False),  # Missing field
            ("0 9 * * MON-INVALID", False),  # Invalid day range
        ]
        
        for cron_expr, should_be_valid in test_cases:
            mock_api_client.get_all_reports.return_value = {
                "data": [{
                    "url": "http://localhost:8000/api/reports/1/",
                    "name": "Test Report",
                    "cron": cron_expr,
                    "event_group": 1
                }]
            }
            
            result = report_cron_schedules(context, config, mock_api_client)
            
            if should_be_valid:
                assert "1" in result["cron_schedules"], f"Valid cron '{cron_expr}' was rejected"
                assert result["cron_schedules"]["1"]["cron"] == cron_expr
            else:
                assert "1" not in result["cron_schedules"], f"Invalid cron '{cron_expr}' was accepted"


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


class TestSchedulingErrorCases:
    """Test error handling in both scheduling approaches"""

    @pytest.fixture
    def mock_asset_materialization(self):
        """Create a test asset materialization"""
        return AssetMaterialization(
            asset_key="report_cron_schedules",
            metadata={
                "cron_schedules": MetadataValue.json({
                    "1": {"cron": "0 12 * * *", "name": "Test Report 1"},
                    "2": {"cron": "0 18 * * *", "name": "Test Report 2"}
                }),
                "total_reports": MetadataValue.int(2),
                "discovery_time": MetadataValue.text(datetime.now().isoformat())
            }
        )

    def test_schedule_handles_missing_metadata_fields(self):
        """Test schedule handles missing metadata fields gracefully"""
        with instance_for_test() as instance:
            # Create asset materialization without required fields
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={
                    "some_other_field": MetadataValue.text("value")
                }
            )
            instance.report_runless_asset_event(materialization)
            
            context = build_schedule_context(instance=instance)
            result = unified_report_schedule(context)
            
            assert isinstance(result, SkipReason)
            assert "No cron schedules found" in str(result)

    def test_schedule_handles_invalid_metadata_types(self):
        """Test schedule handles invalid metadata types gracefully"""
        with instance_for_test() as instance:
            # Create asset materialization with wrong metadata types
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={
                    "cron_schedules": MetadataValue.text("invalid"),  # Should be JSON
                    "total_reports": MetadataValue.text("not a number"),  # Should be int
                }
            )
            instance.report_runless_asset_event(materialization)
            
            context = build_schedule_context(instance=instance)
            result = unified_report_schedule(context)
            
            assert isinstance(result, SkipReason)
            assert "Error in schedule" in str(result)

    def test_sensor_handles_missing_metadata_fields(self):
        """Test sensor handles missing metadata fields gracefully"""
        with instance_for_test() as instance:
            # Create asset materialization without required fields
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={
                    "some_other_field": MetadataValue.text("value")
                }
            )
            instance.report_runless_asset_event(materialization)
            
            context = build_sensor_context(instance=instance)
            result = report_cron_sensor(context)
            
            assert isinstance(result, SkipReason)
            assert "No cron schedules found" in str(result)

    def test_sensor_handles_invalid_metadata_types(self):
        """Test sensor handles invalid metadata types gracefully"""
        with instance_for_test() as instance:
            # Create asset materialization with wrong metadata types
            materialization = AssetMaterialalization(
                asset_key="report_cron_schedules",
                metadata={
                    "cron_schedules": MetadataValue.text("invalid"),  # Should be JSON
                    "total_reports": MetadataValue.text("not a number"),  # Should be int
                }
            )
            instance.report_runless_asset_event(materialization)
            
            context = build_sensor_context(instance=instance)
            result = report_cron_sensor(context)
            
            assert isinstance(result, SkipReason)
            assert "Error in sensor" in str(result)

    @freeze_time("2024-01-01 12:00:00")
    def test_schedule_handles_database_errors(self):
        """Test schedule handles database errors gracefully"""
        with instance_for_test() as instance:
            # Setup valid asset materialization
            instance.report_runless_asset_event(self.mock_asset_materialization())
            context = build_schedule_context(instance=instance)
            
            # Mock database error
            with patch.object(context.instance, 'get_event_records', side_effect=Exception("Database error")):
                result = unified_report_schedule(context)
                
                assert isinstance(result, SkipReason)
                assert "Error in schedule" in str(result)
                assert "Database error" in str(result)

    @freeze_time("2024-01-01 12:00:00")
    def test_sensor_handles_database_errors(self):
        """Test sensor handles database errors gracefully"""
        with instance_for_test() as instance:
            # Setup valid asset materialization
            instance.report_runless_asset_event(self.mock_asset_materialization())
            context = build_sensor_context(instance=instance)
            
            # Mock database error
            with patch.object(context.instance, 'get_event_records', side_effect=Exception("Database error")):
                result = report_cron_sensor(context)
                
                assert isinstance(result, SkipReason)
                assert "Error in sensor" in str(result)
                assert "Database error" in str(result)

    def test_schedule_handles_invalid_json_in_metadata(self):
        """Test schedule handles invalid JSON in metadata gracefully"""
        with instance_for_test() as instance:
            # Create asset materialization with invalid JSON
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={
                    "cron_schedules": MetadataValue.text("{invalid json}"),
                    "total_reports": MetadataValue.int(1)
                }
            )
            instance.report_runless_asset_event(materialization)
            
            context = build_schedule_context(instance=instance)
            result = unified_report_schedule(context)
            
            assert isinstance(result, SkipReason)
            assert "Error in schedule" in str(result)

    def test_sensor_handles_invalid_json_in_metadata(self):
        """Test sensor handles invalid JSON in metadata gracefully"""
        with instance_for_test() as instance:
            # Create asset materialization with invalid JSON
            materialization = AssetMaterialization(
                asset_key="report_cron_schedules",
                metadata={
                    "cron_schedules": MetadataValue.text("{invalid json}"),
                    "total_reports": MetadataValue.int(1)
                }
            )
            instance.report_runless_asset_event(materialization)
            
            context = build_sensor_context(instance=instance)
            result = report_cron_sensor(context)
            
            assert isinstance(result, SkipReason)
            assert "Error in sensor" in str(result)

    @freeze_time("2024-01-01 12:00:00")
    def test_both_approaches_handle_partition_errors(self):
        """Test both approaches handle partition creation errors gracefully"""
        with instance_for_test() as instance:
            # Setup valid asset materialization
            instance.report_runless_asset_event(self.mock_asset_materialization())
            
            # Mock partition error
            error_msg = "Failed to create partition"
            with patch.object(instance, 'add_dynamic_partitions', side_effect=Exception(error_msg)):
                # Test schedule
                schedule_context = build_schedule_context(instance=instance)
                schedule_result = unified_report_schedule(schedule_context)
                assert isinstance(schedule_result, SkipReason)
                assert error_msg in str(schedule_result)
                
                # Test sensor
                sensor_context = build_sensor_context(instance=instance)
                sensor_result = report_cron_sensor(sensor_context)
                assert isinstance(sensor_result, SkipReason)
                assert error_msg in str(sensor_result) 