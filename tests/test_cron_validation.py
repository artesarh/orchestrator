#!/usr/bin/env python3
"""
Comprehensive tests for cron syntax validation and timing
Tests various cron expressions and their execution timing
"""

import pytest
from datetime import datetime, timedelta
from freezegun import freeze_time
from croniter import croniter
from unittest.mock import patch, MagicMock

from orchestrator.assets.report_discovery import report_cron_schedules, ReportDiscoveryConfig
from orchestrator.sensors.report_sensor import report_cron_sensor
from orchestrator.schedules.dynamic_schedules import unified_report_schedule
from dagster import (
    build_asset_context,
    build_sensor_context,
    build_schedule_context,
    instance_for_test,
    AssetMaterialization,
    MetadataValue,
    RunRequest,
    SkipReason,
)


class TestCronSyntaxValidation:
    """Test validation of various cron expressions"""

    def test_valid_cron_expressions(self):
        """Test that valid cron expressions are accepted"""
        valid_crons = [
            "0 9 * * *",      # Daily at 9 AM
            "0 18 * * *",     # Daily at 6 PM
            "0 0 * * 0",      # Weekly on Sunday at midnight
            "0 0 1 * *",      # Monthly on the 1st at midnight
            "0 */2 * * *",    # Every 2 hours
            "30 8 * * 1-5",   # Weekdays at 8:30 AM
            "0 9 * * MON",    # Mondays at 9 AM
            "0 0 1 1 *",      # January 1st at midnight
            "*/15 * * * *",   # Every 15 minutes
            "0 8,12,16 * * *", # 8 AM, 12 PM, 4 PM daily
        ]
        
        for cron_expr in valid_crons:
            try:
                # This should not raise an exception
                cron = croniter(cron_expr)
                next_run = cron.get_next(datetime)
                assert isinstance(next_run, datetime)
                print(f"✅ Valid cron: {cron_expr}")
            except Exception as e:
                pytest.fail(f"Valid cron expression '{cron_expr}' failed validation: {e}")

    def test_invalid_cron_expressions(self):
        """Test that invalid cron expressions are rejected"""
        invalid_crons = [
            "invalid",           # Not a cron expression
            "60 * * * *",       # Invalid minute (60)
            "* 25 * * *",       # Invalid hour (25)
            "* * 32 * *",       # Invalid day (32)
            "* * * 13 *",       # Invalid month (13)
            "* * * * 8",        # Invalid weekday (8)
            "* * * * MON-FRI-SAT", # Invalid range
            "",                 # Empty string
            "* * * *",          # Missing field
            "* * * * * *",      # Too many fields
        ]
        
        for cron_expr in invalid_crons:
            with pytest.raises(Exception):
                cron = croniter(cron_expr)
                # Some invalid expressions might pass croniter() but fail on get_next()
                cron.get_next(datetime)
                print(f"❌ Should have failed: {cron_expr}")

    def test_cron_next_execution_timing(self):
        """Test that cron expressions calculate next execution correctly"""
        test_cases = [
            {
                "cron": "0 9 * * *",
                "current_time": "2024-01-01 08:00:00",
                "expected_next": "2024-01-01 09:00:00",
                "description": "Daily at 9 AM - should run at 9 AM today"
            },
            {
                "cron": "0 9 * * *",
                "current_time": "2024-01-01 10:00:00",
                "expected_next": "2024-01-02 09:00:00",
                "description": "Daily at 9 AM - should run at 9 AM tomorrow"
            },
            {
                "cron": "0 0 * * 0",
                "current_time": "2024-01-06 12:00:00",  # Saturday
                "expected_next": "2024-01-07 00:00:00",  # Sunday
                "description": "Weekly on Sunday - should run Sunday midnight"
            },
            {
                "cron": "0 8,12,16 * * *",
                "current_time": "2024-01-01 10:00:00",
                "expected_next": "2024-01-01 12:00:00",
                "description": "Multiple times daily - should run at next scheduled time"
            },
            {
                "cron": "30 8 * * 1-5",
                "current_time": "2024-01-06 07:00:00",  # Saturday
                "expected_next": "2024-01-08 08:30:00",  # Monday
                "description": "Weekdays only - should skip weekend"
            },
        ]
        
        for case in test_cases:
            current_time = datetime.strptime(case["current_time"], "%Y-%m-%d %H:%M:%S")
            expected_next = datetime.strptime(case["expected_next"], "%Y-%m-%d %H:%M:%S")
            
            cron = croniter(case["cron"], current_time)
            actual_next = cron.get_next(datetime)
            
            assert actual_next == expected_next, (
                f"Cron timing mismatch for '{case['cron']}' - {case['description']}\n"
                f"Current: {current_time}\n"
                f"Expected: {expected_next}\n"
                f"Actual: {actual_next}"
            )
            print(f"✅ {case['description']}: {case['cron']}")


class TestReportDiscoveryWithCronValidation:
    """Test report discovery with various cron expressions"""

    @pytest.fixture
    def mock_api_client(self):
        """Mock API client for testing"""
        mock = MagicMock()
        mock.get_all_reports.return_value = {
            "data": [
                {"id": 1, "name": "Valid Daily Report", "cron": "0 9 * * *"},
                {"id": 2, "name": "Valid Weekly Report", "cron": "0 0 * * 0"},
                {"id": 3, "name": "Invalid Cron Report", "cron": "invalid-cron"},
                {"id": 4, "name": "No Cron Report", "cron": None},
                {"id": 5, "name": "Empty Cron Report", "cron": ""},
                {"id": 6, "name": "Complex Valid Cron", "cron": "30 8 * * 1-5"},
            ]
        }
        return mock

    def test_report_discovery_filters_invalid_crons(self, mock_api_client):
        """Test that report discovery filters out reports with invalid cron expressions"""
        config = ReportDiscoveryConfig()
        context = build_asset_context()
        
        result = report_cron_schedules(context, config, mock_api_client)
        
        # Should only include reports with valid cron expressions
        valid_reports = result["valid_report_ids"]
        cron_schedules = result["cron_schedules"]
        
        # Should have 3 valid reports (1, 2, 6)
        assert len(valid_reports) == 3
        assert 1 in valid_reports
        assert 2 in valid_reports
        assert 6 in valid_reports
        
        # Should not include reports with invalid/missing cron
        assert 3 not in valid_reports  # invalid cron
        assert 4 not in valid_reports  # None cron
        assert 5 not in valid_reports  # empty cron
        
        # Check cron schedules structure
        assert len(cron_schedules) == 3
        assert cron_schedules[1]["cron"] == "0 9 * * *"
        assert cron_schedules[2]["cron"] == "0 0 * * 0"
        assert cron_schedules[6]["cron"] == "30 8 * * 1-5"

    def test_report_discovery_includes_report_names(self, mock_api_client):
        """Test that report discovery includes report names in the output"""
        config = ReportDiscoveryConfig()
        context = build_asset_context()
        
        result = report_cron_schedules(context, config, mock_api_client)
        cron_schedules = result["cron_schedules"]
        
        assert cron_schedules[1]["name"] == "Valid Daily Report"
        assert cron_schedules[2]["name"] == "Valid Weekly Report"
        assert cron_schedules[6]["name"] == "Complex Valid Cron"


class TestCronBasedSchedulingTiming:
    """Test that sensors and schedules trigger at correct times based on cron"""

    @pytest.fixture
    def mock_cron_asset_materialization(self):
        """Create a mock asset materialization with test cron schedules"""
        cron_schedules = {
            "1": {"cron": "0 9 * * *", "name": "Daily 9 AM Report"},
            "2": {"cron": "0 18 * * *", "name": "Daily 6 PM Report"},
            "3": {"cron": "0 0 * * 0", "name": "Weekly Sunday Report"},
            "4": {"cron": "*/15 * * * *", "name": "Every 15 Minutes Report"},
        }
        
        return AssetMaterialization(
            asset_key="report_cron_schedules",
            metadata={
                "cron_schedules": MetadataValue.json(cron_schedules),
                "total_reports": MetadataValue.int(len(cron_schedules)),
            }
        )

    @freeze_time("2024-01-01 09:00:00")  # Monday 9 AM
    def test_sensor_triggers_at_correct_cron_time(self, mock_cron_asset_materialization):
        """Test that sensor triggers reports at the correct cron time"""
        with instance_for_test() as instance:
            instance.report_runless_asset_event(mock_cron_asset_materialization)
            context = build_sensor_context(instance=instance)
            
            with patch('orchestrator.sensors.report_sensor.CONFIG', {
                'CRON_SENSOR_INTERVAL_SECONDS': '60',
                'FIREANT_API_URL': 'https://test-api.com',
                'FIREANT_API_KEY': 'test-key'
            }):
                result = report_cron_sensor(context)
            
            # Should trigger report 1 (9 AM daily) at 9:00 AM
            assert hasattr(result, 'run_requests')
            run_requests = result.run_requests
            assert len(run_requests) == 1
            
            request = run_requests[0]
            assert request.tags["report_id"] == "1"
            assert request.tags["report_name"] == "Daily 9 AM Report"

    @freeze_time("2024-01-01 18:00:00")  # Monday 6 PM
    def test_sensor_triggers_evening_report(self, mock_cron_asset_materialization):
        """Test that sensor triggers evening reports correctly"""
        with instance_for_test() as instance:
            instance.report_runless_asset_event(mock_cron_asset_materialization)
            context = build_sensor_context(instance=instance)
            
            with patch('orchestrator.sensors.report_sensor.CONFIG', {
                'CRON_SENSOR_INTERVAL_SECONDS': '60',
                'FIREANT_API_URL': 'https://test-api.com',
                'FIREANT_API_KEY': 'test-key'
            }):
                result = report_cron_sensor(context)
            
            # Should trigger report 2 (6 PM daily) at 6:00 PM
            assert hasattr(result, 'run_requests')
            run_requests = result.run_requests
            assert len(run_requests) == 1
            
            request = run_requests[0]
            assert request.tags["report_id"] == "2"
            assert request.tags["report_name"] == "Daily 6 PM Report"

    @freeze_time("2024-01-07 00:00:00")  # Sunday midnight
    def test_sensor_triggers_weekly_report(self, mock_cron_asset_materialization):
        """Test that sensor triggers weekly reports correctly"""
        with instance_for_test() as instance:
            instance.report_runless_asset_event(mock_cron_asset_materialization)
            context = build_sensor_context(instance=instance)
            
            with patch('orchestrator.sensors.report_sensor.CONFIG', {
                'CRON_SENSOR_INTERVAL_SECONDS': '60',
                'FIREANT_API_URL': 'https://test-api.com',
                'FIREANT_API_KEY': 'test-key'
            }):
                result = report_cron_sensor(context)
            
            # Should trigger report 3 (weekly Sunday) at Sunday midnight
            assert hasattr(result, 'run_requests')
            run_requests = result.run_requests
            assert len(run_requests) == 1
            
            request = run_requests[0]
            assert request.tags["report_id"] == "3"
            assert request.tags["report_name"] == "Weekly Sunday Report"

    @freeze_time("2024-01-01 10:30:00")  # Monday 10:30 AM
    def test_sensor_skips_when_no_reports_due(self, mock_cron_asset_materialization):
        """Test that sensor skips when no reports are due"""
        with instance_for_test() as instance:
            instance.report_runless_asset_event(mock_cron_asset_materialization)
            context = build_sensor_context(instance=instance)
            
            with patch('orchestrator.sensors.report_sensor.CONFIG', {
                'CRON_SENSOR_INTERVAL_SECONDS': '60',
                'FIREANT_API_URL': 'https://test-api.com',
                'FIREANT_API_KEY': 'test-key'
            }):
                result = report_cron_sensor(context)
            
            # Should not trigger any reports at 10:30 AM
            assert hasattr(result, 'run_requests')
            run_requests = result.run_requests
            assert len(run_requests) == 0

    @freeze_time("2024-01-01 09:15:00")  # Monday 9:15 AM
    def test_sensor_triggers_frequent_report(self, mock_cron_asset_materialization):
        """Test that sensor triggers frequent reports (every 15 minutes)"""
        with instance_for_test() as instance:
            instance.report_runless_asset_event(mock_cron_asset_materialization)
            context = build_sensor_context(instance=instance)
            
            with patch('orchestrator.sensors.report_sensor.CONFIG', {
                'CRON_SENSOR_INTERVAL_SECONDS': '60',
                'FIREANT_API_URL': 'https://test-api.com',
                'FIREANT_API_KEY': 'test-key'
            }):
                result = report_cron_sensor(context)
            
            # Should trigger report 4 (every 15 minutes) at 9:15 AM
            assert hasattr(result, 'run_requests')
            run_requests = result.run_requests
            assert len(run_requests) == 1
            
            request = run_requests[0]
            assert request.tags["report_id"] == "4"
            assert request.tags["report_name"] == "Every 15 Minutes Report"

    @freeze_time("2024-01-01 18:00:00")  # Monday 6 PM
    def test_schedule_triggers_at_correct_cron_time(self, mock_cron_asset_materialization):
        """Test that unified schedule triggers reports at the correct cron time"""
        with instance_for_test() as instance:
            instance.report_runless_asset_event(mock_cron_asset_materialization)
            context = build_schedule_context(instance=instance)
            
            with patch('orchestrator.schedules.dynamic_schedules.CONFIG', {
                'FIREANT_API_URL': 'https://test-api.com',
                'FIREANT_API_KEY': 'test-key'
            }):
                result = unified_report_schedule(context)
            
            # Should trigger report 2 (6 PM daily) at 6:00 PM
            assert isinstance(result, list)
            assert len(result) == 1
            
            request = result[0]
            assert request.tags["report_id"] == "2"
            assert request.tags["report_name"] == "Daily 6 PM Report"


class TestCronEdgeCases:
    """Test edge cases and complex cron scenarios"""

    def test_leap_year_february_29th(self):
        """Test cron expressions work correctly with leap years"""
        # Test a cron that should run on Feb 29th in a leap year
        cron_expr = "0 0 29 2 *"  # Feb 29th at midnight
        
        # 2024 is a leap year
        with freeze_time("2024-02-28 12:00:00"):
            cron = croniter(cron_expr)
            next_run = cron.get_next(datetime)
            assert next_run.month == 2
            assert next_run.day == 29
            assert next_run.year == 2024

    def test_timezone_handling(self):
        """Test that cron expressions work correctly with different timezones"""
        # Note: croniter works with the system timezone by default
        # This test ensures our cron handling is consistent
        cron_expr = "0 12 * * *"  # Daily at noon
        
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        cron = croniter(cron_expr, base_time)
        next_run = cron.get_next(datetime)
        
        assert next_run.hour == 12
        assert next_run.minute == 0

    def test_daylight_saving_time_transitions(self):
        """Test cron behavior during DST transitions"""
        # This is a complex edge case that depends on system timezone
        # We'll test the basic functionality
        cron_expr = "0 2 * * *"  # 2 AM daily
        
        # Test around a typical DST transition date
        base_time = datetime(2024, 3, 10, 1, 0, 0)  # 1 AM on DST transition day
        cron = croniter(cron_expr, base_time)
        next_run = cron.get_next(datetime)
        
        # Should still schedule for 2 AM (or equivalent)
        assert next_run.hour == 2
        assert next_run.minute == 0

    def test_month_boundary_transitions(self):
        """Test cron expressions across month boundaries"""
        test_cases = [
            {
                "cron": "0 0 1 * *",  # First day of month
                "current": "2024-01-31 12:00:00",
                "expected_month": 2,
                "expected_day": 1
            },
            {
                "cron": "0 0 31 * *",  # 31st of month (not all months have 31 days)
                "current": "2024-02-15 12:00:00",
                "expected_month": 3,  # Should skip to March (31 days)
                "expected_day": 31
            },
        ]
        
        for case in test_cases:
            current_time = datetime.strptime(case["current"], "%Y-%m-%d %H:%M:%S")
            cron = croniter(case["cron"], current_time)
            next_run = cron.get_next(datetime)
            
            assert next_run.month == case["expected_month"]
            assert next_run.day == case["expected_day"]
            print(f"✅ Month boundary test: {case['cron']}") 