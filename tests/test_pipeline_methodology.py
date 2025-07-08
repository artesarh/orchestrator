"""
Tests for the pipeline methodology - individual report jobs approach
"""

import pytest
import os
import json
from unittest.mock import patch, MagicMock
from dagster import build_op_context, instance_for_test
from orchestrator.utils.config import SchedulingApproach, get_scheduling_approach
from orchestrator.utils.dynamic_job_manager import DynamicJobManager, detect_and_handle_report_changes
from orchestrator.jobs.individual_report_jobs import (
    create_report_job, 
    generate_individual_report_jobs,
    get_reports_from_cache,
    update_reports_cache
)


class TestPipelineMethodology:
    """Test the pipeline approach for individual report jobs"""

    @pytest.fixture
    def sample_reports(self):
        """Sample report data for testing"""
        return [
            {
                "id": 1,
                "name": "Daily Sales Report",
                "cron": "0 9 * * *",
                "event_group": {"id": 1, "name": "Sales"}
            },
            {
                "id": 2,
                "name": "Weekly Inventory Report", 
                "cron": "0 10 * * 1",
                "event_group": {"id": 2, "name": "Inventory"}
            },
            {
                "id": 3,
                "name": "Monthly Financial Report",
                "cron": "0 8 1 * *",
                "event_group": {"id": 3, "name": "Finance"}
            }
        ]

    @pytest.fixture
    def mock_cache_file(self, tmp_path):
        """Create a temporary cache file for testing"""
        cache_file = tmp_path / "test_cache.json"
        return str(cache_file)

    def test_pipeline_approach_configuration(self):
        """Test that pipeline approach can be configured"""
        with patch.dict(os.environ, {"SCHEDULING_APPROACH": "pipeline"}):
            approach = get_scheduling_approach()
            assert approach == SchedulingApproach.PIPELINE

    def test_individual_job_creation(self, sample_reports):
        """Test creating individual jobs for reports"""
        report = sample_reports[0]
        job_func = create_report_job(report["id"], report["name"])
        
        # Test job function exists and has correct name
        assert job_func is not None
        assert hasattr(job_func, '__name__')
        assert job_func.__name__ == 'individual_report_job'

    def test_cache_management(self, sample_reports, mock_cache_file):
        """Test cache file management"""
        # Test cache update
        cache_data = {
            "reports": sample_reports,
            "cron_schedules": {
                "1": {"cron": "0 9 * * *", "name": "Daily Sales Report"},
                "2": {"cron": "0 10 * * 1", "name": "Weekly Inventory Report"},
                "3": {"cron": "0 8 1 * *", "name": "Monthly Financial Report"}
            },
            "last_updated": "2024-01-01T12:00:00"
        }
        
        # Write cache
        with open(mock_cache_file, 'w') as f:
            json.dump(cache_data, f)
        
        # Test reading cache
        with patch('orchestrator.jobs.individual_report_jobs.os.getcwd', return_value=os.path.dirname(mock_cache_file)):
            with patch('orchestrator.jobs.individual_report_jobs.os.path.join', return_value=mock_cache_file):
                reports = get_reports_from_cache()
                assert len(reports) == 3
                assert reports[0]["name"] == "Daily Sales Report"

    def test_dynamic_job_generation(self, sample_reports, mock_cache_file):
        """Test dynamic job generation from cache"""
        # Create cache with sample reports
        cache_data = {
            "reports": sample_reports,
            "cron_schedules": {
                "1": {"cron": "0 9 * * *", "name": "Daily Sales Report"},
                "2": {"cron": "0 10 * * 1", "name": "Weekly Inventory Report"},
                "3": {"cron": "0 8 1 * *", "name": "Monthly Financial Report"}
            },
            "last_updated": "2024-01-01T12:00:00"
        }
        
        with open(mock_cache_file, 'w') as f:
            json.dump(cache_data, f)
        
        # Test job generation
        with patch('orchestrator.jobs.individual_report_jobs.os.getcwd', return_value=os.path.dirname(mock_cache_file)):
            with patch('orchestrator.jobs.individual_report_jobs.os.path.join', return_value=mock_cache_file):
                jobs = generate_individual_report_jobs()
                
                # Should generate 3 jobs (one for each report with cron)
                assert len(jobs) == 3
                assert "process_report_1" in jobs
                assert "process_report_2" in jobs
                assert "process_report_3" in jobs

    def test_job_generation_filters_reports_without_cron(self, mock_cache_file):
        """Test that job generation only includes reports with cron schedules"""
        reports_mixed = [
            {"id": 1, "name": "With Cron", "cron": "0 9 * * *"},
            {"id": 2, "name": "Without Cron", "cron": None},
            {"id": 3, "name": "Empty Cron", "cron": ""},
            {"id": 4, "name": "With Cron 2", "cron": "0 10 * * *"}
        ]
        
        cache_data = {
            "reports": reports_mixed,
            "cron_schedules": {
                "1": {"cron": "0 9 * * *", "name": "With Cron"},
                "4": {"cron": "0 10 * * *", "name": "With Cron 2"}
            },
            "last_updated": "2024-01-01T12:00:00"
        }
        
        with open(mock_cache_file, 'w') as f:
            json.dump(cache_data, f)
        
        with patch('orchestrator.jobs.individual_report_jobs.os.getcwd', return_value=os.path.dirname(mock_cache_file)):
            with patch('orchestrator.jobs.individual_report_jobs.os.path.join', return_value=mock_cache_file):
                jobs = generate_individual_report_jobs()
                
                # Should only generate 2 jobs (only reports with cron)
                assert len(jobs) == 2
                assert "process_report_1" in jobs
                assert "process_report_4" in jobs
                assert "process_report_2" not in jobs
                assert "process_report_3" not in jobs


class TestDynamicJobManager:
    """Test the dynamic job manager for change detection"""

    @pytest.fixture
    def job_manager(self, tmp_path):
        """Create a job manager with temporary cache file"""
        cache_file = tmp_path / "test_manager_cache.json"
        return DynamicJobManager(str(cache_file))

    @pytest.fixture
    def initial_reports(self):
        """Initial set of reports"""
        return [
            {"id": 1, "name": "Report 1", "cron": "0 9 * * *"},
            {"id": 2, "name": "Report 2", "cron": "0 10 * * *"}
        ]

    def test_detect_new_reports(self, job_manager, initial_reports):
        """Test detection of new reports"""
        # Start with empty cache
        new_reports = initial_reports + [
            {"id": 3, "name": "New Report", "cron": "0 11 * * *"}
        ]
        
        changes = job_manager.detect_changes(new_reports)
        
        assert len(changes["new_reports"]) == 3  # All are new since cache is empty
        assert changes["requires_restart"] is True

    def test_detect_cron_changes(self, job_manager, initial_reports):
        """Test detection of cron schedule changes"""
        # Setup initial cache
        job_manager.update_cache_with_changes(initial_reports, {"new_reports": [], "removed_reports": [], "cron_changes": [], "name_changes": [], "requires_restart": False})
        
        # Change cron for report 1
        modified_reports = [
            {"id": 1, "name": "Report 1", "cron": "0 8 * * *"},  # Changed from 9 to 8
            {"id": 2, "name": "Report 2", "cron": "0 10 * * *"}
        ]
        
        changes = job_manager.detect_changes(modified_reports)
        
        assert len(changes["cron_changes"]) == 1
        assert changes["cron_changes"][0]["report_id"] == 1
        assert changes["cron_changes"][0]["old_cron"] == "0 9 * * *"
        assert changes["cron_changes"][0]["new_cron"] == "0 8 * * *"
        assert changes["requires_restart"] is False  # Cron changes don't require restart

    def test_detect_name_changes(self, job_manager, initial_reports):
        """Test detection of report name changes"""
        # Setup initial cache
        job_manager.update_cache_with_changes(initial_reports, {"new_reports": [], "removed_reports": [], "cron_changes": [], "name_changes": [], "requires_restart": False})
        
        # Change name for report 1
        modified_reports = [
            {"id": 1, "name": "Updated Report 1", "cron": "0 9 * * *"},  # Changed name
            {"id": 2, "name": "Report 2", "cron": "0 10 * * *"}
        ]
        
        changes = job_manager.detect_changes(modified_reports)
        
        assert len(changes["name_changes"]) == 1
        assert changes["name_changes"][0]["report_id"] == 1
        assert changes["name_changes"][0]["old_name"] == "Report 1"
        assert changes["name_changes"][0]["new_name"] == "Updated Report 1"
        assert changes["requires_restart"] is False  # Name changes don't require restart

    def test_detect_removed_reports(self, job_manager, initial_reports):
        """Test detection of removed reports"""
        # Setup initial cache
        job_manager.update_cache_with_changes(initial_reports, {"new_reports": [], "removed_reports": [], "cron_changes": [], "name_changes": [], "requires_restart": False})
        
        # Remove report 2
        modified_reports = [
            {"id": 1, "name": "Report 1", "cron": "0 9 * * *"}
        ]
        
        changes = job_manager.detect_changes(modified_reports)
        
        assert len(changes["removed_reports"]) == 1
        assert changes["removed_reports"][0]["id"] == 2
        assert changes["requires_restart"] is True  # Removed reports require restart

    def test_restart_recommendations(self, job_manager, initial_reports):
        """Test restart recommendation logic"""
        # No changes - no restart needed
        changes = {"new_reports": [], "removed_reports": [], "cron_changes": [], "name_changes": [], "requires_restart": False}
        recommendation = job_manager.get_restart_recommendation(changes)
        assert "✅ NO CHANGES" in recommendation
        
        # Cron changes only - no restart needed
        changes = {"new_reports": [], "removed_reports": [], "cron_changes": [{"report_id": 1}], "name_changes": [], "requires_restart": False}
        recommendation = job_manager.get_restart_recommendation(changes)
        assert "✅ NO RESTART NEEDED" in recommendation
        
        # New reports - restart needed
        changes = {"new_reports": [{"id": 3}], "removed_reports": [], "cron_changes": [], "name_changes": [], "requires_restart": True}
        recommendation = job_manager.get_restart_recommendation(changes)
        assert "🔄 SERVER RESTART RECOMMENDED" in recommendation

    def test_convenience_function(self, initial_reports):
        """Test the convenience function for change detection"""
        with patch('orchestrator.utils.dynamic_job_manager.job_manager') as mock_manager:
            mock_manager.detect_changes.return_value = {"new_reports": [], "removed_reports": [], "cron_changes": [], "name_changes": [], "requires_restart": False}
            mock_manager.get_restart_recommendation.return_value = "✅ NO CHANGES"
            
            changes = detect_and_handle_report_changes(initial_reports)
            
            mock_manager.detect_changes.assert_called_once_with(initial_reports)
            mock_manager.update_cache_with_changes.assert_called_once()
            assert changes is not None


class TestPipelineIntegration:
    """Test pipeline methodology integration with Dagster"""

    def test_pipeline_job_configuration(self):
        """Test that pipeline jobs are configured correctly"""
        with patch('orchestrator.jobs.individual_report_jobs.CONFIG') as mock_config:
            mock_config.__getitem__.return_value = "test_value"
            
            # Test job creation
            job_func = create_report_job(1, "Test Report")
            assert job_func is not None
            
            # Test that job has correct attributes
            assert hasattr(job_func, '__name__')
            assert job_func.__name__ == 'individual_report_job'

    def test_pipeline_approach_in_definitions(self):
        """Test that pipeline approach is handled correctly in definitions"""
        with patch('orchestrator.definitions.get_scheduling_approach') as mock_get_approach:
            with patch('orchestrator.definitions.INDIVIDUAL_REPORT_JOBS', {"process_report_1": MagicMock()}):
                mock_get_approach.return_value = SchedulingApproach.PIPELINE
                
                # Import definitions to trigger the logic
                from orchestrator.definitions import all_jobs
                
                # Should include both unified and individual jobs for pipeline approach
                assert len(all_jobs) >= 2  # At least unified + 1 individual job

    def test_pipeline_vs_other_approaches(self):
        """Test differences between pipeline and other approaches"""
        with patch('orchestrator.definitions.INDIVIDUAL_REPORT_JOBS', {"process_report_1": MagicMock()}):
            
            # Test sensor approach
            with patch('orchestrator.definitions.get_scheduling_approach', return_value=SchedulingApproach.SENSOR):
                from orchestrator.definitions import all_jobs as sensor_jobs
                # Should only have unified job
                assert len(sensor_jobs) == 1
            
            # Test schedule approach  
            with patch('orchestrator.definitions.get_scheduling_approach', return_value=SchedulingApproach.SCHEDULE):
                from orchestrator.definitions import all_jobs as schedule_jobs
                # Should only have unified job
                assert len(schedule_jobs) == 1
            
            # Test pipeline approach
            with patch('orchestrator.definitions.get_scheduling_approach', return_value=SchedulingApproach.PIPELINE):
                from orchestrator.definitions import all_jobs as pipeline_jobs
                # Should have unified + individual jobs
                assert len(pipeline_jobs) >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 