#!/usr/bin/env python3
"""Integration tests for modifier management and event group validation"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pytest
from datetime import date
from orchestrator.resources.api_client import DjangoAPIClient
from orchestrator.utils.config import DJANGO_API_URL, DJANGO_JWT_TOKEN


class TestModifierManagementIntegration:
    """Integration tests that hit the real Django API"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures"""
        self.client = DjangoAPIClient(
            base_url=DJANGO_API_URL,
            api_token=DJANGO_JWT_TOKEN
        )
        self.test_date = date(2025, 7, 7)
    
    def test_reports_discovery(self):
        """Test that we can discover reports and identify their event group status"""
        reports_response = self.client.get_all_reports()
        
        # Verify response structure
        assert "data" in reports_response
        reports = reports_response["data"]
        assert len(reports) > 0
        
        # Check each report's event group status
        for report in reports:
            report_url = report.get("url", "")
            assert "/reports/" in report_url
            
            # Extract report ID
            report_id = int(report_url.strip("/").split("/")[-1])
            name = report.get("name", "Unknown")
            event_group = report.get("event_group")
            
            print(f"Report {report_id} ('{name}'): event_group={event_group}")
            
            # Verify structure
            assert isinstance(report_id, int)
            assert isinstance(name, str)
            # event_group can be int or None
    
    def test_find_modifier_direct_endpoint(self):
        """Test the new find_modifier_for_date implementation"""
        # Test with report 2 (which should have existing modifiers)
        result = self.client.find_modifier_for_date(2, self.test_date)
        
        if result:
            print(f"Found modifier: {result}")
            assert "id" in result
            assert result["fx_date"] == self.test_date.isoformat()
            assert result["as_at_date"] == self.test_date.isoformat()
        else:
            print("No existing modifier found - this is also valid")
    
    def test_complete_modifier_workflow(self):
        """Test the complete get_or_create_modifier_for_date workflow"""
        # Test with report 2
        report_id = 2
        
        # Get or create modifier
        modifier = self.client.get_or_create_modifier_for_date(report_id, self.test_date)
        
        assert "id" in modifier
        assert modifier["fx_date"] == self.test_date.isoformat()
        assert modifier["as_at_date"] == self.test_date.isoformat()
        
        modifier_id = modifier["id"]
        print(f"Got modifier {modifier_id} for report {report_id}")
        
        # Try again - should find the same modifier, not create a new one
        modifier2 = self.client.get_or_create_modifier_for_date(report_id, self.test_date)
        
        assert modifier2["id"] == modifier_id, "Should reuse existing modifier, not create new one"
        print(f"✅ Correctly reused existing modifier {modifier_id}")
    
    def test_report_with_event_group_can_get_complete_data(self):
        """Test that report 1 (with event group) can get complete data"""
        report_id = 1
        
        # Get or create modifier for report 1
        modifier = self.client.get_or_create_modifier_for_date(report_id, date(2025, 7, 1))
        modifier_id = modifier["id"]
        
        try:
            # Try to get complete data
            complete_data = self.client.get_report_with_modifier(report_id, modifier_id)
            
            # Verify structure
            assert "data" in complete_data
            data = complete_data["data"]
            
            assert "report" in data
            assert "modifier" in data
            assert "eventgroup" in data
            
            print(f"✅ Report {report_id} with modifier {modifier_id} returned complete data")
            
        except Exception as e:
            pytest.fail(f"Report 1 should work with complete data: {e}")
    
    def test_report_without_event_group_fails_complete_data(self):
        """Test that report 2 (without event group) fails on complete data endpoint"""
        report_id = 2
        
        # Get or create modifier for report 2
        modifier = self.client.get_or_create_modifier_for_date(report_id, self.test_date)
        modifier_id = modifier["id"]
        
        # This should fail because report 2 has no event group
        with pytest.raises(Exception) as exc_info:
            self.client.get_report_with_modifier(report_id, modifier_id)
        
        error_msg = str(exc_info.value)
        print(f"✅ Report {report_id} correctly failed: {error_msg}")
        
        # Should mention event group issue
        assert "404" in error_msg or "Event Group" in error_msg
    
    def test_event_group_status_check(self):
        """Test identifying which reports have event groups"""
        reports_response = self.client.get_all_reports()
        reports = reports_response["data"]
        
        reports_with_event_groups = []
        reports_without_event_groups = []
        
        for report in reports:
            report_url = report.get("url", "")
            if "/reports/" in report_url:
                report_id = int(report_url.strip("/").split("/")[-1])
                name = report.get("name", "Unknown")
                event_group = report.get("event_group")
                
                if event_group is not None:
                    reports_with_event_groups.append((report_id, name))
                else:
                    reports_without_event_groups.append((report_id, name))
        
        print(f"Reports WITH event groups: {reports_with_event_groups}")
        print(f"Reports WITHOUT event groups: {reports_without_event_groups}")
        
        # Should have at least one of each for testing
        assert len(reports_with_event_groups) > 0, "Need at least one report with event group for testing"
        assert len(reports_without_event_groups) > 0, "Need at least one report without event group for testing"


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v", "-s"])  # -s to show print statements 