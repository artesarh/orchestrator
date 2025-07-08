#!/usr/bin/env python3
"""Test event group validation and modifier management"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pytest
from unittest.mock import Mock, patch
from datetime import date

from orchestrator.resources.api_client import DjangoAPIClient
from orchestrator.assets.report_processing import report_data, ReportProcessingConfig
from orchestrator.utils.config import DJANGO_API_URL, DJANGO_JWT_TOKEN
from dagster import OpExecutionContext, build_op_context


class TestEventGroupValidation:
    """Test event group validation logic"""
    
    def setup_method(self):
        """Setup test fixtures - use environment config for tests"""
        self.client = DjangoAPIClient(
            base_url=DJANGO_API_URL,
            api_token=DJANGO_JWT_TOKEN
        )
        self.config = ReportProcessingConfig(
            report_id=2,
            run_date="2025-07-07",
            external_api_url="https://api.example.com",
            external_api_key="test-key"
        )
    
    def test_find_modifier_with_direct_endpoint(self):
        """Test that find_modifier_for_date uses the direct endpoint correctly"""
        # Mock the response from the direct endpoint
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "modifiers": [
                    {
                        "id": 2,
                        "as_at_date": "2025-07-07",
                        "fx_date": "2025-07-07"
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        
        with patch('requests.get', return_value=mock_response) as mock_get:
            result = self.client.find_modifier_for_date(2, date(2025, 7, 7))
            
            # Verify it called the correct endpoint
            mock_get.assert_called_once()
            args, kwargs = mock_get.call_args
            assert "/api/reports/2/modifiers/" in args[0]
            assert kwargs['params']['fx_date'] == "2025-07-07"
            assert kwargs['params']['as_at_date'] == "2025-07-07"
            
            # Verify it returns the correct modifier
            assert result['id'] == 2
            assert result['fx_date'] == "2025-07-07"
    
    def test_find_modifier_no_results(self):
        """Test when no modifiers are found for the date"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "modifiers": []
            }
        }
        mock_response.raise_for_status.return_value = None
        
        with patch('requests.get', return_value=mock_response):
            result = self.client.find_modifier_for_date(2, date(2025, 7, 7))
            assert result is None
    
    def test_report_validation_with_event_group(self):
        """Test that report processing succeeds when event_group exists"""
        # Mock sequence of requests.get calls for the asset workflow
        mock_responses = [
            # First call: get_all_reports
            Mock(
                json=Mock(return_value={
                    "data": [
                        {
                            "url": f"{DJANGO_API_URL}/api/reports/1/",
                            "name": "Test Report",
                            "event_group": 1  # Has event group
                        }
                    ]
                }),
                raise_for_status=Mock()
            ),
            # Second call: find_modifier_for_date (returns existing modifier)
            Mock(
                json=Mock(return_value={
                    "data": {
                        "modifiers": [
                            {
                                "id": 1,
                                "fx_date": "2025-07-07",
                                "as_at_date": "2025-07-07"
                            }
                        ]
                    }
                }),
                raise_for_status=Mock()
            ),
            # Third call: get_report_with_modifier
            Mock(
                json=Mock(return_value={
                    "data": {
                        "report": {"id": 1, "name": "Test Report"},
                        "modifier": {"id": 1, "fx_date": "2025-07-07", "as_at_date": "2025-07-07"},
                        "eventgroup": {"events": []}
                    }
                }),
                raise_for_status=Mock()
            )
        ]
        
        context = build_op_context()
        
        with patch('requests.get', side_effect=mock_responses):
            config = ReportProcessingConfig(
                report_id=1,
                run_date="2025-07-07",
                external_api_url="https://api.example.com",
                external_api_key="test-key"
            )
            
            # Should not raise an exception
            result = report_data(context, config, self.client)
            assert result is not None
    
    def test_report_validation_without_event_group(self):
        """Test that report processing fails when event_group is null"""
        # Mock reports response without event_group
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                {
                    "url": f"{DJANGO_API_URL}/api/reports/2/",
                    "name": "Test Report",
                    "event_group": None  # No event group
                }
            ]
        }
        mock_response.raise_for_status.return_value = None
        
        context = build_op_context()
        
        with patch('requests.get', return_value=mock_response):
            config = ReportProcessingConfig(
                report_id=2,
                run_date="2025-07-07",
                external_api_url="https://api.example.com",
                external_api_key="test-key"
            )
            
            # Should raise ValueError about missing event group
            with pytest.raises(ValueError) as exc_info:
                report_data(context, config, self.client)
            
            assert "no Event Group associated" in str(exc_info.value)
            assert "Test Report" in str(exc_info.value)
    
    def test_report_not_found(self):
        """Test that processing fails when report doesn't exist"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                {
                    "url": f"{DJANGO_API_URL}/api/reports/1/",
                    "name": "Other Report",
                    "event_group": 1
                }
            ]
        }
        mock_response.raise_for_status.return_value = None
        
        context = build_op_context()
        
        with patch('requests.get', return_value=mock_response):
            config = ReportProcessingConfig(
                report_id=999,  # Non-existent report
                run_date="2025-07-07",
                external_api_url="https://api.example.com",
                external_api_key="test-key"
            )
            
            with pytest.raises(ValueError) as exc_info:
                report_data(context, config, self.client)
            
            assert "Report 999 not found" in str(exc_info.value)


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v"]) 