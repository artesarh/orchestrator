"""
Dynamic Job Manager for handling cron schedule changes without server restarts.
This provides utilities to detect changes and update job configurations.
"""

import json
import os
from typing import Dict, Any, List, Set, Tuple
from datetime import datetime
from dagster import get_dagster_logger
from croniter import croniter


class DynamicJobManager:
    """Manages dynamic job creation and cron schedule updates"""
    
    def __init__(self, cache_file: str = ".dagster_reports_cache.json"):
        self.cache_file = cache_file
        self.logger = get_dagster_logger()
    
    def load_cache(self) -> Dict[str, Any]:
        """Load current cache data"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading cache: {e}")
        return {"reports": [], "cron_schedules": {}, "last_updated": None}
    
    def save_cache(self, data: Dict[str, Any]):
        """Save cache data"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving cache: {e}")
    
    def detect_changes(self, new_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Detect changes in reports and cron schedules.
        Returns a summary of changes that need to be handled.
        """
        current_cache = self.load_cache()
        current_reports = {r["id"]: r for r in current_cache.get("reports", [])}
        new_reports_dict = {r["id"]: r for r in new_reports}
        
        changes = {
            "new_reports": [],
            "removed_reports": [],
            "cron_changes": [],
            "name_changes": [],
            "requires_restart": False
        }
        
        # Check for new reports
        for report_id, report in new_reports_dict.items():
            if report_id not in current_reports:
                if report.get("cron"):  # Only care about reports with cron schedules
                    changes["new_reports"].append(report)
                    changes["requires_restart"] = True
        
        # Check for removed reports
        for report_id, report in current_reports.items():
            if report_id not in new_reports_dict:
                if report.get("cron"):
                    changes["removed_reports"].append(report)
                    changes["requires_restart"] = True
        
        # Check for cron changes in existing reports
        for report_id, new_report in new_reports_dict.items():
            if report_id in current_reports:
                current_report = current_reports[report_id]
                
                # Check cron changes
                current_cron = current_report.get("cron")
                new_cron = new_report.get("cron")
                
                if current_cron != new_cron:
                    changes["cron_changes"].append({
                        "report_id": report_id,
                        "old_cron": current_cron,
                        "new_cron": new_cron,
                        "report_name": new_report.get("name", f"Report {report_id}")
                    })
                
                # Check name changes
                current_name = current_report.get("name")
                new_name = new_report.get("name")
                
                if current_name != new_name:
                    changes["name_changes"].append({
                        "report_id": report_id,
                        "old_name": current_name,
                        "new_name": new_name
                    })
        
        return changes
    
    def update_cache_with_changes(self, new_reports: List[Dict[str, Any]], changes: Dict[str, Any]):
        """Update cache with new report data and log changes"""
        
        # Log all changes
        if changes["new_reports"]:
            self.logger.info(f"New reports detected: {[r['id'] for r in changes['new_reports']]}")
        
        if changes["removed_reports"]:
            self.logger.info(f"Removed reports detected: {[r['id'] for r in changes['removed_reports']]}")
        
        if changes["cron_changes"]:
            for change in changes["cron_changes"]:
                self.logger.info(
                    f"Cron change detected for report {change['report_id']} "
                    f"({change['report_name']}): '{change['old_cron']}' -> '{change['new_cron']}'"
                )
        
        if changes["name_changes"]:
            for change in changes["name_changes"]:
                self.logger.info(
                    f"Name change detected for report {change['report_id']}: "
                    f"'{change['old_name']}' -> '{change['new_name']}'"
                )
        
        # Build new cron_schedules dict
        cron_schedules = {}
        for report in new_reports:
            report_id = report.get("id")
            cron_schedule = report.get("cron")
            if report_id and cron_schedule:
                try:
                    # Validate cron expression
                    croniter(cron_schedule)
                    cron_schedules[report_id] = {
                        "cron": cron_schedule,
                        "name": report.get("name", f"Report {report_id}"),
                    }
                except Exception as e:
                    self.logger.warning(f"Invalid cron for report {report_id}: {e}")
        
        # Update cache
        cache_data = {
            "reports": new_reports,
            "cron_schedules": cron_schedules,
            "last_updated": datetime.now().isoformat(),
            "last_changes": changes
        }
        
        self.save_cache(cache_data)
        
        return changes
    
    def get_restart_recommendation(self, changes: Dict[str, Any]) -> str:
        """Get recommendation message about whether restart is needed"""
        if changes["requires_restart"]:
            restart_reasons = []
            if changes["new_reports"]:
                restart_reasons.append(f"{len(changes['new_reports'])} new reports")
            if changes["removed_reports"]:
                restart_reasons.append(f"{len(changes['removed_reports'])} removed reports")
            
            return (
                f"🔄 SERVER RESTART RECOMMENDED: {', '.join(restart_reasons)} detected. "
                f"Restart Dagster to see new individual job pipelines in the GUI."
            )
        elif changes["cron_changes"] or changes["name_changes"]:
            return (
                "✅ NO RESTART NEEDED: Only cron/name changes detected. "
                "These will be picked up automatically on the next scheduling cycle."
            )
        else:
            return "✅ NO CHANGES: Report configuration is up to date."


# Global instance
job_manager = DynamicJobManager()


def detect_and_handle_report_changes(new_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Convenience function to detect and handle report changes.
    Returns change summary and logs appropriate messages.
    """
    changes = job_manager.detect_changes(new_reports)
    job_manager.update_cache_with_changes(new_reports, changes)
    
    # Log restart recommendation
    recommendation = job_manager.get_restart_recommendation(changes)
    logger = get_dagster_logger()
    
    if changes["requires_restart"]:
        logger.warning(recommendation)
    else:
        logger.info(recommendation)
    
    return changes 