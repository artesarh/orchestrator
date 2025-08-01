from dagster import asset, Config, get_dagster_logger, MetadataValue
from ..resources.api_client import DjangoAPIClient
from ..utils.dynamic_job_manager import detect_and_handle_report_changes
from typing import Dict, List, Any
from croniter import croniter
from datetime import datetime
import json
import os


class ReportDiscoveryConfig(Config):
    check_new_reports: bool = True


@asset(group_name="discovery", compute_kind="api_call")
def report_cron_schedules(
    context, config: ReportDiscoveryConfig, api_client: DjangoAPIClient
) -> Dict[str, Any]:
    """
    Discover all reports and their cron schedules.
    This runs daily to check for new reports or cron changes.
    """
    logger = get_dagster_logger()

    reports_response = api_client.get_all_reports()
    reports = reports_response.get("data", [])

    # Detect changes and handle them intelligently
    changes = detect_and_handle_report_changes(reports)

    cron_map = {}
    valid_reports = []

    for report in reports:
        report_id = report.get("id")
        if not report_id:
            logger.warning(f"Report missing ID field: {report}")
            continue

        cron_schedule = report.get("cron")
        if cron_schedule:
            try:
                # Validate cron expression
                croniter(cron_schedule)
                cron_map[report_id] = {
                    "cron": cron_schedule,
                    "name": report.get("name", f"Report {report_id}"),
                }
                valid_reports.append(report_id)
                logger.info(
                    f"Report {report_id} has valid cron: {cron_schedule}"
                )
            except Exception as e:
                logger.warning(
                    f"Report {report_id} has invalid cron '{cron_schedule}': {e}"
                )

    result_data = {
        "cron_schedules": cron_map,
        "valid_report_ids": valid_reports,
        "discovered_at": datetime.now().isoformat(),
        "changes_detected": changes,
    }

    # Add metadata for the sensor to easily access
    context.add_output_metadata(
        {
            "cron_schedules": MetadataValue.json(cron_map),
            "total_reports": MetadataValue.int(len(valid_reports)),
            "discovery_time": MetadataValue.text(result_data["discovered_at"]),
            "changes_summary": MetadataValue.json(changes),
            "requires_restart": MetadataValue.bool(changes.get("requires_restart", False)),
        }
    )

    return result_data
