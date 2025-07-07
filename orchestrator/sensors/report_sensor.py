from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
    SensorResult,
)
from croniter import croniter
from datetime import datetime, timedelta
from typing import List
from ..jobs.report_pipeline import process_report_job
from ..assets.report_discovery import report_cron_schedules
from ..utils.config import CRON_SENSOR_INTERVAL, DISCOVERY_SENSOR_INTERVAL, EXTERNAL_API_URL, EXTERNAL_API_KEY


@sensor(
    job=process_report_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=CRON_SENSOR_INTERVAL,
)
def report_cron_sensor(context: SensorEvaluationContext):
    """
    Asset-based sensor that triggers jobs based on cron schedules.
    Reacts immediately when schedules change and checks timing at configured interval.
    """
    logger = context.log
    
    try:
        # Get the latest report schedules from the asset
        schedules_asset = context.instance.get_latest_materialization_record(
            "report_cron_schedules"
        )

        if not schedules_asset:
            return SkipReason("No report schedules discovered yet")

        # Extract schedule data from the asset materialization metadata
        cron_schedules = {}
        
        # Look for cron_schedules in metadata
        for entry in schedules_asset.asset_materialization.metadata_entries:
            if entry.label == "cron_schedules":
                try:
                    # MetadataValue.json stores the actual dict
                    if hasattr(entry.value, 'value'):
                        cron_schedules = entry.value.value
                    else:
                        cron_schedules = entry.value
                    break
                except Exception as e:
                    logger.warning(f"Could not parse cron_schedules metadata: {e}")
        
        if not cron_schedules:
            return SkipReason("No cron schedules found in asset metadata")

        current_time = datetime.now()
        run_requests = []
        
        # Get sensor interval for timing window
        timing_window = timedelta(seconds=CRON_SENSOR_INTERVAL)

        for report_id, schedule_info in cron_schedules.items():
            cron_expr = schedule_info["cron"]

            # Check if this report should run within the current timing window
            cron = croniter(cron_expr, current_time - timing_window)
            next_run = cron.get_next(datetime)

            if next_run <= current_time:
                # Create partition key with precise timing
                partition_key = f"report_{report_id}_{int(next_run.timestamp())}"

                # Add partition if it doesn't exist
                context.instance.add_dynamic_partitions("reports", [partition_key])
                
                logger.info(f"Scheduling report {report_id} ('{schedule_info.get('name', 'Unnamed')}') "
                           f"for partition {partition_key}")

                run_requests.append(
                    RunRequest(
                        partition_key=partition_key,
                        run_config={
                            "ops": {
                                "report_data": {
                                    "config": {
                                        "report_id": int(report_id),
                                        "modifier_id": 1,  # You'll need logic to determine this
                                        "external_api_url": EXTERNAL_API_URL,
                                        "external_api_key": EXTERNAL_API_KEY,
                                    }
                                }
                            }
                        },
                        tags={
                            "report_id": report_id,
                            "report_name": schedule_info.get(
                                "name", f"Report {report_id}"
                            ),
                            "cron_schedule": cron_expr,
                        },
                    )
                )

        if run_requests:
            logger.info(f"Triggering {len(run_requests)} report runs")
        
        return SensorResult(run_requests=run_requests)

    except Exception as e:
        logger.error(f"Error in cron sensor: {str(e)}")
        return SkipReason(f"Error in sensor: {str(e)}")


@sensor(
    asset_selection=[report_cron_schedules],
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=DISCOVERY_SENSOR_INTERVAL,
)
def daily_report_discovery_sensor(context: SensorEvaluationContext):
    """Trigger report discovery at configurable interval"""
    context.log.info("Triggering report discovery")
    return RunRequest()
