from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
    SensorResult,
    AssetKey,
    EventRecordsFilter,
    DagsterEventType,
)
from croniter import croniter
from datetime import datetime, timedelta
from ..jobs.report_pipeline import process_report_job
from ..assets.report_discovery import report_cron_schedules
from ..utils.config import CONFIG


@sensor(
    asset_selection=[report_cron_schedules],
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(CONFIG["CRON_SENSOR_INTERVAL_SECONDS"]),
)
def report_cron_sensor(context: SensorEvaluationContext):
    """
    Asset-based sensor that triggers jobs based on cron schedules.
    Reads from report_cron_schedules asset that gets updated daily.
    """
    logger = context.log
    
    try:
        # Get the latest report schedules from the materialized asset
        asset_key = AssetKey(["report_cron_schedules"])
        
        events = context.instance.get_event_records(
            EventRecordsFilter(
                asset_key=asset_key,
                event_type=DagsterEventType.ASSET_MATERIALIZATION
            ),
            limit=1
        )
        
        if not events:
            return SkipReason("No report schedules discovered yet - waiting for report_cron_schedules asset")

        # Get the latest materialization event
        latest_event = events[0]
        asset_materialization = latest_event.event_log_entry.dagster_event.event_specific_data.materialization
        
        # Extract cron_schedules from metadata
        cron_schedules = {}
        for entry in asset_materialization.metadata_entries:
            if entry.label == "cron_schedules":
                if hasattr(entry.value, 'value'):
                    cron_schedules = entry.value.value
                else:
                    cron_schedules = entry.value
                break
        
        if not cron_schedules:
            return SkipReason("No cron schedules found in asset metadata")

        current_time = datetime.now()
        run_requests = []
        
        # Get sensor interval for timing window
        timing_window = timedelta(seconds=int(CONFIG["CRON_SENSOR_INTERVAL_SECONDS"]))

        for report_id, schedule_info in cron_schedules.items():
            cron_expr = schedule_info["cron"]

            # Check if this report should run within the current timing window
            cron = croniter(cron_expr, current_time - timing_window)
            next_run = cron.get_next(datetime)

            if next_run <= current_time:
                # Use the actual cron-determined run date
                cron_run_date = next_run.date()
                
                # Create partition key with precise timing
                partition_key = f"report_{report_id}_{int(next_run.timestamp())}"

                # Add partition if it doesn't exist
                context.instance.add_dynamic_partitions("reports", [partition_key])
                
                logger.info(f"Scheduling report {report_id} ('{schedule_info.get('name', 'Unnamed')}') "
                           f"for cron date {cron_run_date} (partition {partition_key})")

                run_requests.append(
                    RunRequest(
                        partition_key=partition_key,
                        run_config={
                            "ops": {
                                "report_data": {
                                    "config": {
                                        "report_id": int(report_id),
                                        "run_date": cron_run_date.isoformat(),
                                        "fireant_api_url": CONFIG["FIREANT_API_URL"],
                                        "fireant_api_key": CONFIG["FIREANT_API_KEY"],
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
                            "cron_run_date": cron_run_date.isoformat(),
                            "scheduled_time": next_run.isoformat(),
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
    minimum_interval_seconds=int(CONFIG["DISCOVERY_SENSOR_INTERVAL_SECONDS"]),
)
def daily_report_discovery_sensor(context: SensorEvaluationContext):
    """Trigger report discovery at configurable interval"""
    context.log.info("Triggering report discovery")
    return RunRequest()
