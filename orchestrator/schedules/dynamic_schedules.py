from dagster import schedule, RunRequest, SkipReason, DefaultScheduleStatus, AssetKey, EventRecordsFilter, DagsterEventType
from ..jobs.report_pipeline import process_report_job
from ..utils.config import CONFIG
from croniter import croniter
from datetime import datetime, timedelta


@schedule(
    job=process_report_job,
    cron_schedule=CONFIG["SCHEDULE_CRON_EXPRESSION"].split('#')[0].strip(),  # Clean cron expression, remove comments
    default_status=DefaultScheduleStatus.RUNNING,
    name="unified_report_schedule",
    description="Unified schedule that checks all reports and runs those due now"
)
def unified_report_schedule(context):
    """
    Single schedule that reads from report_cron_schedules asset and 
    determines which reports need to run at the current time
    """
    logger = context.log
    
    try:
        # Get the latest report schedules from the asset
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
        
        # Get configurable timing window for checking reports
        timing_window = timedelta(seconds=int(CONFIG["SCHEDULE_INTERVAL_SECONDS"]))
        
        # Check each report's cron to see if it should run now
        for report_id_str, schedule_info in cron_schedules.items():
            report_id = int(report_id_str)
            cron_expr = schedule_info["cron"]
            report_name = schedule_info["name"]

            # Check if this report should run within the configured timing window
            cron = croniter(cron_expr, current_time - timing_window)
            next_run = cron.get_next(datetime)

            if next_run <= current_time:
                # This report should run now
                run_date = next_run.date()
                
                # Create partition key with precise timing
                partition_key = f"report_{report_id}_{int(next_run.timestamp())}"

                # Add partition if it doesn't exist
                context.instance.add_dynamic_partitions("reports", [partition_key])
                
                logger.info(f"Scheduling report {report_id} ('{report_name}') "
                           f"for scheduled time {next_run} (partition {partition_key})")

                run_requests.append(
                    RunRequest(
                        partition_key=partition_key,
                        run_config={
                            "ops": {
                                "report_data": {
                                    "config": {
                                        "report_id": report_id,
                                        "run_date": run_date.isoformat(),
                                        "fireant_api_url": CONFIG["FIREANT_API_URL"],
                                        "fireant_api_key": CONFIG["FIREANT_API_KEY"],
                                    }
                                }
                            }
                        },
                        tags={
                            "report_id": str(report_id),
                            "report_name": report_name,
                            "cron_schedule": cron_expr,
                            "scheduled_date": run_date.isoformat(),
                            "scheduled_time": next_run.isoformat(),
                        },
                    )
                )

        if run_requests:
            logger.info(f"Triggering {len(run_requests)} report runs")
            return run_requests
        else:
            return SkipReason("No reports due to run at this time")

    except Exception as e:
        logger.error(f"Error in unified schedule: {str(e)}")
        return SkipReason(f"Error in schedule: {str(e)}") 