# Alternative: Asset-based sensor (more efficient)
from dagster import sensor, asset, RunRequest, SensorEvaluationContext
from croniter import croniter
from datetime import datetime, timedelta

@sensor(
    asset_selection=[report_cron_schedules],  # Only runs when this asset updates
    minimum_interval_seconds=60  # Check every minute instead of 5
)
def smart_cron_sensor(context: SensorEvaluationContext):
    """
    More efficient: Only evaluates when schedules change,
    but checks timing more frequently
    """
    try:
        # Get latest schedules (only when they've been updated)
        latest_schedules = context.instance.get_latest_materialization_record("report_cron_schedules")
        
        if not latest_schedules:
            return
            
        schedules_data = latest_schedules.asset_materialization.metadata["schedules"]
        current_time = datetime.now()
        run_requests = []
        
        for report_id, schedule_info in schedules_data.items():
            cron_expr = schedule_info["cron"]
            
            # Check if this should have run in the last minute
            cron_iter = croniter(cron_expr, current_time - timedelta(minutes=1))
            next_run = cron_iter.get_next(datetime)
            
            if current_time - timedelta(minutes=1) <= next_run <= current_time:
                # Create partition for this specific run
                partition_key = f"report_{report_id}_{int(next_run.timestamp())}"
                
                run_requests.append(RunRequest(
                    partition_key=partition_key,
                    run_config={
                        "resources": {
                            "report_config": {
                                "report_id": int(report_id),
                                "modifier_id": schedule_info.get("modifier_id", 1),
                            }
                        }
                    },
                    tags={
                        "report_id": report_id,
                        "scheduled_time": next_run.isoformat(),
                        "cron_schedule": cron_expr
                    }
                ))
        
        return run_requests
        
    except Exception as e:
        context.log.error(f"Sensor error: {e}")
        return [] 