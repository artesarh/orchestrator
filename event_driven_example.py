# Alternative: Event-driven approach
from dagster import sensor, DefaultSensorStatus, RunRequest
import requests

@sensor(
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30  # Check for external events every 30s
)
def webhook_trigger_sensor(context):
    """
    Check for external triggers (webhooks, queue messages, etc.)
    instead of time-based polling
    """
    try:
        # Option 1: Check a webhook endpoint
        response = requests.get("http://your-django-app/api/pending-reports/")
        pending_reports = response.json().get("pending", [])
        
        run_requests = []
        for report in pending_reports:
            partition_key = f"report_{report['id']}_{report['trigger_time']}"
            
            run_requests.append(RunRequest(
                partition_key=partition_key,
                run_config={
                    "ops": {
                        "report_data": {
                            "config": {
                                "report_id": report["id"],
                                "modifier_id": report["modifier_id"],
                            }
                        }
                    }
                },
                tags={"trigger_type": "webhook", "report_id": str(report["id"])}
            ))
            
            # Mark as processed
            requests.post(f"http://your-django-app/api/reports/{report['id']}/mark-processed/")
        
        return run_requests
        
    except Exception as e:
        context.log.error(f"Webhook sensor error: {e}")
        return []

# Alternative: Database polling (more efficient than cron checking)
@sensor(minimum_interval_seconds=60)
def database_queue_sensor(context):
    """
    Poll a 'report_queue' table instead of parsing cron expressions
    """
    try:
        # Your Django app populates a queue table based on cron schedules
        response = requests.get("http://your-django-app/api/report-queue/")
        queued_reports = response.json().get("queue", [])
        
        run_requests = []
        for queued_report in queued_reports:
            partition_key = f"report_{queued_report['report_id']}_{queued_report['queue_id']}"
            
            run_requests.append(RunRequest(
                partition_key=partition_key,
                run_config={
                    "ops": {
                        "report_data": {
                            "config": {
                                "report_id": queued_report["report_id"],
                                "modifier_id": queued_report["modifier_id"],
                            }
                        }
                    }
                }
            ))
            
            # Remove from queue
            requests.delete(f"http://your-django-app/api/report-queue/{queued_report['queue_id']}/")
        
        return run_requests
        
    except Exception as e:
        context.log.error(f"Queue sensor error: {e}")
        return [] 