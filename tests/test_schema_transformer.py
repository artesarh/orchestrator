import json
from orchestrator.utils.schema_transformer import transform_report_schema

# Test data based on your example
test_data = {
    "meta": {
        "report_id": 1,
        "event_group_id": 1,
        "modifier_id": 1,
        "links": {
            "report": "http://127.0.0.1:8000/api/reports/1/",
            "event_group": "http://127.0.0.1:8000/api/event-groups/1/",
            "modifier": "http://127.0.0.1:8000/api/report-modifiers/1/"
        }
    },
    "data": {
        "report": {
            "id": 1,
            "name": "New Report",
            "peril": "Unknown",
            "dr": 1,
            "cron": "0 0 * * 0",
            "cob": "string",
            "loss_perspective": "Gross",
            "is_apply_calibration": True,
            "is_apply_inflation": True,
            "is_tag_outwards_ptns": False,
            "is_location_breakout": False,
            "is_ignore_missing_lat_lon": True,
            "location_breakout_max_events": 500000,
            "location_breakout_max_locations": 1000000,
            "priority": "AboveNormal",
            "ncores": 24,
            "gross_node_id": 0,
            "net_node_id": 0,
            "rollup_context_id": 42,
            "dynamic_ring_loss_threshold": 5000000,
            "blast_radius": 50,
            "no_overlap_radius": 50,
            "is_valid": True,
            "created": "2025-06-26T09:09:11.203367Z",
            "updated": "2025-06-26T12:28:41.110843Z",
            "event_group": 1
        },
        "eventgroup": {
            "id": 1,
            "name": "string",
            "created": "2025-06-26T08:54:11.963000Z",
            "updated": "2025-06-26T08:54:15.932967Z",
            "events": [
                {
                    "id": 1,
                    "name": "string",
                    "description": "string",
                    "is_valid": True,
                    "country": "string",
                    "area": "string",
                    "subarea": "string",
                    "subarea2": "string",
                    "event_type": "geo"
                },
                {
                    "id": 2,
                    "name": "string",
                    "description": "string",
                    "is_valid": True,
                    "country": "string",
                    "area": "string",
                    "subarea": "string",
                    "subarea2": "string",
                    "event_type": "geo"
                },
                {
                    "id": 3,
                    "name": "string",
                    "description": "string",
                    "is_valid": True,
                    "country": "string",
                    "area": "string",
                    "subarea": "string",
                    "subarea2": "string",
                    "event_type": "geo"
                }
            ]
        },
        "modifier": {
            "id": 1,
            "as_at_date": "2025-07-01",
            "fx_date": "2025-07-01",
            "quarter": 1,
            "year": 2025,
            "month": 7,
            "day": 1
        }
    }
}

if __name__ == "__main__":
    try:
        result = transform_report_schema(test_data)
        print("Transformation successful!")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {e}") 