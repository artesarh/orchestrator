from typing import Dict, Any, Tuple


def transform_report_schema(api_response: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    Transform the API response to move modifiers inside report
    and event group to top level.
    Returns (transformed_data, event_type)
    """
    data = api_response.get("data", {})

    # Extract components
    report = data.get("report", {})
    modifier = data.get("modifier", {})
    eventgroup = data.get("eventgroup", {})

    events = eventgroup.get("events", [])

    # Create a set of event types to check consistency
    event_types = set()
    for event in events:
        event_type = event.get("event_type")
        if event_type:
            event_types.add(event_type.lower())  # Normalize to lowercase

    # Check that there's only one type of event
    if len(event_types) > 1:
        raise ValueError(f"Multiple event types found: {event_types}. Expected only one type.")
    elif len(event_types) == 0:
        raise ValueError("No event type found in events")

    # Get the single event type
    event_type = next(iter(event_types))

    # Determine the events key name based on event type
    events_key = "events"  # default
    if event_type == "ring":
        events_key = "rings"
    elif event_type == "geo":
        events_key = "geos"
    elif event_type == 'box':
        events_key = 'boxes'
    else:
        events_key = event_type + 's'

    # Create transformed structure
    transformed = {
        **{k: v for k, v in report.items() if k not in ['cron', 'id']},
        **{'as_at_date': modifier['as_at_date'],
           'fx_date': modifier['fx_date']},
        events_key: events,
    }

    # Preserve metadata if it exists
    if "meta" in api_response:
        transformed["meta"] = api_response["meta"]

    return transformed, event_type
