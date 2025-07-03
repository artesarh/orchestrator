from typing import Dict, Any


def transform_report_schema(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform the API response to move modifiers inside report
    and event group to top level
    """
    data = api_response.get("data", {})

    # Extract components
    report = data.get("report", {})
    modifiers = data.get("modifiers", {})
    eventgroup = data.get("eventgroup", {})

    events = eventgroup.get("events", [])

    # Create a set of event types to check consistency
    event_types = set()
    for event in events:
        event_type = event.get("type")
        if event_type:
            event_types.add(event_type)

    # Check that there's only one type of event
    if len(event_types) > 1:
        raise ValueError(f"Multiple event types found: {event_types}. Expected only one type.")
    
    # Determine the events key name based on event type
    events_key = "events"  # default
    if len(event_types) == 1:
        event_type = next(iter(event_types))
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
        "report": {
            **report,
            # Add all modifier keys to the top level report
            **modifiers,
            # Add events to report with appropriate key name
            events_key: events,
        },
    }

    # Preserve metadata if it exists
    if "meta" in api_response:
        transformed["meta"] = api_response["meta"]

    return transformed
