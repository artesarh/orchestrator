# Scheduling System Overview

This diagram shows how the report discovery, scheduling, and execution phases work together.

```mermaid
graph TB
    subgraph "Report Discovery Phase"
        A[Django API] --> B[report_cron_schedules Asset]
        B --> C[Asset Materialization<br/>with cron metadata]
    end
    
    subgraph "Scheduling Decision"
        D[definitions.py] --> E{Scheduling Approach?}
        E -->|SENSOR| F[Sensor Active]
        E -->|SCHEDULE| G[Schedule Active]
    end
    
    subgraph "Sensor Path"
        F --> H[report_cron_sensor<br/>runs every N seconds]
        H --> I[Read Asset Events]
        I --> J[Check Cron Timing]
        J --> K[Create RunRequests]
    end
    
    subgraph "Schedule Path"
        G --> L[unified_report_schedule<br/>runs every minute]
        L --> M[Read Asset Events]
        M --> N[Check Cron Timing]
        N --> O[Create RunRequests]
    end
    
    subgraph "Execution"
        K --> P[process_report_job]
        O --> P
        P --> Q[Report Processing Pipeline]
    end
    
    C --> I
    C --> M
```

## Key Components

- **Report Discovery**: Daily discovery of reports and their cron schedules
- **Scheduling Decision**: Environment-based choice between sensor and schedule approaches
- **Sensor Path**: Event-driven scheduling based on asset changes
- **Schedule Path**: Time-based scheduling with configurable intervals
- **Execution**: Unified job processing pipeline 