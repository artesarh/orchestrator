# Individual Jobs Architecture

This diagram shows how individual report jobs are created and configured in the Dagster GUI.

```mermaid
graph TB
    subgraph "Dagster GUI - Jobs View"
        A[process_report_job<br/>Unified Pipeline]
        B[process_report_1<br/>Daily Sales Report]
        C[process_report_2<br/>Weekly Inventory Report]
        D[process_report_3<br/>Monthly Financial Report]
        E[process_report_N<br/>Other Reports...]
    end
    
    subgraph "Configuration Options"
        F[SCHEDULE_CRON_EXPRESSION<br/>0 * * * * = Every hour]
        G[SCHEDULE_INTERVAL_SECONDS<br/>3600 = 1 hour window]
        H[*/30 * * * * = Every 30 min<br/>1800 = 30 min window]
    end
    
    subgraph "Scheduling Logic"
        I[unified_report_schedule<br/>Runs at configured interval]
        I --> J[Check last N seconds for due reports]
        J --> K[Create RunRequests for due reports]
        K --> L[Each report gets its own partition]
    end
    
    subgraph "Individual Report Benefits"
        M[✓ Separate pipeline in GUI]
        N[✓ Individual run history]
        O[✓ Report-specific partitions]
        P[✓ Easy filtering and search]
        Q[✓ Isolated failure handling]
    end
    
    F --> I
    G --> J
    H --> I
    
    K --> B
    K --> C
    K --> D
    
    B --> M
    C --> N
    D --> O
```

## Configuration Examples

### Hourly Checks (Production)
```bash
SCHEDULE_CRON_EXPRESSION=0 * * * *  # Every hour at minute 0
SCHEDULE_INTERVAL_SECONDS=3600      # 1 hour window
```

### 30-Minute Checks (Development)
```bash
SCHEDULE_CRON_EXPRESSION=*/30 * * * *  # Every 30 minutes
SCHEDULE_INTERVAL_SECONDS=1800         # 30 minute window
```

### 2-Hour Checks (Low Frequency)
```bash
SCHEDULE_CRON_EXPRESSION=0 */2 * * *  # Every 2 hours
SCHEDULE_INTERVAL_SECONDS=7200        # 2 hour window
```

## Benefits

- **Visibility**: Each report appears as its own job in the GUI
- **Isolation**: Failures in one report don't affect others
- **Searchability**: Easy to find and filter specific reports
- **Monitoring**: Individual run histories and metrics
- **Debugging**: Easier to trace issues for specific reports 