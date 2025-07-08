# Dagster Orchestrator

A comprehensive Dagster-based orchestration system for automated report processing with flexible scheduling approaches.

## Overview

This orchestrator provides a robust, scalable solution for processing reports from a Django API through external services (Fireant API) and storing results in Microsoft Fabric. The system supports three different scheduling methodologies to fit various operational needs.

## Architecture

### Core Components

1. **Report Discovery** - Daily discovery of reports and their cron schedules from Django API
2. **Scheduling System** - Three different approaches for triggering report processing
3. **Processing Pipeline** - Complete data processing workflow from API to storage
4. **Dynamic Job Management** - Intelligent handling of configuration changes

### Data Flow

```
Django API → Report Discovery → Scheduling Logic → Report Processing → Fireant API → Fabric Storage
```

## Three Scheduling Methodologies

### 1. **Sensor Approach** (`SCHEDULING_APPROACH=sensor`)
- **Event-driven**: Monitors asset materializations and timing
- **Efficient**: Only evaluates when report schedules change
- **Configurable interval**: Runs every `CRON_SENSOR_INTERVAL_SECONDS`
- **Best for**: Dynamic environments with frequent schedule changes

### 2. **Schedule Approach** (`SCHEDULING_APPROACH=schedule`)
- **Time-based**: Runs at fixed intervals to check for due reports
- **Predictable**: Consistent evaluation timing
- **Configurable**: `SCHEDULE_CRON_EXPRESSION` and `SCHEDULE_INTERVAL_SECONDS`
- **Best for**: Stable environments with predictable schedules

### 3. **Pipeline Approach** (`SCHEDULING_APPROACH=pipeline`)
- **Individual jobs**: Each report gets its own visible pipeline in Dagster GUI
- **Isolated**: Report failures don't affect others
- **Searchable**: Easy to find and monitor specific reports
- **Best for**: Environments requiring detailed monitoring and control

## Key Features

### Intelligent Change Detection
- **Cron Changes**: Automatically picked up without restart
- **New Reports**: Detected and flagged for restart
- **Removed Reports**: Handled gracefully with restart recommendation
- **Clear Messaging**: Logs explain exactly what changed and why

### Flexible Configuration
- **Environment-based**: Different settings for dev/prod
- **Configurable intervals**: Adjust timing to your needs
- **Multiple approaches**: Choose the best fit for your use case

### Robust Processing
- **Error handling**: Comprehensive error management
- **Retry logic**: Configurable retry attempts
- **Status tracking**: Full job lifecycle monitoring
- **Result storage**: Secure storage in Microsoft Fabric

## Configuration

### Environment Variables

```bash
# Application Environment
APP_ENV=dev  # or 'prod'

# Scheduling Approach (choose one)
SCHEDULING_APPROACH=sensor    # or 'schedule' or 'pipeline'

# Django API
DJANGO_API_URL=http://localhost:8000
DJANGO_JWT_TOKEN=your_jwt_token

# Fireant API
FIREANT_API_URL=https://fireant-api.com
FIREANT_API_KEY=your_api_key

# Microsoft Fabric
AZURE_TENANT_ID=your_tenant_id
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret
AZURE_ACCOUNT_NAME=your_account_name
FABRIC_WORKSPACE_ID=your_workspace_id

# Scheduling Configuration
CRON_SENSOR_INTERVAL_SECONDS=60
DISCOVERY_SENSOR_INTERVAL_SECONDS=86400
SCHEDULE_CRON_EXPRESSION=0 * * * *  # Every hour
SCHEDULE_INTERVAL_SECONDS=3600      # 1 hour window

# Processing Configuration
MAX_POLL_ATTEMPTS=120
POLL_INTERVAL_SECONDS=60
MAX_RETRIES=3
TIMEOUT_SECONDS=30
```

### Configuration Examples

#### Development (30-minute intervals)
```bash
APP_ENV=dev
SCHEDULING_APPROACH=schedule
SCHEDULE_CRON_EXPRESSION=*/30 * * * *
SCHEDULE_INTERVAL_SECONDS=1800
```

#### Production (hourly intervals)
```bash
APP_ENV=prod
SCHEDULING_APPROACH=sensor
CRON_SENSOR_INTERVAL_SECONDS=60
DISCOVERY_SENSOR_INTERVAL_SECONDS=86400
```

#### Individual Pipeline Monitoring
```bash
APP_ENV=prod
SCHEDULING_APPROACH=pipeline
SCHEDULE_CRON_EXPRESSION=0 * * * *
SCHEDULE_INTERVAL_SECONDS=3600
```

## Quick Start

1. **Clone and Install**
   ```bash
   git clone <repository>
   cd orchestrator
   pip install -e .
   ```

2. **Configure Environment**
   ```bash
   cp env.example .env
   # Edit .env with your settings
   ```

3. **Run Dagster**
   ```bash
   dagster dev
   ```

4. **Access Web UI**
   - Open http://localhost:3000
   - View jobs, schedules, and sensors
   - Monitor pipeline execution

## Processing Pipeline

### Assets
1. **report_data** - Fetch and transform report data from Django API
2. **external_job_submission** - Submit job to Fireant API
3. **local_job_record** - Create job record in Django database
4. **job_completion_status** - Poll Fireant API until job completes
5. **results_storage** - Download results and save to Fabric lakehouse

### Job Flow
```
Report Discovery → Cron Evaluation → Job Trigger → Data Processing → External API → Result Storage
```

## Monitoring and Debugging

### Dagster Web UI
- **Jobs**: View all report processing jobs
- **Schedules**: Monitor scheduling status
- **Sensors**: Check sensor evaluation logs
- **Assets**: Track asset materializations
- **Runs**: Detailed execution history

### Logging
- **Structured logging**: JSON format with metadata
- **Change detection**: Clear messages about configuration changes
- **Error tracking**: Comprehensive error information
- **Performance metrics**: Timing and resource usage

### Individual Pipeline Benefits
When using `SCHEDULING_APPROACH=pipeline`:
- Each report appears as separate job in GUI
- Individual run histories and metrics
- Easy filtering and searching
- Isolated failure handling
- Report-specific partitions

## Testing

### Test Structure
```
tests/
├── test_api_connectivity.py      # Django API endpoint tests
├── test_config.py                # Configuration system tests
├── test_cron_validation.py       # Cron syntax and timing tests
├── test_fireant_integration.py   # Fireant API integration tests
├── test_fabric_storage.py        # Azure Fabric storage tests
├── test_dagster_components.py    # Dagster-specific tests
└── test_schema_transformer.py    # Data transformation tests
```

### Running Tests
```bash
# All tests
pytest

# Specific test categories
pytest tests/test_api_connectivity.py
pytest tests/test_config.py
pytest -m integration  # Integration tests only
```

## Development

### Adding New Reports
1. Add report with cron schedule in Django admin
2. Wait for daily discovery (or trigger manually)
3. Check logs for change detection
4. Restart if new reports detected

### Modifying Cron Schedules
1. Update cron in Django admin
2. No restart needed - automatically picked up
3. Check logs for confirmation

### Debugging Issues
1. Check Dagster Web UI for run details
2. Review logs for error messages
3. Use individual pipeline approach for detailed monitoring
4. Test with development environment settings

## Architecture Diagrams

See `schematics/` directory for detailed architecture diagrams:
- `scheduling_overview.md` - Overall system architecture
- `individual_jobs_architecture.md` - Pipeline approach details
- `cron_change_handling.md` - Change detection and handling

## Contributing

1. Follow the existing code structure
2. Add tests for new functionality
3. Update documentation
4. Test with all three scheduling approaches
5. Ensure environment compatibility
