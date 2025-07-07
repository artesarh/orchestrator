# Alternative Dagster Architecture Patterns

This directory contains alternative implementations and design patterns for the orchestrator system. These are **reference implementations** that demonstrate different approaches to building Dagster-based orchestration systems.

## 📁 Files Overview

- `alternative_sensor_example.py` - Asset-based sensor with improved efficiency
- `event_driven_example.py` - Event-driven and queue-based sensor patterns
- `README.md` - This file

## 🎯 Current vs Alternative Approaches

### Current Implementation (`orchestrator/sensors/report_sensor.py`)

**Pattern**: Timer-based cron evaluation with asset dependency
**Characteristics**:
- ✅ Simple and reliable
- ✅ Works well for 10-100 reports
- ✅ Easy to understand and maintain
- ⚠️ Polls every 60 seconds regardless of activity
- ⚠️ Some latency (up to 60s delay)

```python
@sensor(job=process_report_job, minimum_interval_seconds=CRON_SENSOR_INTERVAL)
def report_cron_sensor(context):
    # Evaluates cron schedules on timer
```

---

## 🚀 Alternative Pattern 1: Smart Asset-Based Sensor

**File**: `alternative_sensor_example.py`

**Pattern**: Reactive sensor that only evaluates when schedules change
**Benefits**:
- 🏃 More efficient - only runs when schedules update
- 🎯 Better resource utilization
- 📈 Scales better with many reports
- 🔧 Same complexity as current approach

**When to Use**:
- 100-1000 reports
- Frequent schedule changes
- Want to reduce Dagster overhead
- Performance optimization needed

```python
@sensor(
    asset_selection=[report_cron_schedules],  # Only runs when this asset changes
    minimum_interval_seconds=60
)
def smart_cron_sensor(context):
    # Only evaluates when schedules have been updated
```

**Migration Effort**: Low - mostly drop-in replacement

---

## ⚡ Alternative Pattern 2: Event-Driven Architecture

**File**: `event_driven_example.py`

**Pattern**: External event triggers instead of cron-based scheduling
**Benefits**:
- ⚡ Near real-time execution
- 🎯 No unnecessary polling
- 🔗 Better integration with external systems
- 📊 More granular control

**Two Sub-Patterns Shown**:

### A. Webhook/Event Polling
```python
def webhook_trigger_sensor(context):
    # Check for external triggers (webhooks, queue messages)
    response = requests.get("http://your-django-app/api/pending-reports/")
```

**Use Case**: External systems can trigger reports immediately

### B. Database Queue Pattern
```python
def database_queue_sensor(context):
    # Poll a 'report_queue' table instead of parsing cron expressions
    response = requests.get("http://your-django-app/api/report-queue/")
```

**Use Case**: Django app manages scheduling, Dagster just processes queue

**When to Use**:
- 1000+ reports
- Real-time requirements
- Complex triggering logic
- External system integration needs
- Multiple trigger sources

**Migration Effort**: High - requires Django API changes

---

## 📊 Performance Comparison

| Approach | Latency | Resource Usage | Complexity | Scalability |
|----------|---------|----------------|------------|-------------|
| **Current** | 0-60s | Constant polling | Low | Good (100s) |
| **Smart Asset** | 0-60s | Reduced polling | Low | Better (1000s) |
| **Event-Driven** | Near-instant | Minimal | High | Excellent (10k+) |

---

## 🛣️ Migration Path

### Phase 1: Current System ✅
- **Status**: Implemented
- **Good for**: Development, initial production
- **Limit**: ~100 reports

### Phase 2: Smart Asset-Based (Optional Optimization)
- **Effort**: 1-2 hours
- **Benefit**: 30-50% resource reduction
- **Migration**: Replace sensor function

```python
# Simple migration - replace current sensor with:
from docs.examples.alternative_sensor_example import smart_cron_sensor
```

### Phase 3: Event-Driven (Major Architecture Change)
- **Effort**: 1-2 weeks
- **Benefit**: Real-time processing, unlimited scale
- **Requirements**: Django API changes, queue infrastructure

---

## 🔧 How to Test Alternatives

### Testing Smart Asset Sensor

```bash
# 1. Backup current sensor
cp orchestrator/sensors/report_sensor.py orchestrator/sensors/report_sensor.py.backup

# 2. Copy alternative implementation
cp docs/examples/alternative_sensor_example.py orchestrator/sensors/smart_sensor.py

# 3. Update definitions.py to use smart_cron_sensor instead

# 4. Test and compare performance
```

### Testing Event-Driven Approach

```bash
# 1. Add Django endpoints (see event_driven_example.py comments)
# GET /api/pending-reports/
# POST /api/reports/{id}/mark-processed/
# GET /api/report-queue/

# 2. Implement alternative sensor
# 3. A/B test with current system
```

---

## 🎯 Recommendations

### **Keep Current System If**:
- Report count < 100
- Schedule changes are rare
- 1-minute delay is acceptable
- Team prefers simplicity

### **Consider Smart Asset Sensor If**:
- Report count 100-1000
- Want better performance
- Easy migration acceptable
- Performance optimization needed

### **Consider Event-Driven If**:
- Report count > 1000
- Real-time requirements
- Complex external integrations
- Team has DevOps capacity

---

## 🧪 Implementation Notes

### Smart Asset Sensor Implementation
```python
# Key difference: asset_selection parameter
@sensor(asset_selection=[report_cron_schedules])  # This is the magic
```

### Event-Driven Django Integration
```python
# Required Django endpoints:
# models.py
class ReportQueue(models.Model):
    report_id = models.IntegerField()
    modifier_id = models.IntegerField()
    trigger_time = models.DateTimeField()
    processed = models.BooleanField(default=False)

# views.py  
def pending_reports(request):
    return JsonResponse({
        "pending": list(ReportQueue.objects.filter(processed=False).values())
    })
```

---

## 🎓 Learning Value

These examples demonstrate:

1. **Dagster Sensor Patterns**: Different ways to trigger jobs
2. **Performance Optimization**: Resource-conscious design
3. **Architecture Evolution**: How systems grow and change
4. **Integration Patterns**: External system communication
5. **Scalability Planning**: Designing for growth

---

## 🚨 Important Notes

- These are **reference implementations**, not production code
- Test thoroughly before using in production
- Consider your specific requirements and constraints
- Start simple, optimize when needed
- Monitor performance after any changes

---

## 📚 Further Reading

- [Dagster Sensor Documentation](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors)
- [Asset-based Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#asset-sensors)
- [Performance Best Practices](https://docs.dagster.io/guides/dagster/performance)

---

*These patterns represent real-world architectural decisions. Choose the approach that best fits your current needs and growth trajectory.* 