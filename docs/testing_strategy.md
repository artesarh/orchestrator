# Testing Strategy for DjangoAPIClient

## Problem: Testing POST/PATCH Without Hitting Production

Testing write operations (POST/PATCH) against APIs without affecting production data requires a multi-layered approach.

## Recommended Testing Pyramid

### 1. Unit Tests (Mocked) - 70%
```bash
pytest tests/test_api_client.py -k "not integration"
```
**Purpose**: Verify client logic without external dependencies
**Coverage**: HTTP method, URL construction, headers, payload formatting
**Speed**: Fast (< 1s)
**Reliability**: High

### 2. Integration Tests (Test Environment) - 20%
```bash
pytest tests/test_api_client_integration.py -m local
```
**Purpose**: Test against real API with test data
**Environment**: Local Django API on different port (8001)
**Database**: Separate test database
**Speed**: Medium (5-10s)

### 3. Staging Tests - 10%
```bash
pytest tests/test_api_client_integration.py -m staging
```
**Purpose**: Test against production-like environment
**Environment**: Staging server with production configuration
**Speed**: Slow (10-30s)

## Environment Setup

### Option 1: Local Test API (Recommended)
Run Django on different port with test database:
```bash
# Terminal 1: Test Django API
cd /path/to/django/project
python manage.py runserver 8001 --settings=myproject.settings.test

# Terminal 2: Run tests
pytest tests/test_api_client_integration.py -m local
```

### Option 2: Docker Test Environment
```dockerfile
# docker-compose.test.yml
version: '3.8'
services:
  test-api:
    build: .
    ports:
      - "8001:8000"
    environment:
      - DATABASE_URL=postgres://test:test@test-db:5432/testdb
      - DEBUG=True
  test-db:
    image: postgres:15
    environment:
      - POSTGRES_DB=testdb
      - POSTGRES_USER=test
      - POSTGRES_PASSWORD=test
```

### Option 3: Staging Environment
Deploy identical infrastructure with test data:
- Same Docker images as production
- Same environment variables (except database)
- Separate database with test data
- Same network configuration

## Preventing Dev/Prod Differences

### 1. Infrastructure as Code
```yaml
# Same deployment config for all environments
# Only differs in environment variables
apiVersion: apps/v1
kind: Deployment
metadata:
  name: django-api-${ENVIRONMENT}
spec:
  template:
    spec:
      containers:
      - name: django
        image: myregistry/django-api:${VERSION}
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: ${ENVIRONMENT}-secrets
              key: database-url
```

### 2. Configuration Management
```python
# settings/base.py - shared settings
# settings/test.py - test overrides
# settings/staging.py - staging overrides  
# settings/production.py - production settings

# All inherit from base.py
from .base import *

# Only override what's different
DATABASE_URL = os.getenv('TEST_DATABASE_URL')
```

### 3. Contract Testing
```python
# tests/contracts/test_api_contract.py
def test_create_job_contract():
    """Test that API contract matches expected schema"""
    schema = {
        "type": "object",
        "required": ["id", "report", "status"],
        "properties": {
            "id": {"type": "integer"},
            "report": {"type": "integer"},
            "status": {"type": "string", "enum": ["submitted", "running", "completed"]}
        }
    }
    
    # Test against mocked response
    response = mock_create_job_response()
    validate(response, schema)
    
    # Test against staging
    response = staging_client.create_job(test_data)
    validate(response, schema)
```

### 4. Smoke Tests in Production
```python
# Run read-only tests in production after deployment
@pytest.mark.production_smoke
def test_production_health():
    """Verify production API is healthy without modifying data"""
    response = prod_client.get_all_reports()
    assert response is not None
    assert "data" in response
```

## CI/CD Pipeline

```yaml
# .github/workflows/test.yml
name: Test Pipeline

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run unit tests
        run: pytest tests/ -k "not integration"

  integration-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: test
    steps:
      - uses: actions/checkout@v3
      - name: Start test Django API
        run: |
          python manage.py migrate --settings=myproject.settings.test
          python manage.py runserver 8001 --settings=myproject.settings.test &
      - name: Run integration tests
        run: pytest tests/ -m "integration and local"

  staging-tests:
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - name: Run staging tests
        env:
          DJANGO_STAGING_API_URL: ${{ secrets.STAGING_API_URL }}
          DJANGO_STAGING_JWT_TOKEN: ${{ secrets.STAGING_JWT_TOKEN }}
        run: pytest tests/ -m "integration and staging"
```

## Running Tests

```bash
# Development: Quick feedback
pytest tests/test_api_client.py -k "not integration" -x

# Pre-commit: Local integration
pytest tests/test_api_client_integration.py -m local

# CI/CD: Full test suite
pytest tests/ -m "not staging"

# Pre-deployment: Staging validation
pytest tests/ -m staging

# Post-deployment: Production smoke tests
pytest tests/ -m production_smoke
```

## Risk Mitigation

1. **Schema Validation**: Use JSON Schema to validate API responses
2. **Database Transactions**: Wrap tests in transactions and rollback
3. **Test Data Isolation**: Use unique test identifiers
4. **Monitoring**: Alert on unusual API patterns during tests
5. **Feature Flags**: Test new features behind flags before full deployment
6. **Blue/Green Deployments**: Test against production clone before switching traffic

## Tools to Consider

- **Pact**: Contract testing framework
- **WireMock**: API mocking for integration tests
- **Testcontainers**: Docker containers for integration tests
- **Newman**: Postman collection runner for API tests
- **Artillery**: Load testing for API performance 