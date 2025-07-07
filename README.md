# Orchestrator

A Dagster-based orchestration system for report processing and external API integration.

## Quick Start

### 1. Environment Setup

The project supports environment-specific configuration files:

```bash
# For development
cp env.dev.example .env.dev

# For testing  
cp env.test.example .env.test

# For production
cp env.prod.example .env.prod
```

### 2. Configure Your Environment

Edit the appropriate `.env.*` file with your actual credentials:

```bash
# Development
vim .env.dev

# Set APP_ENV to activate the environment
export APP_ENV=dev  # or test, prod
```

### 3. Test Connectivity

```bash
# Test Django API connectivity
python test_api_connectivity.py

# Run unit tests
python -m pytest tests/test_api_client.py -k "mocked" -v

# Run integration tests (requires Django API running)
python -m pytest tests/test_api_client.py -m integration -v
```

## Environment Configuration

### Development (`APP_ENV=dev`)
- Uses `.env.dev` configuration
- Django API: `http://localhost:8000` (default)
- Debug logging enabled
- Lower timeouts and retry counts

### Testing (`APP_ENV=test`)
- Uses `.env.test` configuration  
- Django API: `http://localhost:8001` (default)
- Separate test database/resources
- Debug logging enabled

### Production (`APP_ENV=prod`)
- Uses `.env.prod` configuration
- Production URLs and credentials
- Warning-level logging
- Higher retry counts and timeouts

## Configuration Priority

1. **Environment variables** (highest priority)
2. **Environment-specific files** (`.env.dev`, `.env.test`, `.env.prod`)
3. **Base .env file** 
4. **Built-in defaults** (lowest priority)

## Required Environment Variables

```bash
# Django API
DJANGO_API_URL=http://localhost:8000
DJANGO_JWT_TOKEN=your-jwt-token

# External API
EXTERNAL_API_URL=https://external-service.com/api
EXTERNAL_API_KEY=your-api-key

# Azure Storage
AZURE_CONNECTION_STRING=your-connection-string
AZURE_CONTAINER_NAME=your-container
AZURE_ACCOUNT_NAME=your-account
AZURE_ACCOUNT_KEY=your-key
FABRIC_WORKSPACE_ID=your-workspace-id
```

## Development Workflow

1. **Setup**: Copy example files and configure for your environment
2. **Test connectivity**: Run `python test_api_connectivity.py`
3. **Run tests**: Use pytest with appropriate markers
4. **Development**: Use `APP_ENV=dev` for local development
5. **Testing**: Use `APP_ENV=test` for integration testing

## Architecture

- **Assets**: Data processing and storage operations
- **Jobs**: Orchestrated workflows combining multiple assets
- **Sensors**: Event-driven triggers for job execution
- **Resources**: Shared services (API clients, storage)

## Testing Strategy

- **Unit tests**: Mocked external dependencies
- **Integration tests**: Real API calls against test environment
- **Contract tests**: Verify API response schemas
- **Environment isolation**: Separate configs for each environment
