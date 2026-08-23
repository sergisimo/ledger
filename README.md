# Ledger - Investment Portfolio Status & Metrics Service

> **Status**: 🚧 In Progress

## Overview

Ledger is a REST service designed to manage and monitor investment portfolio accounts. It provides functionality to:

- **Register** current status of all accounts in an investment portfolio
- **Extract** metrics and insights from portfolio data
- **Query** and manage account information through RESTful APIs

## Architecture

### Deployment

The project includes complete deployment configurations for containerized environments:

- **Docker**: Containerized application deployable to any Docker-compatible environment
- **Kubernetes (K8s)**: Helm charts and Kubernetes manifests for cloud-native deployments with automatic scaling and health management

### Storage

- **Database**: SQLite for lightweight, serverless data persistence
- **Persistent Volume**: Database is stored in a Kubernetes persistent volume for data durability across pod restarts

### Local Development

```bash
# Install dependencies
make init

# Run the service
make local
```

## License

See [LICENSE](LICENSE) file for details.
