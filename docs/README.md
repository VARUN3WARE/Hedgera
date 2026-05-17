# AEGIS documentation

Detailed guides for the Hedgera / AEGIS trading system. Start with the [repository README](../README.md) for a short overview, then use the topics below.

## Getting started

| Guide | Description |
|-------|-------------|
| [Setup](setup.md) | Prerequisites, installation, environment variables |
| [Quick start](quickstart.md) | First run: historical data, pipeline, monitoring |
| [Docker](docker.md) | Compose services, ports, health checks |

## System design

| Guide | Description |
|-------|-------------|
| [Architecture](architecture.md) | Layers, data flow, ASCII diagram |
| [Components](components.md) | Producers, streaming, FinRL, agents, execution |
| [Pipelines](pipelines.md) | Entrypoints, workflows, log layout |
| [Project structure](project-structure.md) | Repository tree |

## Operations

| Guide | Description |
|-------|-------------|
| [API reference](api.md) | Redis streams, MongoDB schema, Alpaca MCP |
| [Testing](testing.md) | Pytest layout, local CI scripts, FinRL suites |
| [Monitoring](monitoring.md) | Live stats, logs, optional dashboard bridge |
| [Troubleshooting](troubleshooting.md) | Common failures, debug commands, performance |

## Related READMEs

- [Backend](../backend/README.md) — Python package layout and API entry
- [Local CI](../scripts/ci/README.md) — `run-tests.sh` wrapper
- [FinRL integration](../backend/finrl_integration/README.md) — model and training notes
