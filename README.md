# DataLakeHouseMCP: Model Context Protocol (MCP) Server

## Overview

DataLakeHouseMCP is a Python-based MCP server built using FastMCP. It exposes resources and tools for interacting with data infrastructure components like Kafka, Flink, and Trino/Iceberg. The server is designed to be discoverable and usable by AI-powered MCP clients such as Copilot Chat (VSCode/IntelliJ) and Claude Desktop.

## Prerequisites

- **Lakehouse Setup**: Before using this MCP server, you must bring up the lakehouse environment by following the instructions in the [flink-iceberg GitHub repository](https://github.com/zabi82/flink-iceberg). Complete all setup steps in that repo's README to ensure Kafka, Flink, Trino, and Iceberg are running locally.
- **Python 3.8+** must be installed on your system. Download from [python.org](https://www.python.org/downloads/).
- **uv package manager** (recommended for fast installs):
  - Install via pip: `pip install uv`
  - More info: [uv documentation](https://github.com/astral-sh/uv)

## File Structure

- `main.py`: MCP server entry point, tool registration.
- `kafka_tools.py`: Kafka-related MCP tools.
- `flink_tools.py`: Flink-related MCP tools.
- `trino_tools.py`: Trino/Iceberg-related MCP tools.
- `env_config.py`: Centralized environment variable loader for all tools.
- `logging_config.py`: Centralized logging configuration for all tools.
- `requirements.txt`: Python dependencies.

## Features

- **Kafka Tools**: List topics, peek latest messages, dynamic support for Avro/JSON/Text.
- **Flink Tools**: Cluster metrics, job listing, job details, **TaskManager listing and details**.
- **Trino/Iceberg Tools**: List catalogs, schemas, tables, get table schema, execute queries, time travel queries, list snapshots.
- **MCP Discovery**: All tools/resources are annotated for easy discovery by MCP clients.

### Timestamp Format for Iceberg Time Travel Queries
When using the `iceberg_time_travel_query` tool, the `timestamp` parameter must be in ISO 8601 format.
Example: `'2024-09-12T15:30:45.123456+05:30'`
This format includes date, time (with optional milliseconds), and timezone offset.

## Installation

1. **Clone the repository**

```bash
git clone <your-repo-url>
cd DataLakeHouseMCP
```

2. **Create and activate a virtual environment (recommended)**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. **Install dependencies using uv**

```bash
uv pip install -r requirements.txt
```

## Running the MCP Server (Stdio Mode)

This MCP server runs in local stdio mode and does **not** expose an HTTP endpoint. It is intended to be launched and connected to directly by MCP clients (such as Copilot Chat or Claude Desktop) using standard input/output.

```bash
uv run "/path/to/DataLakeHouseMCP/main.py"
```

## Configuring MCP Clients

### Claude Desktop

1. Go to Settings > Integrations > Model Context Protocol (MCP).
2. Click "Add MCP Server" and set the executable path to your MCP server (e.g. `uv`).
3. Set arguments to: `run /path/to/DataLakeHouseMCP/main.py`
4. Optionally, set the working directory to your project folder (e.g. `/path/to/DataLakeHouseMCP`).
5. Save and enable the integration.
6. Claude will launch the MCP server in stdio mode and auto-discover available MCP tools and resources.
7. Example `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "mcp-data-lakehouse": {
      "command": "uv",
      "args": [
        "run",
        "/path/to/DataLakeHouseMCP/main.py"
      ]
    }
  }
}
```

### Copilot Chat in VSCode

1. Open Copilot Chat and go to MCP server configuration (usually in the extension settings or via command palette).
2. Add a new MCP server:
   - Executable: `uv`
   - Arguments: `run main.py`
   - Working directory: `/path/to/DataLakeHouseMCP`
3. Save the configuration.
4. Example `mcp.json`:
```json
{
  "servers": {
    "mcp-data-lakehouse-test": {
      "type": "stdio",
      "command": "uv",
      "args": ["run", "/path/to/DataLakeHouseMCP/main.py"]
    }
  },
  "inputs": []
}
```

### Copilot Chat in IntelliJ

1. Open Copilot Chat and go to MCP server configuration (usually in plugin settings).
2. Add a new MCP server:
   - Executable: `uv`
   - Arguments: `run /path/to/DataLakeHouseMCP/main.py`
   - Working directory: `/path/to/DataLakeHouseMCP`
3. Save the configuration.
4. Example `mcp.json`:
```json
{
  "servers": {
    "mcp-data-lakehouse": {
      "type": "stdio",
      "command": "uv",
      "args": [
        "run",
        "/path/to/DataLakeHouseMCP/main.py"
      ]
    }
  }
}
```

## MCP Tools & Features

### Kafka Tools
- **List Kafka Topics**  
  `kafka_topics` — Lists all Kafka topics available in the local cluster.
- **Peek Kafka Topic**  
  `peek_kafka_topic` — Retrieves the latest N messages from a specified Kafka topic (supports Avro, JSON, and plain text).

### Flink Tools
- **Cluster Overview**  
  `flink_overview` — Shows Flink cluster metrics: number of task managers, slots, jobs running/finished/cancelled/failed.
- **JobManager Metrics**  
  `flink_jobmanager_metrics` — Returns JobManager metrics (heap memory, CPU load, JVM/process stats).
- **TaskManagers Metrics**  
  `flink_taskmanagers_metrics` — Returns TaskManagers metrics (heap memory, network IO, slot utilization).
- **List Flink Jobs**  
  `flink_jobs` — Lists all Flink jobs running on the cluster (IDs, names, status).
- **Flink Job Details**  
  `flink_job_details` — Returns details for one or more Flink jobs by job ID(s): status, vertices, configuration.  
  _Note: Accepts a list of job IDs._
- **Probe JobManager Metrics**  
  `probe_jobmanager_metrics` — Probe one or more JobManager metrics by name (pass a list, even for a single metric).
- **Probe TaskManager Metrics**  
  `probe_taskmanager_metrics` — Probe one or more TaskManager metrics by name and TaskManager ID (pass a list, even for a single metric).
- **List TaskManagers**  
  `flink_taskmanagers` — Lists all Flink TaskManagers and their details.

### Trino & Iceberg Tools
- **List Iceberg Tables**  
  `trino_iceberg_tables` — Lists all Iceberg tables in a specified Trino catalog.
- **List Trino Catalogs**  
  `trino_catalogs` — Lists all catalogs available in the Trino cluster.
- **List Trino Schemas**  
  `trino_schemas` — Lists all schemas in a specified Trino catalog.
- **Get Iceberg Table Schema**  
  `get_iceberg_table_schema` — Returns the schema (columns/types) of an Iceberg table.
- **Execute Trino Query**  
  `execute_trino_query` — Executes a SQL query on Trino and returns results.
- **Iceberg Time Travel Query**  
  `iceberg_time_travel_query` — Executes a time travel query on Iceberg tables using Trino.  
  _Timestamp format: ISO 8601 (e.g., `2024-09-12T15:30:45.123456+05:30`)._
- **List Iceberg Snapshots**  
  `list_iceberg_snapshots` — Lists all snapshots for a given Iceberg table (snapshot_id, committed_at, operation, etc.).

## Example Prompts

You can use the following prompts in any MCP-enabled client:
- "List all Kafka topics."
- "Show Flink cluster metrics."
- "List Iceberg tables in Trino."
- "Run a query on Iceberg table."
- "Show Flink jobs."
- "Get schema for table ice_users."
- "Time travel query on Iceberg table."
- "List Trino catalogs."
- "Get details of Flink job 123."
- "List snapshots for iceberg table."
- "List all Flink TaskManagers and their details."

## MCP Tool Discovery

All tools are annotated with descriptions. MCP clients will auto-discover available tools and their parameters, making it easy to interact programmatically or via chat.

## Extending

Add new tools/resources by creating functions in the appropriate file and annotating with `@mcp.tool` or `@mcp.resource`.

## Testing & Troubleshooting

### MCP Inspector Tool

You can use the [MCP Inspector](https://www.npmjs.com/package/@modelcontextprotocol/inspector) to test and troubleshoot the MCP server and its tools. This is especially useful for verifying tool interfaces, inspecting tool annotations, and simulating LLM interactions.

#### Usage

Run the following command in your project directory:

```
npx @modelcontextprotocol/inspector uv run
```

This will start the MCP Inspector in stdio mode, allowing you to interactively test tool definitions and server responses. For more details, see the [MCP Inspector documentation](https://github.com/modelcontextprotocol/inspector).

## Troubleshooting

- Ensure all dependencies are installed.
- Check MCP client configuration for correct executable path.
- Review logs for errors (e.g., missing modules, connection issues).
