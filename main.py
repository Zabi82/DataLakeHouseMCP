from mcp.server.fastmcp import FastMCP
from kafka_tools import get_kafka_topics, peek_kafka_topic
from flink_tools import (
    get_flink_overview,
    get_flink_jobmanager_metrics,
    get_flink_taskmanagers_metrics,
    get_flink_jobs,
    get_flink_job_details,
    probe_jobmanager_metric,
    probe_taskmanager_metric,
    probe_jobmanager_metrics,
    probe_taskmanager_metrics,
    get_flink_taskmanagers
)
from trino_tools import (
    list_iceberg_tables,
    list_trino_catalogs,
    list_trino_schemas,
    get_iceberg_table_schema,
    execute_trino_query,
    iceberg_time_travel_query,
    list_iceberg_snapshots
)
from logging_config import logger

mcp = FastMCP(name="KafkaFlinkResourceServer")

@mcp.tool("kafka_topics", description="List all Kafka topics available in the local cluster.")
def mcp_kafka_topics() -> dict:
    """
    Returns a dictionary containing the list of Kafka topics from the local cluster.
    """
    try:
        result = get_kafka_topics()
        logger.info(f"Fetched Kafka topics: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching Kafka topics: {e}")
        return {"error": str(e)}

@mcp.tool("peek_kafka_topic", description="Get the latest N messages from a specified Kafka topic. Handles Avro, JSON, and plain text.")
def mcp_peek_kafka_topic(topic: str, max_messages: int = 10) -> dict:
    """
    Returns the latest N messages from the given Kafka topic, decoding Avro, JSON, or plain text as needed.
    """
    try:
        result = peek_kafka_topic(topic, max_messages)
        logger.info(f"Peeked Kafka topic '{topic}': {result}")
        return result
    except Exception as e:
        logger.error(f"Error peeking Kafka topic '{topic}': {e}")
        return {"error": str(e)}

@mcp.tool("flink_overview", description="Get Flink cluster overview metrics such as number of task managers, slots available, jobs running, jobs finished, jobs cancelled, and jobs failed from the REST API.")
def mcp_flink_overview() -> dict:
    """
    Returns Flink cluster overview metrics from the REST API, including:
    - Number of task managers
    - Slots available
    - Jobs running
    - Jobs finished
    - Jobs cancelled
    - Jobs failed
    """
    return get_flink_overview()

@mcp.tool("flink_jobmanager_metrics", description="Get Flink JobManager metrics such as heap memory usage, CPU load, and other JVM/process stats from the REST API.")
def mcp_flink_jobmanager_metrics() -> dict:
    """
    Returns JobManager metrics from the Flink REST API, including:
    - Heap memory usage
    - CPU load
    - JVM stats
    - Process stats
    """
    return get_flink_jobmanager_metrics()

@mcp.tool("flink_taskmanagers_metrics", description="Get Flink TaskManagers metrics such as heap memory usage, network IO, and task slot utilization from the REST API.")
def mcp_flink_taskmanagers_metrics() -> dict:
    """
    Returns TaskManagers metrics from the Flink REST API, including:
    - Heap memory usage
    - Network IO
    - Task slot utilization
    """
    return get_flink_taskmanagers_metrics()

@mcp.tool("flink_jobs", description="List all Flink jobs running on the cluster, including job IDs, names, and status.")
def mcp_flink_jobs() -> dict:
    """
    Returns a list of all Flink jobs running on the cluster, including job IDs, names, and status.
    """
    return get_flink_jobs()

@mcp.tool("flink_job_details", description="Get details for a list of Flink jobs by job ID(s), including status, vertices, and configuration. Accepts a 'job_ids' list parameter.")
def mcp_flink_job_details(job_ids: list) -> dict:
    """
    Returns details for a list of Flink jobs, including status, vertices, and configuration.
    :param job_ids: List of Job IDs to fetch details for. Must be a list.
    """
    if not isinstance(job_ids, list):
        return {"error": "job_ids must be a list of job IDs. Example: job_ids=['job1','job2']"}
    return get_flink_job_details(job_ids)

@mcp.tool("probe_jobmanager_metric", description="Probe a specific JobManager metric by name from the Flink REST API.")
def mcp_probe_jobmanager_metric(metric_name: str) -> dict:
    """
    Probe a specific JobManager metric by name from the Flink REST API.
    """
    return probe_jobmanager_metric(metric_name)

@mcp.tool("probe_taskmanager_metric", description="Probe a specific TaskManager metric by name from the Flink REST API. Requires TaskManager ID and metric name.")
def mcp_probe_taskmanager_metric(taskmanager_id: str, metric_name: str) -> dict:
    """
    Probe a specific TaskManager metric by name from the Flink REST API.
    :param taskmanager_id: The ID of the TaskManager to probe.
    :param metric_name: The metric name to fetch.
    """
    return probe_taskmanager_metric(taskmanager_id, metric_name)

@mcp.tool("probe_jobmanager_metrics", description="Probe a list of JobManager metrics by name from the Flink REST API. Accepts a list of metric names.")
def mcp_probe_jobmanager_metrics(metric_names: list) -> dict:
    """
    Probe a list of JobManager metrics by name from the Flink REST API.
    :param metric_names: List of metric names to fetch.
    """
    return probe_jobmanager_metrics(metric_names)

@mcp.tool("probe_taskmanager_metrics", description="Probe a list of TaskManager metrics by name from the Flink REST API. Accepts TaskManager ID and a list of metric names.")
def mcp_probe_taskmanager_metrics(taskmanager_id: str, metric_names: list) -> dict:
    """
    Probe a list of TaskManager metrics by name from the Flink REST API.
    :param taskmanager_id: The ID of the TaskManager to probe.
    :param metric_names: List of metric names to fetch.
    """
    return probe_taskmanager_metrics(taskmanager_id, metric_names)

@mcp.tool("trino_iceberg_tables", description="List all Iceberg tables in the specified Trino catalog using Trino REST API.")
def mcp_trino_iceberg_tables(catalog: str = "flink_demo") -> dict:
    """
    Lists all Iceberg tables in the given Trino catalog.
    Default catalog is 'flink_demo'.
    """
    return list_iceberg_tables(catalog)

@mcp.tool("trino_catalogs", description="List all catalogs available in the Trino cluster using the REST API.")
def mcp_trino_catalogs() -> dict:
    """
    Lists all catalogs available in the Trino cluster.
    """
    return list_trino_catalogs()

@mcp.tool("trino_schemas", description="List all schemas in the specified Trino catalog using the REST API.")
def mcp_trino_schemas(catalog: str = "flink_demo") -> dict:
    """
    Lists all schemas in the given Trino catalog. Default catalog is 'flink_demo'.
    """
    return list_trino_schemas(catalog)

@mcp.tool("get_iceberg_table_schema", description="Get the schema (columns and types) of an Iceberg table using Trino.")
def mcp_get_iceberg_table_schema(catalog: str = "flink_demo", schema: str = "ice_db", table: str = None) -> dict:
    """
    Returns the schema (columns and types) of the specified Iceberg table using Trino.
    """
    return get_iceberg_table_schema(catalog, schema, table)

@mcp.tool("execute_trino_query", description="Execute a SQL query on Trino and return the result as rows and columns.")
def mcp_execute_trino_query(query: str, catalog: str = "flink_demo", schema: str = "ice_db") -> dict:
    """
    Executes a SQL query on Trino and returns the result as rows and columns.
    """
    return execute_trino_query(query, catalog, schema)

@mcp.tool("iceberg_time_travel_query", description="Execute an Iceberg time travel query using Trino. Specify table, timestamp (ISO 8601 format, e.g. '2024-09-12T15:30:45.123456+05:30') or snapshot_id, and a query. The tool rewrites the query to use FOR TIMESTAMP AS OF or FOR SNAPSHOT AS OF syntax.")
def mcp_iceberg_time_travel_query(query: str, table: str, catalog: str = "flink_demo", schema: str = "ice_db", timestamp: str = None, snapshot_id: int = None) -> dict:
    """
    Executes an Iceberg time travel query using Trino.
    Specify table, timestamp (ISO 8601 format, e.g. '2024-09-12T15:30:45.123456+05:30') or snapshot_id, and a query.
    The tool rewrites the query to use FOR TIMESTAMP AS OF or FOR SNAPSHOT AS OF syntax.
    Example timestamp: '2024-09-12T15:30:45.123456+05:30'
    """
    return iceberg_time_travel_query(query, table, catalog, schema, timestamp, snapshot_id)

@mcp.tool("list_iceberg_snapshots", description="List all snapshots for a given Iceberg table using Trino's $snapshots metadata table. Returns snapshot_id, committed_at, operation, etc.")
def mcp_list_iceberg_snapshots(catalog: str = "flink_demo", schema: str = "ice_db", table: str = None) -> dict:
    """
    Lists all snapshots for a given Iceberg table using Trino's $snapshots metadata table. Returns snapshot_id, committed_at, operation, etc.
    """
    return list_iceberg_snapshots(catalog, schema, table)

@mcp.tool("flink_taskmanagers", description="List all Flink TaskManagers and their details from the REST API.")
def mcp_flink_taskmanagers() -> dict:
    """
    Returns the list of Flink TaskManagers and their details from the REST API.
    """
    return get_flink_taskmanagers()

if __name__ == "__main__":
    mcp.run()
