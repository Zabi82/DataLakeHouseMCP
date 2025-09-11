from env_config import TRINO_HOST, TRINO_PORT, TRINO_USER
import trino
from datetime import datetime
import re
from logging_config import logger

# Helper to get a Trino connection

def get_trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=None  # Catalog is set per query
    )

def list_trino_catalogs() -> dict:
    """
    Lists all catalogs available in the Trino cluster using the Trino Python client.
    """
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info(f"Trino catalogs fetched: {catalogs}")
        return {"catalogs": catalogs}
    except Exception as e:
        logger.error(f"Error fetching Trino catalogs: {e}")
        return {"error": str(e)}


def list_trino_schemas(catalog: str = "flink_demo") -> dict:
    """
    Lists all schemas in the specified Trino catalog using the Trino Python client.
    """
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info(f"Trino schemas for catalog '{catalog}' fetched: {schemas}")
        return {"schemas": schemas}
    except Exception as e:
        logger.error(f"Error fetching Trino schemas for catalog '{catalog}': {e}")
        return {"error": str(e)}


def list_iceberg_tables(catalog: str = "flink_demo", schema: str = "ice_db") -> dict:
    """
    Lists all tables in the specified Trino catalog and 'ice_db' schema using the Trino Python client.
    """
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES FROM {catalog}.{schema}")
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info(f"Trino tables for catalog '{catalog}', schema '{schema}' fetched: {tables}")
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error fetching Trino tables for catalog '{catalog}', schema '{schema}': {e}")
        return {"error": str(e)}


def get_iceberg_table_schema(catalog: str = "flink_demo", schema: str = "ice_db", table: str = None) -> dict:
    """
    Gets the schema of the specified Iceberg table using Trino's DESCRIBE statement.
    Returns a list of columns with their names and types.
    """
    if not table:
        logger.error("Table name must be provided for get_iceberg_table_schema.")
        return {"error": "Table name must be provided."}
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(f"DESCRIBE {catalog}.{schema}.{table}")
        columns = [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info(f"Trino schema for table '{table}' in catalog '{catalog}', schema '{schema}' fetched: {columns}")
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Error fetching Trino schema for table '{table}': {e}")
        return {"error": str(e)}


def _sanitize_identifier(identifier: str) -> str:
    """
    Allow only alphanumeric and underscores in identifiers (catalog, schema, table).
    """
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(f"Invalid identifier: {identifier}")
    return identifier


def execute_trino_query(query: str, catalog: str = "flink_demo", schema: str = "ice_db") -> dict:
    """
    Executes a SQL query using Trino and returns the result as a list of rows.
    Only allows SELECT queries. Validates catalog and schema names.
    If the query is not prefixed with catalog and schema, it rewrites the table references to include them.
    """
    if not query:
        logger.error("Query must be provided for execute_trino_query.")
        return {"error": "Query must be provided."}
    # Only allow SELECT queries
    if not query.strip().lower().startswith("select"):
        logger.error(f"Non-SELECT query attempted: {query}")
        return {"error": "Only SELECT queries are allowed."}
    forbidden = ["delete", "update", "drop", "insert", "alter", "truncate"]
    lowered = query.lower()
    for word in forbidden:
        if word in lowered:
            logger.error(f"Forbidden keyword '{word}' found in query: {query}")
            return {"error": f"Query contains forbidden keyword: {word}"}
    try:
        catalog = _sanitize_identifier(catalog)
        schema = _sanitize_identifier(schema)
        conn = get_trino_connection()
        cur = conn.cursor()
        # Check if query contains catalog.schema prefix
        prefix = f"{catalog}.{schema}."
        rewritten_query = query
        if prefix not in query:
            # Attempt to rewrite table references in FROM/JOIN clauses
            # This is a simple regex for common cases
            table_pattern = re.compile(r"(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE)
            def repl(match):
                return f"{match.group(1)} {catalog}.{schema}.{match.group(2)}"
            rewritten_query = table_pattern.sub(repl, query)
        cur.execute(rewritten_query)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info(f"Trino query executed: {rewritten_query}, rows returned: {len(rows)}")
        return {"columns": columns, "rows": rows, "rewritten_query": rewritten_query}
    except Exception as e:
        logger.error(f"Error executing Trino query: {e}")
        return {"error": str(e)}


def iceberg_time_travel_query(query: str, table: str, catalog: str = "flink_demo", schema: str = "ice_db", timestamp: str = None, snapshot_id: str = None) -> dict:
    """
    Executes an Iceberg time travel query using Trino.
    Rewrites the query to use FOR TIMESTAMP AS OF or FOR VERSION AS OF syntax.
    Converts ISO 8601 timestamps to Trino's TIMESTAMP literal with correct timezone format.
    Accepts snapshot_id as a string to avoid rounding issues from clients (e.g., JavaScript).
    Logs all key parameters and the final rewritten query for debugging.
    """
    import sys
    if not query or not table:
        print(f"[iceberg_time_travel_query] ERROR: Missing query or table", file=sys.stderr)
        return {"error": "Both query and table name must be provided."}
    try:
        print(f"[iceberg_time_travel_query] INPUTS: query={query}, table={table}, catalog={catalog}, schema={schema}, timestamp={timestamp}, snapshot_id={snapshot_id} (type={type(snapshot_id)})", file=sys.stderr)
        # Build the time travel clause
        time_travel_clause = ""
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp)
                formatted = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Truncate to milliseconds
                if dt.tzinfo:
                    offset = dt.strftime('%z')
                    if offset:
                        offset = offset[:3] + ':' + offset[3:]
                        formatted = f"{formatted} {offset}"
                time_travel_clause = f" FOR TIMESTAMP AS OF TIMESTAMP '{formatted}'"
                print(f"[iceberg_time_travel_query] time_travel_clause={time_travel_clause}", file=sys.stderr)
            except Exception as e:
                print(f"[iceberg_time_travel_query] ERROR: Invalid timestamp format: {timestamp}. Error: {str(e)}", file=sys.stderr)
                return {"error": f"Invalid timestamp format: {timestamp}. Error: {str(e)}"}
        elif snapshot_id:
            try:
                snapshot_id_int = int(snapshot_id)
                time_travel_clause = f" FOR VERSION AS OF {snapshot_id_int}"
                print(f"[iceberg_time_travel_query] time_travel_clause={time_travel_clause}", file=sys.stderr)
            except Exception as e:
                print(f"[iceberg_time_travel_query] ERROR: Invalid snapshot_id: {snapshot_id}. Error: {str(e)}", file=sys.stderr)
                return {"error": f"Invalid snapshot_id: {snapshot_id}. Error: {str(e)}"}
        prefix = f"{catalog}.{schema}.{table}"
        table_pattern = re.compile(rf"(FROM|JOIN)\s+{table}(?![\w.])", re.IGNORECASE)
        def repl(match):
            return f"{match.group(1)} {prefix}{time_travel_clause}"
        rewritten_query = table_pattern.sub(repl, query)
        print(f"[iceberg_time_travel_query] rewritten_query={rewritten_query}", file=sys.stderr)
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(rewritten_query)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"[iceberg_time_travel_query] result columns={columns}, rows_count={len(rows)}", file=sys.stderr)
        return {"columns": columns, "rows": rows, "rewritten_query": rewritten_query}
    except Exception as e:
        print(f"[iceberg_time_travel_query] ERROR: {str(e)}", file=sys.stderr)
        return {"error": str(e)}


def list_iceberg_snapshots(catalog: str = "flink_demo", schema: str = "ice_db", table: str = None) -> dict:
    """
    Lists all snapshots for a given Iceberg table using Trino's $snapshots metadata table.
    Returns snapshot_id, committed_at, operation, etc. Sorted by committed_at descending.
    Logs all key parameters and the query for debugging.
    """
    import sys
    if not table:
        print(f"[list_iceberg_snapshots] ERROR: Missing table", file=sys.stderr)
        return {"error": "Table name must be provided."}
    try:
        print(f"[list_iceberg_snapshots] INPUTS: catalog={catalog}, schema={schema}, table={table}", file=sys.stderr)
        query = f'SELECT * FROM {catalog}.{schema}."{table}$snapshots"'
        print(f"[list_iceberg_snapshots] query={query}", file=sys.stderr)
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute(query)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"[list_iceberg_snapshots] result columns={columns}, rows_count={len(rows)}", file=sys.stderr)
        if 'committed_at' in columns:
            rows.sort(key=lambda x: x.get('committed_at', ''), reverse=True)
        for i, row in enumerate(rows):
            if i == 0:
                row['label'] = 'latest'
            elif i == len(rows) - 1:
                row['label'] = 'oldest'
        return {"columns": columns, "rows": rows}
    except Exception as e:
        print(f"[list_iceberg_snapshots] ERROR: {str(e)}", file=sys.stderr)
        return {"error": str(e)}
