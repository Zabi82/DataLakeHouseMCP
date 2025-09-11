from env_config import FLINK_REST_URL
import requests
from logging_config import logger

def get_flink_overview() -> dict:
    """
    Returns Flink cluster overview metrics from the REST API.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/overview")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Flink overview fetched: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching Flink overview: {e}")
        return {"error": str(e)}

def get_flink_jobmanager_metrics() -> dict:
    """
    Returns JobManager metrics from the Flink REST API.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/jobmanager/metrics")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Flink JobManager metrics fetched: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching Flink JobManager metrics: {e}")
        return {"error": str(e)}

def get_flink_taskmanagers_metrics() -> dict:
    """
    Returns TaskManagers metrics from the Flink REST API.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/taskmanagers/metrics")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Flink TaskManagers metrics fetched: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching Flink TaskManagers metrics: {e}")
        return {"error": str(e)}

def get_flink_jobs() -> dict:
    """
    Returns the list of jobs running on the Flink cluster from the REST API.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/jobs")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Flink jobs fetched: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching Flink jobs: {e}")
        return {"error": str(e)}


def _sanitize_job_id(job_id: str) -> str:
    """
    Allow only alphanumeric, dashes, and underscores in job_id.
    """
    import re
    if not re.match(r'^[a-zA-Z0-9_-]+$', job_id):
        raise ValueError(f"Invalid job_id: {job_id}")
    return job_id

def get_flink_job_details(job_ids: list) -> dict:
    """
    Returns details for a list of Flink jobs from the REST API.
    :param job_ids: List of Job IDs to fetch details for.
    """
    if not isinstance(job_ids, list):
        raise TypeError("job_ids must be a list of job IDs.")
    results = {}
    for job_id in job_ids:
        try:
            sanitized_id = _sanitize_job_id(job_id)
        except Exception as e:
            logger.error(f"Invalid job_id '{job_id}': {e}")
            results[job_id] = {"error": str(e)}
            continue
        try:
            resp = requests.get(f"{FLINK_REST_URL}/jobs/{sanitized_id}")
            resp.raise_for_status()
            result = resp.json()
            logger.info(f"Flink job details for '{sanitized_id}': {result}")
            results[job_id] = result
        except Exception as e:
            logger.error(f"Error fetching Flink job details for '{sanitized_id}': {e}")
            results[job_id] = {"error": str(e)}
    return results

def probe_jobmanager_metric(metric_name: str) -> dict:
    """
    Probe a specific JobManager metric by name from the Flink REST API.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/jobmanager/metrics?get={metric_name}")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Probed JobManager metric '{metric_name}': {result}")
        return result
    except Exception as e:
        logger.error(f"Error probing JobManager metric '{metric_name}': {e}")
        return {"error": str(e)}


def probe_taskmanager_metric(taskmanager_id: str, metric_name: str) -> dict:
    """
    Probe a specific TaskManager metric by name from the Flink REST API.
    :param taskmanager_id: The ID of the TaskManager to probe.
    :param metric_name: The metric name to fetch.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/taskmanagers/{taskmanager_id}/metrics?get={metric_name}")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Probed TaskManager '{taskmanager_id}' metric '{metric_name}': {result}")
        return result
    except Exception as e:
        logger.error(f"Error probing TaskManager '{taskmanager_id}' metric '{metric_name}': {e}")
        return {"error": str(e)}

def probe_jobmanager_metrics(metric_names: list) -> dict:
    """
    Probe a list of JobManager metrics by name from the Flink REST API.
    :param metric_names: List of metric names to fetch.
    """
    try:
        metrics_query = "&".join([f"get={name}" for name in metric_names])
        resp = requests.get(f"{FLINK_REST_URL}/jobmanager/metrics?{metrics_query}")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Probed JobManager metrics '{metric_names}': {result}")
        return {"metrics": result}
    except Exception as e:
        logger.error(f"Error probing JobManager metrics '{metric_names}': {e}")
        return {"error": str(e)}


def probe_taskmanager_metrics(taskmanager_id: str, metric_names: list) -> dict:
    """
    Probe a list of TaskManager metrics by name from the Flink REST API.
    :param taskmanager_id: The ID of the TaskManager to probe.
    :param metric_names: List of metric names to fetch.
    """
    try:
        metrics_query = "&".join([f"get={name}" for name in metric_names])
        resp = requests.get(f"{FLINK_REST_URL}/taskmanagers/{taskmanager_id}/metrics?{metrics_query}")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Probed TaskManager '{taskmanager_id}' metrics '{metric_names}': {result}")
        return {"metrics": result}
    except Exception as e:
        logger.error(f"Error probing TaskManager '{taskmanager_id}' metrics '{metric_names}': {e}")
        return {"error": str(e)}

def get_flink_taskmanagers() -> dict:
    """
    Returns the list of Flink TaskManagers and their details from the REST API.
    """
    try:
        resp = requests.get(f"{FLINK_REST_URL}/taskmanagers")
        resp.raise_for_status()
        result = resp.json()
        logger.info(f"Flink TaskManagers fetched: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching Flink TaskManagers: {e}")
        return {"error": str(e)}
