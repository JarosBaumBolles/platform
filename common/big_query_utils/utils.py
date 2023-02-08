"""Simple utils needed to BIgQery batch data load"""

import queue
import time
import uuid
from typing import Any, Dict, Iterable, List, Tuple, Union

from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.cloud.bigquery import Client, LoadJobConfig, SchemaField, SourceFormat
from google.cloud.bigquery.table import Table, TableListItem, TableReference
from googleapiclient.errors import HttpError

from common import settings as CFG
from common.logging import Logger
from common.thread_pool_executor import run_thread_pool_executor

LOGGER = Logger(
    name="BIGQUERY",
    level="DEBUG",
    description="BIGQUERY",
    trace_id=uuid.uuid4(),
)


def get_bq_client(project: str = CFG.PROJECT):
    return Client(project=project)


def _put_queue_by_chunks(it_queu, iterable, chunk_size=50000):
    chunk_size = max(1, chunk_size)
    for idx in range(0, len(iterable), chunk_size):
        it_queu.put(iterable[idx : idx + chunk_size])


def _insert_json_to_bigquery_worker(
    connection: Client,
    worker_queue: queue.Queue,
    full_table_id: Union[Table, TableReference, TableListItem, str],
    job_config: LoadJobConfig,
    exceptions: Union[List, Tuple],
    max_retries: int = 3,
    delay: int = 1,
    backoff: int = 2,
    work_idx: int = -1,
) -> None:
    empty_run_count = 0
    count_run = 0
    while True:
        if worker_queue.empty():
            if empty_run_count == 3:
                break
            empty_run_count += 1
        else:
            count_run += 1
            json_rows = worker_queue.get()
            delay_time = delay
            for _ in range(max_retries):
                try:
                    connection.load_table_from_json(
                        json_rows, full_table_id, job_config=job_config
                    )
                    break
                except exceptions as err:
                    LOGGER.error(
                        f"ERROR: BIG_QUERY JSON INSERT WORKER {work_idx}. "
                        f" Failed insetion due to the error '{err}'"
                        f"Retrying in {delay_time} seconds..."
                    )
                    time.sleep(delay_time)
                    delay_time *= backoff
                else:
                    worker_queue.task_done()
            else:
                connection.load_table_from_json(
                    json_rows, full_table_id, job_config=job_config
                )
                worker_queue.task_done()
            time.sleep(1)


def insert_json_data(
    connection: Client,
    json_rows: Iterable[Dict[str, Any]],
    full_table_id: Union[Table, TableReference, TableListItem, str],
    schema: List[SchemaField],
    max_worker_replica: int = 3,
) -> dict[Any, Any]:
    """Parallel load json data in BigQuery"""

    # In general, parallel load to BigQuery are very tricky question due to a billion
    # restrictions
    # For more information see  https://cloud.google.com/bigquery/quotas#tablesinsert_requests
    #
    # So code below is written to provide some way which allow to use some paralellism
    # which allow speed up insetions
    # We are using list of dictionaries as data source and splitting those into chunks
    # to load in parallel in thread_pool_executor
    #
    # At the moment logic is based on the following assumptions and contains simple
    # error check logic:
    #   1. Use JSON Load data-
    #   2. Input data splits in big chunks in accordance with limitations
    #   3. Calculate workers amount based on chunks amount
    #   4. Each worker takes some pause beteen loop iteration
    #   5. After run we return set of futures for errors check
    #   6. Woeker contains simple retry logic

    data_queue = queue.Queue()

    _put_queue_by_chunks(
        it_queu=data_queue,
        iterable=json_rows,
    )

    job_config = LoadJobConfig()
    job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = schema

    q_size = data_queue.qsize()

    futures = run_thread_pool_executor(
        workers=[
            (
                _insert_json_to_bigquery_worker,
                [
                    connection,  # conncetion
                    data_queue,  # worker_queue
                    full_table_id,  # full_table_id:
                    job_config,  # job_config
                    (BadRequest, Forbidden, NotFound, HttpError),  # exceptions
                    3,  # max_retries
                    1,  # delay
                    2,  # backoff
                ],
            )
        ],
        worker_replica=q_size if q_size <= max_worker_replica else max_worker_replica,
    )

    return futures
