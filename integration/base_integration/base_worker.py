""" WatTime Workers module"""
import re
import time
import uuid
from abc import abstractmethod
from collections import Counter
from dataclasses import dataclass
from json import JSONDecodeError, dumps, loads
from pathlib import Path
from queue import Queue
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from expiringdict import ExpiringDict
from google.cloud.exceptions import GoogleCloudError
from google.cloud.storage import Client
from googleapiclient.errors import HttpError
from pendulum import DateTime

import common.settings as CFG
from common.bucket_helpers import file_exists, get_file_contents, upload_file_to_bucket
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, truncate
from common.logging import Logger, ThreadPoolExecutorLogger
from common.request_helpers import retry
from common.thread_pool_executor import run_thread_pool_executor
from integration.base_integration.exceptions import EmptyRawFile
from integration.wattime.data import DataFile


@dataclass
class UpdateConfig:
    """Config used to configure Fetch Or Standardize update status"""

    bucket: str = ""
    path_preffix: str = ""
    file_name_preffix: str = ""


class BaseWorker:  # pylint: disable=too-many-instance-attributes
    """Base Worker functionality"""

    __description__ = "Base Worker"
    __name__ = "Base Worker"

    __max_retry_count__ = 5
    __fetch_file_name_tmpl__ = "{base_file_name}_{idx}"

    __update_chunk_main_part_regex__ = re.compile(r"^storage_([\w\-\/]+){1}[_]{1}\d+$")

    __workers_amount__ = 10

    def __init__(
        self,
        config: Any,
        update_filename_tmpl: str = CFG.UPDATE_FILENAME_TMPL,
        update_preffix: str = CFG.UPDATE_PREFIX,
        max_chunk_size: int = CFG.DW_LOAD_FILES_BUCKET_LIMIT,
    ) -> None:
        self._config: Any = config
        self._trace_id: str = uuid.uuid4()
        self._run_time: Optional[DateTime] = None
        self._lock: Lock = Lock()
        self._fetch_lock: Lock = Lock()
        self._update_filename_tmpl = update_filename_tmpl
        self._update_preffix = update_preffix
        self._max_chunk_size = max_chunk_size
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)
        self._th_logger = ThreadPoolExecutorLogger(
            description=self.__description__, trace_id=self._trace_id
        )

    def process_consumer_results(self, futures, logs) -> None:
        """Process worker statuses and collected logs"""
        for res in futures:
            try:
                res.result()
            except Exception as err:  # pylint: disable=broad-except
                self._logger.error(
                    f"ERROR: Received error '{err}' during downloading blobs"
                )

        while not logs.empty():
            level, _, message = logs.get()
            self._logger.log(level, message)
            logs.task_done()

        self._th_logger.flush_logs()

    def _run_consumers(
        self, consumers: List[Tuple[Callable, List[Any]]], run_parallel: bool = True
    ) -> None:
        if not isinstance(consumers, list):
            consumers = [consumers]
        futures = []
        for consumer, arguments in consumers:
            logs = Queue()
            args = arguments + [logs]
            if run_parallel:
                futures = run_thread_pool_executor(
                    workers=[
                        (
                            consumer,
                            args,
                        )
                    ],
                    worker_replica=self.__workers_amount__,
                )
            else:
                args.append(f"worker_idx_{uuid.uuid4()}")
                consumer(*args)
            self.process_consumer_results(futures, logs)

    @staticmethod
    @retry((HttpError,))
    def _is_file_exists(
        bucket: str, path: str, file_name: str, client: Optional[Client] = None
    ) -> bool:
        return file_exists(
            bucket=bucket, subdirectory=path, file_name=file_name, client=client
        )

    @retry((HttpError,))
    def _retry_load_json_data(  # pylint:disable=too-many-arguments
        self, bucket: str, path: str, filename: str, client: Client, logs: Queue
    ) -> Dict:
        return self._load_json_data(
            bucket=bucket, path=path, filename=filename, client=client, logs=logs
        )

    def _load_json_data(  # pylint:disable=too-many-arguments
        self,
        bucket: str,
        path: str,
        filename: str,
        client: Client,
        logs: Queue,
    ) -> Union[bytes, str]:
        try:
            data = loads(
                self._load_data(
                    bucket=bucket,
                    path=path,
                    filename=filename,
                    client=client,
                    logs=logs,
                )
            )
        except JSONDecodeError as err:
            data = {}
            logs.put(
                (
                    "ERROR",
                    self._trace_id,
                    f"Cannot encode raw file to JSON due to the error {err}.",
                )
            )
        return data

    @retry((HttpError,))
    def _retry_load_data(  # pylint:disable=too-many-arguments
        self, bucket: str, path: str, filename: str, client: Client, logs: Queue
    ) -> Dict:
        return self._load_data(
            bucket=bucket, path=path, filename=filename, client=client, logs=logs
        )

    # TODO @todo Refactor to avoid unused-argument
    def _load_data(  # pylint:disable=too-many-arguments
        self,
        bucket: str,
        path: str,
        filename: str,
        client: Client,
        logs: Queue,  # pylint:disable=unused-argument
    ) -> Optional[str]:
        path = f'{path.lstrip("/")}'
        fl_path = f"{path}/{filename}"

        return get_file_contents(bucket_name=bucket, blob_path=fl_path, client=client)

    def configure(self, run_time: DateTime) -> None:
        """Configure worker before run"""
        self._run_time = run_time

    @staticmethod
    def _clear_queue(task_queue: Queue):
        with task_queue.mutex:
            task_queue.queue.clear()
            task_queue.all_tasks_done.notify_all()
            task_queue.unfinished_tasks = 0

    @abstractmethod
    def _add_to_update(self, file: Dict, chunk_storage: Dict) -> None:
        pass

    def _save_files_worker(
        self,
        files_queue: Queue,
        logs: Queue,
        worker_idx: str,
    ) -> None:
        if files_queue.empty():
            logs.put(
                (
                    "WARNING",
                    self._trace_id,
                    f"[Saving data] - [{worker_idx}]. The files queue is empty. "
                    "Shutdown worker.",
                )
            )
            return None
        empty_run_count = 0
        while True:
            if files_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                file_info = files_queue.get()
                retry_count = 0
                delay = 0.5
                while retry_count < self.__max_retry_count__:
                    filepath = (
                        f"{file_info.bucket}/{file_info.path}/"
                        f"{file_info.file_name}/"
                    )
                    try:
                        upload_file_to_bucket(
                            bucket_name=file_info.bucket,
                            blob_path=file_info.path,
                            file_name=file_info.file_name,
                            blob_text=file_info.body,
                        )
                        logs.put(
                            (
                                "INFO",
                                self._trace_id,
                                f"[{worker_idx}] - Saved file '{filepath}'.",
                            )
                        )
                        break
                    except GoogleCloudError as err:
                        retry_count += 1
                        delay = retry_count
                        if retry_count < self.__max_retry_count__:
                            logs.put(
                                (
                                    "WARNING",
                                    self._trace_id,
                                    f"Cannot upload file '{filepath}' due "
                                    f"to the {err}. Try to save in a "
                                    "few seconds.",
                                )
                            )
                        else:
                            logs.put(
                                (
                                    "ERROR",
                                    self._trace_id,
                                    f"Cannot upload file '{filepath}' due "
                                    f"to the {err}.",
                                )
                            )
                        time.sleep(delay)
                files_queue.task_done()

    def _adjust_meter_date(
        self, date: DateTime, truncate_lvl: Optional[str] = None
    ) -> DateTime:
        """Adjust meter time in accordance with the given configuration"""
        mtr_dt = truncate(date, level=truncate_lvl) if truncate_lvl else date
        time_shift = self._config.timestamp_shift.get("shift", None).strip().lower()
        kwargs = self._config.timestamp_shift.get("shift_hours", {})

        if kwargs and any(kwargs.values()):
            if time_shift == "add":
                return mtr_dt.add(**kwargs)
            if time_shift.strip().lower() == "subtruct":
                return mtr_dt.subtract(**kwargs)
        return mtr_dt


class BaseFetchWorker(BaseWorker):
    """Base Fetch Worker functionality"""

    def __init__(
        self,
        missed_hours: Queue,
        fetched_files: Queue,
        fetch_update: Queue,
        config: Any,
    ) -> None:
        super().__init__(config=config)
        self._missed_hours_queue: Queue = missed_hours
        self._fetched_files_queue: Queue = fetched_files
        self._shadow_fetched_files_queue: Queue = Queue()
        self._fetch_update_queue: Queue = fetch_update
        self._fetch_update_counter: Counter = Counter()
        self._fetch_update_file_buffer: ExpiringDict = ExpiringDict(
            max_len=2000, max_age_seconds=3600
        )
        self._update_config: Optional[UpdateConfig] = None

    def save_fetched_files_worker(self, logs: Queue, worker_idx: str) -> None:
        """Worker used to upload fetched files to cloud storage"""
        self._save_files_worker(
            files_queue=self._shadow_fetched_files_queue,
            logs=logs,
            worker_idx=worker_idx,
        )

    def save_fetch_status_worker(self, logs: Queue, worker_idx: str) -> None:
        """Worker used to upload fetch status to storage"""
        self._save_files_worker(
            files_queue=self._fetch_update_queue,
            logs=logs,
            worker_idx=worker_idx,
        )

    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time=run_time)

        update_base_filename = format_date(
            truncate(self._run_time, level="hour"), CFG.PROCESSING_DATE_FORMAT
        )

        self._update_config = UpdateConfig(
            bucket=self._config.extra.raw.bucket,
            path_preffix=self._config.extra.raw.path,
            file_name_preffix=update_base_filename,
        )

        self._clear_queue(self._fetched_files_queue)
        self._clear_queue(self._fetch_update_queue)
        self._clear_queue(self._shadow_fetched_files_queue)
        self._fetch_update_file_buffer.clear()
        self._fetch_update_counter = Counter()

    def _add_to_update(self, file: Dict, chunk_storage: Queue) -> None:
        self._add_to_updates_generic(
            file=file,
            update_config=self._update_config,
            update_queue=self._fetch_update_queue,
            update_counter=self._fetch_update_counter,
            chunk_storage=chunk_storage,
        )

    def _finalize_update_status_generic(
        self,
        # update_config: UpdateConfig,
        chunk_id_key: str,
        chunk_storage: Queue,
    ) -> None:
        chunk_reg = self.__update_chunk_main_part_regex__.match(chunk_id_key)

        if chunk_reg is not None:
            chunk_key = f"chunk_{chunk_reg.group(1)}"
            chunk_id = self._fetch_update_counter[chunk_key]
            update_path = Path(self._update_config.path_preffix).joinpath(
                self._update_preffix
            )
            self._fetch_update_queue.put(
                DataFile(
                    file_name=self._update_filename_tmpl.format(
                        update_prefix=self._update_preffix,
                        cnt=chunk_id,
                        run_date=self._update_config.file_name_preffix,
                    ),
                    bucket=self._update_config.bucket,
                    path=str(update_path),
                    body=dumps(
                        {"files": list(chunk_storage.queue)},
                        indent=4,
                        sort_keys=True,
                    ),
                    cfg={},
                )
            )

    # TODO: Should be refactored to avoid unused parameters
    def _add_to_updates_generic(  # pylint:disable=too-many-arguments
        self,
        file: DataFile,
        update_config: UpdateConfig,  # pylint:disable=unused-argument
        update_queue: Queue,  # pylint:disable=unused-argument
        update_counter: Counter,
        chunk_storage: Dict[str, Queue],
    ) -> None:
        with self._lock:
            chunk_key = f"chunk_{file.bucket}_{file.path}"
            chunk_id = update_counter[chunk_key]
            chunk_idx_key = f"storage_{file.bucket}_{file.path}_{chunk_id}"

            chunk_storage.setdefault(chunk_idx_key, Queue())
            chunk_storage[chunk_idx_key].put(
                {
                    "bucket": file.bucket,
                    "path": file.path,
                    "filename": file.file_name,
                }
            )

            update_counter[chunk_idx_key] += 1
            if update_counter[chunk_idx_key] >= self._max_chunk_size:
                self._finalize_update_status_generic(
                    chunk_id_key=chunk_idx_key,
                    chunk_storage=chunk_storage[chunk_idx_key],
                )
                update_counter.pop(chunk_idx_key)
                update_counter[chunk_key] += 1
            elif chunk_key not in update_counter:
                update_counter[chunk_key] = chunk_id

    def finalize_fetch_update_status(self) -> None:
        """Save all not full chunk into update status"""
        with self._lock:
            while len(self._fetch_update_file_buffer):
                chunk_id_key, chunk_storage = self._fetch_update_file_buffer.popitem()
                self._finalize_update_status_generic(
                    chunk_id_key=chunk_id_key,
                    chunk_storage=chunk_storage,
                )


class BaseStandardizeWorker(BaseWorker):
    """Base Standardize Worker"""

    __created_by__ = "Base Worker"

    def __init__(
        self,
        raw_files: Queue,
        standardized_files: Queue,
        standardize_update: Queue,
        config: Any,
    ) -> None:

        super().__init__(config=config)
        self._raw_files_queue: Queue = raw_files
        self._st_files_queue: Queue = standardized_files
        self._st_update_queue: Queue = standardize_update
        self._st_update_file_buffer: ExpiringDict = ExpiringDict(
            max_len=2000, max_age_seconds=3600
        )
        self._st_update_counter = Counter()
        self._st_base_update_file_name: Optional[str] = None
        self._fetch_update_file_buffer: Optional[ExpiringDict] = None

    def run_standardize_worker(
        self,
        logs: Queue,
        worker_idx: str,
    ) -> None:
        """Run Standardize worker"""
        if self._raw_files_queue.empty():
            logs.put(
                (
                    "WARNING",
                    self._trace_id,
                    f"[Standardizing data] - [{worker_idx}] - The raw files "
                    "queue is empty. Shutdown worker.",
                )
            )
            return None
        empty_run_count = 0
        while True:
            if self._raw_files_queue.empty():
                if empty_run_count == 3:
                    logs.put(
                        (
                            "INFO",
                            self._trace_id,
                            f"[Standardizing data] - [{worker_idx}] - The raw files "
                            "queue is empty. Shutdown worker.",
                        )
                    )
                    break
                empty_run_count += 1
            else:
                raw_file_data = self._raw_files_queue.get()
                try:
                    standardized_obj = self._standardize(raw_file_obj=raw_file_data)
                except (RuntimeError, EmptyRawFile) as err:
                    logs.put(
                        (
                            "ERROR",
                            self._trace_id,
                            f"Skip standardizing due to the error '{err}'",
                        )
                    )
                else:
                    if not isinstance(standardized_obj, list):
                        standardized_obj = [standardized_obj]
                    # TODO: Change code to use an iterable object
                    for st_obj in standardized_obj:
                        self._st_files_queue.put(st_obj)
                        # self._add_to_update(standardized_obj, self._st_update_queue)
                        self._add_to_update(st_obj)

                self._raw_files_queue.task_done()

    def save_standardized_files_worker(self, logs: Queue, worker_idx: str) -> None:
        """Upload standardized data on cloud storage"""
        self._save_files_worker(
            files_queue=self._st_files_queue,
            logs=logs,
            worker_idx=worker_idx,
        )

    def save_standardize_status_worker(self, logs: Queue, worker_idx: str) -> None:
        """Upload standardizion status on cloud storage"""
        self._save_files_worker(
            files_queue=self._st_update_queue,
            logs=logs,
            worker_idx=worker_idx,
        )

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time=run_time)
        self._clear_queue(self._st_files_queue)
        self._clear_queue(self._st_update_queue)
        self._st_update_file_buffer.clear()
        self._st_update_counter = Counter()

        self._st_base_update_file_name = format_date(
            truncate(self._run_time, level="hour"), CFG.PROCESSING_DATE_FORMAT
        )

    def _standardize_generic(self, data: Dict, mtr_cfg: Any, getter: Callable) -> Meter:

        meter = Meter()

        usage, start_date, end_date = getter(data)

        meter.created_date = self._run_time
        meter.start_time = start_date
        meter.end_time = end_date
        meter.created_by = self.__created_by__
        meter.usage = usage
        meter.meter_uri = mtr_cfg.meter_uri

        return meter

    # TODO: @todo Shoud be refactored to avoid arguments-differ
    def _add_to_update(  # pylint:disable=arguments-differ
        self,
        file: DataFile,
    ) -> None:
        with self._lock:
            chunk_key = f"chunk_{file.bucket}_{file.path}"
            chunk_id = self._st_update_counter[chunk_key]
            chunk_idx_key = f"storage_{file.bucket}_{file.path}_{chunk_id}"

            self._st_update_file_buffer.setdefault(chunk_idx_key, Queue())
            self._st_update_file_buffer[chunk_idx_key].put(file)

            self._st_update_counter[chunk_idx_key] += 1
            if self._st_update_counter[chunk_idx_key] >= self._max_chunk_size:
                self._finalize_update_status_generic(
                    chunk_id_key=chunk_idx_key,
                    chunk_storage=self._st_update_file_buffer[chunk_idx_key],
                )
                self._st_update_counter.pop(chunk_idx_key)
                self._st_update_counter[chunk_key] += 1
            elif chunk_key not in self._st_update_counter:
                self._st_update_counter[chunk_key] = chunk_id

    def finalize_standardize_update_status(self) -> None:
        """Save all not full chunk into update status"""
        with self._lock:
            while len(self._st_update_file_buffer):
                chunk_id_key, chunk_storage = self._st_update_file_buffer.popitem()

                chunk_reg = self.__update_chunk_main_part_regex__.match(chunk_id_key)

                if chunk_reg is not None:
                    self._finalize_update_status_generic(
                        chunk_id_key=chunk_id_key,
                        chunk_storage=chunk_storage,
                    )

    def _finalize_update_status_generic(
        self,
        chunk_id_key: str,
        chunk_storage: Queue,
    ) -> None:
        chunk_reg = self.__update_chunk_main_part_regex__.match(chunk_id_key)

        if chunk_reg is not None:
            chunk_key = f"chunk_{chunk_reg.group(1)}"
            chunk_id = self._st_update_counter[chunk_key]

            update_path, bucket = None, ""

            update = {
                "amounts": self._st_update_counter[chunk_id_key],
                "files": [],
                "updates": [],
            }

            while not chunk_storage.empty():
                mtr_file = chunk_storage.get()

                if not update_path:
                    update_path = Path(mtr_file.path).joinpath(self._update_preffix)
                    bucket = mtr_file.bucket

                update["files"].append(
                    {
                        "bucket": mtr_file.bucket,
                        "path": mtr_file.path,
                        "filename": mtr_file.file_name,
                    }
                )

                update["updates"].append(
                    {
                        "ref_hour_id": int(
                            format_date(
                                mtr_file.meter.start_time, CFG.HOUR_ID_DATE_FORMAT
                            )
                        ),
                        "ref_participant_id": int(self._config.extra.participant_id),
                        "ref_meter_id": int(mtr_file.meter.meter_id),
                        "data": float(mtr_file.meter.usage),
                    }
                )

                chunk_storage.task_done()

            self._st_update_queue.put(
                DataFile(
                    file_name=self._update_filename_tmpl.format(
                        update_prefix=self._update_preffix,
                        cnt=chunk_id,
                        run_date=self._st_base_update_file_name,
                    ),
                    bucket=bucket,
                    path=str(update_path),
                    body=dumps(update, indent=4, sort_keys=True),
                    cfg={},
                )
            )

    def finalize_fetch_update_status(self) -> None:
        """Save all not full chunk into update status"""
        with self._lock:
            while len(self._st_update_file_buffer):
                chunk_id_key, chunk_storage = self._fetch_update_file_buffer.popitem()
                self._finalize_update_status_generic(
                    chunk_id_key=chunk_id_key,
                    chunk_storage=chunk_storage,
                )

    @abstractmethod
    def _standardize(self, raw_file_obj: DataFile) -> DataFile:
        """Standardize the given raw file"""
