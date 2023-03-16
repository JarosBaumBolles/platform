""" Data Warehouse Workers module"""

import uuid
from abc import abstractmethod
from collections import Counter
from pathlib import Path
from queue import Queue
from typing import Any, Dict

from google.cloud.exceptions import GoogleCloudError
from google.cloud.storage import Client
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import list_blobs_with_prefix, move_blob, require_client
from common.logging import Logger, ThreadPoolExecutorLogger
from integration.base_integration import BaseWorker
from integration.db_load.meters_data_db_load.data_structures import FileIno

LOAD_UPDATES_FILES_PARALLEL = True
LOAD_UPDATES_DATA_PARALLEL = True
LOAD_FINALYZE_UPDATES_PARALLEL = True


class LoadUpdatesFilesWorker(BaseWorker):
    """DB Load get list of updates files"""

    __created_by__ = "DB Load Updates List Worker"
    __description__ = "DB Load Integration"
    __name__ = "DB Load Updates List Worker"

    __update_prefix__ = CFG.UPDATE_PREFIX
    __update_filename_preffix_tmpl__ = CFG.UPDATE_FILENAME_PREFFIX_TMPL
    __max_idle_run_count__ = 5

    def __init__(  # pylint:disable=super-init-not-called
        self,
        updates_files: Queue,
        config: Any,
    ) -> None:
        self._task_q: Queue = Queue()
        self._updates_q: Queue = updates_files
        self._config: Any = config
        self._trace_id: str = uuid.uuid4()
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)
        self._th_logger = ThreadPoolExecutorLogger(
            description=self.__description__, trace_id=self._trace_id
        )

    def configure(self, run_time: DateTime) -> None:
        self._run_time = run_time
        self._clear_queue(self._task_q)
        self._clear_queue(self._updates_q)
        for mtr_cfg in self._config.meters:
            self._task_q.put(mtr_cfg.extra.standardized)

    def load_updates_consumer(
        self, storage_client: Client, logs: Queue, worker_idx: str
    ) -> None:
        """Load update"""
        if self._task_q.empty():
            logs.put(
                (
                    "WARNING",
                    worker_idx,
                    "Meters queue is empty.",
                )
            )
            return None
        empty_run_count = 0
        while True:
            if self._task_q.empty():
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
                continue
            mtr_cfg = self._task_q.get()

            mtr_path = Path(mtr_cfg.path).joinpath(self.__update_prefix__)

            update_files = list_blobs_with_prefix(
                client=storage_client,
                bucket_name=mtr_cfg.bucket,
                prefix=str(
                    mtr_path.joinpath(
                        self.__update_filename_preffix_tmpl__.format(
                            update_prefix=self.__update_prefix__
                        )
                    )
                ),
                force_dir=False,
            )
            if update_files:
                for upd_fl in update_files:
                    file_path = Path(upd_fl.name)

                    self._updates_q.put(
                        FileIno(
                            bucket=upd_fl.bucket.name,
                            path=file_path.parent,
                            filename=file_path.name,
                        )
                    )

            self._task_q.task_done()

    def run(self, run_time: DateTime) -> None:
        """Run loop Entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            (self.load_updates_consumer, [require_client()]),
            run_parallel=LOAD_UPDATES_FILES_PARALLEL,
        )

    @abstractmethod
    def _add_to_update(self, file: Dict, chunk_storage: Dict) -> None:
        pass


class LoadUpdatesdataWorker(BaseWorker):
    """DB Load Load update data to insert"""

    __created_by__ = "DB Load Load update data"
    __description__ = "DB Load Integration"
    __name__ = "DB Load Load update data"

    __rows_max_size__ = CFG.DW_LOAD_FILES_BUCKET_LIMIT
    __max_idle_run_count__ = 5

    def __init__(
        self,
        updates_files: Queue,
        update_data: Queue,
        loaded_update_files: Queue,
        config: Any,
    ) -> None:

        super().__init__(config)

        self._update_files_q: Queue = updates_files
        self._updata_data_q: Queue = update_data
        self._updata_data_counter: Counter = Counter()
        self._loaded_update_files_q: Queue() = loaded_update_files

        self._trace_id: str = uuid.uuid4()
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)

        self._th_logger = ThreadPoolExecutorLogger(
            description=self.__description__, trace_id=self._trace_id
        )

    def configure(self, run_time: DateTime) -> None:
        self._run_time = run_time
        self._clear_queue(self._updata_data_q)
        self._clear_queue(self._loaded_update_files_q)
        self._updata_data_counter.clear()

    def load_updates_data_consumer(
        self, storage_client: Client, logs: Queue, worker_idx: str
    ) -> None:
        """Update files loading"""
        if self._update_files_q.empty():
            logs.put(
                (
                    "WARNING",
                    worker_idx,
                    "Meters queue is empty.",
                )
            )
            return None
        empty_run_count = 0
        while True:
            if self._update_files_q.empty():
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
                continue
            upd_fl = self._update_files_q.get()

            logs.put(
                (
                    "DEBUG",
                    worker_idx,
                    f"Processing update status file 'gs://"
                    f"{upd_fl.bucket.rstrip('/')}/{upd_fl.path}/{upd_fl.filename}",
                )
            )

            upd_json = self._load_json_data(
                bucket=str(upd_fl.bucket),
                path=str(upd_fl.path),
                filename=str(upd_fl.filename),
                client=storage_client,
                logs=logs,
            )

            with self._lock:
                row_count = self._updata_data_counter["row_count"]

                batch_size = int(upd_json.get("amounts", 0))
                if row_count + batch_size > self._max_chunk_size:
                    logs.put(
                        (
                            "INFO",
                            worker_idx,
                            f"Riched out allowed rows amount {self._max_chunk_size}"
                            ". Exit.",
                        )
                    )
                    break
                for row in upd_json.get("updates", []):
                    self._updata_data_q.put(row)
                logs.put(
                    (
                        "DEBUG",
                        worker_idx,
                        f"Processed update status file 'gs://"
                        f"{upd_fl.bucket.rstrip('/')}/{upd_fl.path}/{upd_fl.filename}",
                    )
                )
                self._updata_data_counter["row_count"] += batch_size
                self._loaded_update_files_q.put(upd_fl)

            self._update_files_q.task_done()

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            (self.load_updates_data_consumer, [require_client()]),
            run_parallel=LOAD_UPDATES_DATA_PARALLEL,
        )
        self._clear_queue(self._update_files_q)

    @abstractmethod
    def _add_to_update(self, file: Dict, chunk_storage: Dict) -> None:
        pass


class FinalizeUpdatesWorker(BaseWorker):
    """DB Load get list of updates files"""

    __created_by__ = "DB Load Updates List Worker"
    __description__ = "DB Load Integration"
    __name__ = "DB Load Updates List Worker"

    __update_prefix__ = CFG.UPDATE_PREFIX
    __update_filename_preffix_tmpl__ = CFG.UPDATE_FILENAME_PREFFIX_TMPL
    __processed_prefix__ = CFG.PROCESSED_PREFIX
    __max_idle_run_count__ = 5
    __max_retry_count__ = 3
    __retry_delay__ = 0.5

    def __init__(  # pylint:disable=super-init-not-called
        self,
        updates_files: Queue,
        config: Any,
    ) -> None:
        self._update_files_q: Queue = updates_files
        self._config: Any = config
        self._trace_id: str = uuid.uuid4()
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)

        self._th_logger = ThreadPoolExecutorLogger(
            description=self.__description__, trace_id=self._trace_id
        )

    def updates_to_processed_consumer(
        self, storage_client: Client, logs: Queue, worker_idx: str
    ) -> None:
        """Move processed update files"""
        if self._update_files_q.empty():
            logs.put(
                (
                    "WARNING",
                    worker_idx,
                    "Meters queue is empty.",
                )
            )
            return None
        empty_run_count = 0
        while True:
            if self._update_files_q.empty():
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
                continue
            upd_fl = self._update_files_q.get()

            if not upd_fl.filename.startswith(self.__update_prefix__):
                logs.put(
                    (
                        "WARNING",
                        worker_idx,
                        f"Noticed not update file 'gs://{upd_fl.bucket}'/"
                        f"{upd_fl.path}/{upd_fl.filename} in the queue. Skipping",
                    )
                )
                continue

            fl_base_path = Path(upd_fl.path)

            fl_ex_path = fl_base_path.joinpath(upd_fl.filename)

            fl_dest_path = fl_base_path.joinpath(
                upd_fl.filename.replace(
                    self.__update_prefix__, self.__processed_prefix__
                )
            )
            retry_count, delay = 0, self.__retry_delay__
            while retry_count < self.__max_retry_count__:
                try:
                    move_blob(
                        client=storage_client,
                        bucket_name=upd_fl.bucket,
                        blob_name=str(fl_ex_path).lstrip("/"),
                        destination_bucket=upd_fl.bucket,
                        new_blob_name=str(fl_dest_path).lstrip("/"),
                    )
                    break
                except GoogleCloudError as err:
                    retry_count += 1
                    delay *= retry_count

                    logs.put(
                        (
                            "ERROR",
                            self._trace_id,
                            f"Failed moving file {str(fl_ex_path)} to "
                            f"'{str(fl_dest_path)}' due to the '{err}'"
                            f"Retrying in {delay} seconds...",
                        )
                    )
            if retry_count >= self.__max_retry_count__:
                logs.put(
                    (
                        "ERROR",
                        self._trace_id,
                        f"Failed moving file {str(fl_ex_path)} to "
                        f"'{str(fl_dest_path)}'",
                    )
                )

    @abstractmethod
    def _add_to_update(self, file: Dict, chunk_storage: Dict) -> None:
        pass

    def run(self) -> None:
        """Run loop entrypoint"""
        self._run_consumers(
            (self.updates_to_processed_consumer, [require_client()]),
            run_parallel=LOAD_FINALYZE_UPDATES_PARALLEL,
        )
        self._clear_queue(self._update_files_q)
