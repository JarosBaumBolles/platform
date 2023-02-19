""" Facit integration module"""
from abc import abstractmethod
from collections import Counter
from pathlib import Path
from queue import Queue
from typing import Any, List

from dataclass_factory import Factory, Schema
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pendulum import DateTime

from common.bucket_helpers import list_blobs_with_prefix, move_blob, require_client
from common.data_representation.config.participant import (
    ConfigException,
    XmlTypeException,
)
from common.data_representation.standardized.meter import Meter as MeterValue
from common.data_representation.standardized.meter import StandardizedMeterException
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.facit.data_structures import DataFile, MoveFile, StandardizedFile

RUN_FETCH_PARALLEL = True
RUN_STANDARDIZE_PARALLEL = True


class FetchWorker(BaseFetchWorker):
    """Facit fetch worker functionality"""

    __created_by__ = "Facit Fetch Connector"
    __description__ = "Facit Integration"
    __name__ = "Facit Fetch Worker"
    __ocpy_fl_name_pref__ = "occupancy_"

    __junk_folder__ = "junk"
    __failed_folder__ = "failed"

    def __init__(
        self,
        missed_hours: ExpiringDict,
        fetched_files: Queue,
        fetch_update: Queue,
        config: Any,
    ) -> None:
        super().__init__(
            missed_hours=missed_hours,
            fetched_files=fetched_files,
            fetch_update=fetch_update,
            config=config,
        )

        self._missed_hours_queue: ExpiringDict = missed_hours

        self._raw_fetch_queue = Queue()
        self._fetch_counter = Counter()
        self._fetch_processed_q = Queue()

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time)
        self._clear_queue(self._fetch_processed_q)

    def get_new_files(self, storage_client: Client) -> None:
        """Get list of new uploaded files"""
        blobs = list_blobs_with_prefix(
            client=storage_client,
            delimiter="/",
            bucket_name=self._config.extra.raw.bucket,
            prefix=self._config.extra.raw.path,
        )

        for blob in blobs:
            fl_path = Path(blob.name)
            fl_name = fl_path.name
            fl_path = str(fl_path.parent)
            fl_name_check = fl_name.startswith(self.__ocpy_fl_name_pref__)
            fl_path_check = fl_path != self._config.extra.raw.path

            if not fl_name_check or fl_path_check:
                self._th_logger.warning(
                    f"Found junk file 'gs://{self._config.extra.raw.bucket}/"
                    f"{fl_path}/{fl_name}' that does not match with the"
                    " expected occupancy pattern. Moving to junk."
                )
                self._fetch_processed_q.put(
                    MoveFile(
                        filename=fl_name,
                        bucket=self._config.extra.raw.bucket,
                        path=fl_path,
                        destination_filename=fl_name,
                        destination_bucket=self._config.extra.raw.bucket,
                        destination_path=str(
                            Path(self._config.extra.raw.path).joinpath(
                                self.__failed_folder__
                            )
                        ),
                    )
                )
                continue

            file_info = DataFile(
                file_name=fl_name,
                bucket=blob.bucket.name,
                path=fl_path,
                body="",
            )

            for mtr in self._config.meters:
                file_info.meters.put(mtr)

            self._fetched_files_queue.put(file_info)
            self._shadow_fetched_files_queue.put(file_info)
            self._add_to_update(file_info, self._fetch_update_file_buffer)

    # TODO @todo Should be refactored. The similar code is in Facit
    def _move_processed_files_consumer(
        self,
        storage_client: Client,
        logs: Queue,        # pylint:disable=unused-argument
        worker_idx: str,    # pylint:disable=unused-argument
    ) -> None:
        if self._fetch_processed_q.empty():
            self._th_logger.warning(
                "The files queue is empty. Shutdown worker.",
            )
            return None
        empty_run_count = 0
        while True:
            if self._fetch_processed_q.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                file_info = self._fetch_processed_q.get()
                self._th_logger.debug(
                    f"Moving file gs://{file_info.bucket}/{file_info.path}/"
                    f"{file_info.filename}"
                    f" to gs://{file_info.destination_bucket}/"
                    f"/{file_info.destination_path}"
                    f"{file_info.destination_filename}"
                )

                move_blob(
                    client=storage_client,
                    bucket_name=file_info.bucket,
                    blob_name=str(Path(file_info.path).joinpath(file_info.filename)),
                    destination_bucket=file_info.destination_bucket,
                    new_blob_name=str(
                        Path(file_info.destination_path).joinpath(
                            file_info.destination_filename
                        )
                    ),
                    quiet=True,
                )
                self._th_logger.debug(
                    f"Moved file 'gs://{file_info.bucket}/{file_info.path}"
                    f"{file_info.filename}' to the "
                    f"'gs://{file_info.destination_bucket}/"
                    f"{file_info.destination_path}/{file_info.destination_filename}'.",
                )
                self._fetch_processed_q.task_done()

    def run(self, run_time: DateTime) -> None:
        """Run loop entry point"""
        self.configure(run_time)
        self.get_new_files(require_client())

        self.finalize_fetch_update_status()
        self._run_consumers(
            [
                (self.save_fetch_status_worker, []),
                (self._move_processed_files_consumer, [require_client()]),
            ],
            run_parallel=RUN_FETCH_PARALLEL,
        )
        self._logger.info("Fetching has been done.")

    # TODO: should be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class StandardizeWorker(BaseStandardizeWorker):
    """Facit Standardization Worker."""

    __created_by__ = "Facit Connector"
    __description__ = "Facit Integration"
    __name__ = "Facit Standardize Worker"

    __junk_folder__ = "junk"
    __failed_folder__ = "failed"
    __processed_folder__ = "processed"

    def __init__(
        self,
        raw_files: Queue,
        standardized_files: Queue,
        standardize_update: Queue,
        config: Any,
    ) -> None:
        super().__init__(
            raw_files=raw_files,
            standardized_files=standardized_files,
            standardize_update=standardize_update,
            config=config,
        )

        self._st_processed_q = Queue()

    def configure(self, run_time: DateTime) -> None:
        """Configurator"""
        super().configure(run_time=run_time)
        self._clear_queue(self._st_processed_q)

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""
        # TODO: Redesign worker to process all meter points in parallel
        standardized_files = []
        expected_exceptions = (
            StandardizedMeterException,
            XmlTypeException,
            ConfigException,
        )
        while not raw_file_obj.meters.empty():
            mtr_cfg = raw_file_obj.meters.get()

            self._th_logger.info("sdfg")
            meter = MeterValue()
            try:
                meter.read_from_bucket(
                    bucket=raw_file_obj.bucket,
                    subdirectory=raw_file_obj.path,
                    filename=raw_file_obj.file_name,
                    binary_mode=False,
                )
                meter.meter_uri = mtr_cfg.meter_uri
                self._th_logger.info("sdfg")
            except expected_exceptions as err:
                self._th_logger.error(
                    f"Cannot standardize file 'gs://{raw_file_obj.bucket}/"
                    f"{raw_file_obj.path}/{raw_file_obj.file_name}' due to "
                    f"erro {err}"
                )
                self._st_processed_q.put(
                    MoveFile(
                        filename=raw_file_obj.file_name,
                        bucket=raw_file_obj.bucket,
                        path=raw_file_obj.path,
                        destination_filename=raw_file_obj.file_name,
                        destination_bucket=raw_file_obj.bucket,
                        destination_path=str(
                            Path(raw_file_obj.path).joinpath(self.__failed_folder__)
                        ),
                    )
                )

            else:
                standardized_files.append(
                    StandardizedFile(
                        file_name=raw_file_obj.file_name,
                        bucket=mtr_cfg.standardized.bucket,
                        path=mtr_cfg.standardized.path,
                        meter=meter,
                        body=meter.as_str(),
                        cfg=mtr_cfg,
                    )
                )

                self._st_processed_q.put(
                    MoveFile(
                        filename=raw_file_obj.file_name,
                        bucket=raw_file_obj.bucket,
                        path=raw_file_obj.path,
                        destination_filename=raw_file_obj.file_name,
                        destination_bucket=raw_file_obj.bucket,
                        destination_path=str(
                            Path(raw_file_obj.path).joinpath(self.__processed_folder__)
                        ),
                    )
                )

            raw_file_obj.meters.task_done()

        return standardized_files

    def _move_processed_files_consumer(
        self,
        storage_client: Client,
        logs: Queue,        # pylint:disable=unused-argument
        worker_idx: str,    # pylint:disable=unused-argument
    ) -> None:
        if self._st_processed_q.empty():
            self._th_logger.warning(
                "The files queue is empty. Shutdown worker.",
            )
            return None
        empty_run_count = 0
        while True:
            if self._st_processed_q.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                file_info = self._st_processed_q.get()
                self._th_logger.debug(
                    f"Moving file gs://{file_info.bucket}/{file_info.path}/"
                    f"{file_info.filename}"
                    f" to gs://{file_info.destination_bucket}/"
                    f"/{file_info.destination_path}"
                    f"{file_info.destination_filename}"
                )

                move_blob(
                    client=storage_client,
                    bucket_name=file_info.bucket,
                    blob_name=str(Path(file_info.path).joinpath(file_info.filename)),
                    destination_bucket=file_info.destination_bucket,
                    new_blob_name=str(
                        Path(file_info.destination_path).joinpath(
                            file_info.destination_filename
                        )
                    ),
                    quiet=True,
                )
                self._th_logger.debug(
                    f"Moved file 'gs://{file_info.bucket}/{file_info.path}"
                    f"{file_info.filename}' to the "
                    f"'gs://{file_info.destination_bucket}/"
                    f"{file_info.destination_path}/{file_info.destination_filename}'.",
                )
                self._st_processed_q.task_done()

    # TODO: @todo Possible candite to be in base class. Or boiler plate code
    def run(self, run_time: DateTime) -> None:
        """Run loop entry point"""
        self.configure(run_time)

        self._run_consumers(
            [
                (self.run_standardize_worker, []),
                (self.save_standardized_files_worker, []),
            ],
            run_parallel=RUN_STANDARDIZE_PARALLEL,
        )

        self.finalize_standardize_update_status()
        self._run_consumers(
            [
                (self.save_standardize_status_worker, []),
                (self._move_processed_files_consumer, [require_client()]),
            ],
            run_parallel=RUN_STANDARDIZE_PARALLEL,
        )
