""" Orion Workers module"""

import csv
from abc import abstractmethod
from collections import Counter
from io import StringIO
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional

from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import list_blobs_with_prefix, move_blob, require_client
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.orion.data_structures import DataFile, MoveFile, StandardizedFile
from integration.orion.exceptions import EmptyDataInterruption, RawFileValidationError

RUN_FETCH_PARALLEL = False
RUN_STANDARDIZE_PARALLEL = False


# TODO: @todo Candidate tfor rfactoring. All general code of integration
# where iles are readed from storage shoud be moved to the general class
class FetchWorker(BaseFetchWorker):
    """Orion fetch worker functionality"""

    __created_by__ = "Orion Fetch Connector"
    __description__ = "Orion Integration"
    __name__ = "Orion Fetch Worker"

    __raw_fl_name_pref__ = "raw-data"

    __junk_folder__ = "junk"
    __failed_folder__ = "failed"
    __processed_files__ = "processed"

    __max_idle_run_count__ = 5

    __csv_min_len__ = 2
    __csv_first_row_len__ = 2
    __csv_first_row_header__ = "date"

    __csv_second_row_len__ = 2
    __csv_second_row_header__ = "total_in_building"

    __raw_occupancy_col__ = "occupancy"
    __raw_date_col__ = "date"

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
        self._tmp_task_q: Queue = Queue()
        self._junk_folder: Optional[str] = None
        self._failed_folder: Optional[str] = None
        self._processed_folder: Optional[str] = None

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time)
        self._clear_queue(self._fetch_processed_q)
        self._junk_folder = str(
            Path(self._config.extra.raw.path).joinpath(self.__junk_folder__)
        )
        self._failed_folder = str(
            Path(self._config.extra.raw.path).joinpath(self.__failed_folder__)
        )
        self._processed_folder = str(
            Path(self._config.extra.raw.path).joinpath(self.__processed_files__)
        )

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
            fl_name, fl_path = fl_path.name, str(fl_path.parent)
            fl_name_check = fl_name.startswith(self.__raw_fl_name_pref__)
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
                        destination_path=self._junk_folder,
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

            self._tmp_task_q.put(file_info)

    def _get_csv_datafrom_str(self, data: str) -> List[List[str]]:
        return list(
            csv.reader(
                StringIO(data),
                delimiter=self._config.csv_delimiter,
                quotechar=self._config.csv_delimiter,
                quoting=csv.QUOTE_MINIMAL,
            )
        )

    def _csv_validate(self, csv_data: List[List[str]]) -> None:

        if len(csv_data) < self.__csv_min_len__:
            raise RawFileValidationError(
                f"CSV file does not match with Orion template."
                f"File must contains at list {self.__csv_min_len__} rows with data"
            )

        first_row = csv_data[0]

        if len(first_row) != self.__csv_first_row_len__:
            raise RawFileValidationError(
                f"CSV file does not match with Orion template."
                f"First row must contains {self.__csv_first_row_len__} elements only."
            )

        if first_row[0].strip() != self.__csv_first_row_header__:
            raise RawFileValidationError(
                f"CSV file does not match with Orion template."
                f"First row field name {first_row[0]} is not equal with "
                f" '{self.__csv_first_row_header__}'"
            )

        second_row = csv_data[1]

        if len(second_row) != self.__csv_second_row_len__:
            raise RawFileValidationError(
                f"CSV file does not match with Orion template."
                f"Second row must contains {self.__csv_second_row_len__} "
                "elements."
            )

        if second_row[0].strip() != self.__csv_second_row_header__:
            raise RawFileValidationError(
                f"CSV file does not match with Orion template."
                f"Second row first field name {second_row[0]} is not "
                f"equal with expected '{self.__csv_second_row_header__}'"
            )

    def fetch_consumer(
        self,
        storage_client: Client,
        logs: Queue,
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Fetch data worker"""
        if self._tmp_task_q.empty():
            self._th_logger.warning("Meters queue is empty.")
            return None
        empty_run_count = 0
        while True:
            if self._tmp_task_q.empty():
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
                continue

            file_info = self._tmp_task_q.get()
            data = self._retry_load_data(
                client=storage_client,
                bucket=file_info.bucket,
                path=file_info.path,
                filename=file_info.file_name,
                logs=logs,
            )

            csv_data = self._get_csv_datafrom_str(data)

            try:
                self._csv_validate(csv_data)
            except RawFileValidationError as err:
                self._th_logger.error(
                    f"Find unexpected file due to the reason '{err}'. Moving to"
                    f" {self.__failed_folder__}"
                )
                self._fetch_processed_q.put(
                    MoveFile(
                        filename=file_info.file_name,
                        bucket=self._config.extra.raw.bucket,
                        path=file_info.path,
                        destination_filename=file_info.file_name,
                        destination_bucket=self._config.extra.raw.bucket,
                        destination_path=self._failed_folder,
                    )
                )
                continue

            data_fl = DataFile(
                bucket=file_info.bucket,
                path=file_info.path,
                file_name=file_info.file_name,
                body={
                    self.__raw_date_col__: csv_data[0][1].replace(
                        self._config.timezone_to_delete, self._config.timezone_to_insert
                    ),
                    self.__raw_occupancy_col__: csv_data[1][1],
                },
                meters=file_info.meters,
            )

            self._fetched_files_queue.put(data_fl)
            self._add_to_update(data_fl, self._fetch_update_file_buffer)
            self._fetch_processed_q.put(
                MoveFile(
                    filename=file_info.file_name,
                    bucket=self._config.extra.raw.bucket,
                    path=file_info.path,
                    destination_filename=file_info.file_name,
                    destination_bucket=self._config.extra.raw.bucket,
                    destination_path=self._processed_folder,
                )
            )
            self._tmp_task_q.task_done()

    # TODO @todo Should be refactored.
    def _move_processed_files_consumer(
        self,
        storage_client: Client,
        logs: Queue,  # pylint:disable=unused-argument
        worker_idx: str,  # pylint:disable=unused-argument
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
                (self.fetch_consumer, [require_client()]),
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
    """Orion Standardization Worker."""

    __created_by__ = "Orion Connector"
    __description__ = "Orion Integration"
    __name__ = "Orion Standardize Worker"

    __junk_folder__ = "junk"
    __failed_folder__ = "failed"
    __processed_folder__ = "processed"

    __raw_occupancy_col__ = "occupancy"
    __raw_date_col__ = "date"

    __date_format__ = "YYYY-MM-DD[T]HH:mm:ssZ"

    def _standardize_occupancy(
        self, data_row: Dict, mtr_date: DateTime, mtr_cfg: Dict
    ) -> Meter:
        """Standardize electric value"""
        if not bool(len(data_row)):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        start_date = self._adjust_meter_date(mtr_date)
        end_date = start_date.add(minutes=59, seconds=59)

        def _getter(xdata: List[Dict[str, Any]]) -> Optional[str]:
            return (xdata.get(self.__raw_occupancy_col__), start_date, end_date)

        return self._standardize_generic(data_row, mtr_cfg, _getter)

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""

        if not raw_file_obj.body:
            self._th_logger.error("Recived empty standardize data. Skipping")
            return []

        raw_data = raw_file_obj.body

        # # TODO: Redesign worker to process all meter points in parallel
        standardized_files = []

        while not raw_file_obj.meters.empty():
            mtr_cfg = raw_file_obj.meters.get()
            meter_type = mtr_cfg.type.strip().lower().replace(" ", "_")
            stndrdz_mthd_nm = f"_standardize_{meter_type}"
            stndrdz_func = getattr(self, stndrdz_mthd_nm, "")
            if not callable(stndrdz_func):
                #  TODO: Add loging here
                continue

            try:
                mtr_hr = parse(
                    raw_data[self.__raw_date_col__], self.__date_format__
                ).in_timezone(CFG.UTC_TIMEZONE)

                meter = stndrdz_func(
                    data_row=raw_data,
                    mtr_date=mtr_hr,
                    mtr_cfg=mtr_cfg,
                )
            except EmptyDataInterruption as err:
                self._th_logger.error(
                    f"Cannot standardize meter {mtr_cfg.meter_name}"
                    f" point '{mtr_hr}' due to the error '{err}'"
                )
                continue

            standardized_files.append(
                StandardizedFile(
                    file_name=format_date(mtr_hr, CFG.PROCESSING_DATE_FORMAT),
                    bucket=mtr_cfg.standardized.bucket,
                    path=mtr_cfg.standardized.path,
                    meter=meter,
                    body=meter.as_str(),
                    cfg=mtr_cfg,
                )
            )
            raw_file_obj.meters.task_done()
        return standardized_files

    # TODO: @todo Possible candite to be in base class. Or boiler plate code
    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
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
            [(self.save_standardize_status_worker, [])],
            run_parallel=RUN_STANDARDIZE_PARALLEL,
        )
