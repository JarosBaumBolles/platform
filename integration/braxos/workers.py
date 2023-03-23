""" Braxos Workers module"""

import tempfile
import uuid
from abc import abstractmethod
from collections import Counter
from io import StringIO
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional

import pysftp
from dataclass_factory import Factory, Schema
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pandas import DataFrame, read_csv
from paramiko.ssh_exception import SSHException
from pendulum import DateTime

import common.settings as CFG
from common.bucket_helpers import get_missed_standardized_files, require_client
from common.data_representation.standardized.meter import Meter
from common.date_utils import GapDatePeriod, format_date, parse, truncate
from common.logging import Logger, ThreadPoolExecutorLogger
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.braxos.data_structures import DataFile, StandardizedFile
from integration.braxos.exception import EmptyDataInterruption, LoadFromConnectorAPI

RUN_GAPS_PARALLEL = False
RUN_FETCH_PARALLEL = False
RUN_STANDARDIZE_PARALLEL = False


class GapsDetectionWorker(BaseFetchWorker):
    """Braxos get missed hours worker functionality"""

    __created_by__ = "Braxos Missed Hours Worker"
    __description__ = "Braxos Integration"
    __name__ = "Braxos Missed Hours Worker"
    __max_idle_run_count__ = 5

    def __init__(  # pylint:disable=super-init-not-called
        self,
        missed_hours_cache: ExpiringDict,
        config: Any,
    ) -> None:
        self._missed_hours_cache = missed_hours_cache
        self._config = config
        self._meters_queue = Queue()
        self._trace_id: str = uuid.uuid4()
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)
        self._th_logger = ThreadPoolExecutorLogger(
            description=self.__description__, trace_id=self._trace_id
        )
        self._expected_hours: Optional[GapDatePeriod] = None

    def configure(self, run_time: DateTime) -> None:
        self._run_time = run_time
        self._clear_queue(self._meters_queue)
        self._missed_hours_cache.clear()
        self._expected_hours = GapDatePeriod(
            self._run_time, self._config.gap_regeneration_window - 1
        )
        for mtr_cfg in self._config.meters:
            self._meters_queue.put(mtr_cfg)

    def missed_hours_consumer(
        self,
        storage_client: Client,
        logs: Queue,  # pylint:disable=unused-argument
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Get missed hours"""
        if self._meters_queue.empty():
            self._th_logger.warning("Meters queue is empty.")
            return None
        empty_run_count = 0
        while True:
            if self._meters_queue.empty():
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
                continue

            mtr_cfg = self._meters_queue.get()
            mtr_msd_poll_hrs = get_missed_standardized_files(
                bucket_name=mtr_cfg.standardized.bucket,
                bucket_path=mtr_cfg.standardized.path,
                range_hours=self._config.gap_regeneration_window,
                client=storage_client,
                date_range=self._expected_hours,
            )
            if mtr_msd_poll_hrs:
                # missed_hours.put((mtr_cfg, mtr_msd_poll_hrs))
                for mtr_hr in mtr_msd_poll_hrs:
                    try:
                        self._missed_hours_cache[mtr_hr].put(mtr_cfg)
                    except KeyError:
                        self._missed_hours_cache[mtr_hr] = Queue()
                        self._missed_hours_cache[mtr_hr].put(mtr_cfg)

                self._th_logger.info(
                    f"Found {len(mtr_msd_poll_hrs)} in '{mtr_cfg.meter_name}' "
                    "meter.",
                )
            else:
                self._th_logger.info(
                    f"Meter {mtr_cfg.meter_name} is up to date.",
                )
            self._meters_queue.task_done()

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            (self.missed_hours_consumer, [require_client()]),
            run_parallel=RUN_GAPS_PARALLEL,
        )

    # TODO: should be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class FetchWorker(BaseFetchWorker):
    """Braxos fetch worker functionality"""

    __created_by__ = "Braxos Fetch Connector"
    __description__ = "Braxos Integration"
    __name__ = "Braxos Fetch Worker"

    __braxos_api_datetime_format__ = "YYYY-MM-DD-HH"
    __braxos_api_date_format__ = "YYYY-MM-DD"
    __provider_filename_tmpl__ = "sensor-events-{provider_datetime}.csv"

    __max_retry_count__ = 3
    __retry_delay__ = 0.5
    __max_idle_run_count__ = 5

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

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

    def _load_from_file(
        self, filename: str, storage_client: Client, logs: Queue
    ) -> Optional[Dict]:
        fl_exists = self._is_file_exists(
            client=storage_client,
            bucket=self._config.extra.raw.bucket,
            path=self._config.extra.raw.path,
            file_name=filename,
        )
        if not fl_exists:
            raise LoadFromConnectorAPI(
                f"The local file 'gs://{self._config.extra.raw.bucket}/"
                f"{self._config.extra.raw.path}/{filename}'"
                "is absent"
            )
        self._th_logger.warning(
            f"File 'gs://{self._config.extra.raw.bucket}/"
            f"{self._config.extra.raw.path}/{filename} already exists. "
            "Loading from the bucket."
        )
        data = self._retry_load_data(
            bucket=self._config.extra.raw.bucket,
            path=self._config.extra.raw.path,
            filename=filename,
            logs=logs,
            client=storage_client,
        )

        if not data:
            raise LoadFromConnectorAPI(
                f"The raw file 'gs://{self._config.extra.raw.bucket}/"
                f"{self._config.extra.raw.path}/{filename}'"
                "is empty. Skipping"
            )
        return data

    def fetch_consumer(  # pylint:disable=too-many-locals
        self,
        storage_client: Client,
        logs: Queue,
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Fetch data worker"""
        if not bool(len(self._missed_hours_queue)):
            self._th_logger.warning(
                "Missed hours queue is empty. Maybe data up to date.",
            )
            return None
        empty_run_count = 0
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        cnopts.log = True

        params = {
            "host": self._config.server,
            "username": self._config.username,
            "private_key": self._config.key,
        }
        params["cnopts"] = cnopts

        with tempfile.TemporaryDirectory() as temp_dir:
            with pysftp.Connection(**params) as sftp:
                while True:
                    if not bool(len(self._missed_hours_queue)):
                        if empty_run_count == self.__max_idle_run_count__:
                            break
                        empty_run_count += 1
                    else:
                        mtr_hr, mtr_cfgs = self._missed_hours_queue.popitem()
                        mtr_hr = truncate(mtr_hr, level="hour")
                        provider_filename = self.__provider_filename_tmpl__.format(
                            provider_datetime=format_date(
                                mtr_hr, self.__braxos_api_datetime_format__
                            )
                        )

                        try:
                            data = self._load_from_file(
                                filename=provider_filename,
                                storage_client=storage_client,
                                logs=logs,
                            )
                            add_to_update = False
                        except LoadFromConnectorAPI as err:
                            self._th_logger.info(
                                f"Canot load the local file due to the reason '{err}'"
                                " Loading from the WatTime API"
                            )
                            filepath = Path(temp_dir).joinpath(provider_filename)
                            try:
                                sftp.get(provider_filename, str(filepath))
                            except (FileNotFoundError, SSHException) as ft_err:
                                self._th_logger.error(
                                    f"Cannot download the file {provider_filename} due to "
                                    f"the error {ft_err}"
                                )
                                continue
                            with open(filepath, "r", encoding="utf-8") as file:
                                data = file.read()

                            add_to_update = True

                        file_info = DataFile(
                            file_name=provider_filename,
                            bucket=self._config.extra.raw.bucket,
                            path=self._config.extra.raw.path,
                            body=data,
                            meters=mtr_cfgs,
                        )
                        file_info.timestamps.put(mtr_hr)

                        self._fetched_files_queue.put(file_info)
                        self._shadow_fetched_files_queue.put(file_info)
                        if add_to_update:
                            self._add_to_update(
                                file_info, self._fetch_update_file_buffer
                            )

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            [
                (self.fetch_consumer, [require_client()]),
                (self.save_fetched_files_worker, []),
            ],
            run_parallel=RUN_FETCH_PARALLEL,
        )
        self.finalize_fetch_update_status()
        self._run_consumers(
            [
                (self.save_fetch_status_worker, []),
            ],
            run_parallel=RUN_FETCH_PARALLEL,
        )
        self._logger.info("Fetching has been done.")

    # TODO: should be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class StandardizeWorker(BaseStandardizeWorker):
    """Density Standardization Worker."""

    __created_by__ = "Density Connector"
    __description__ = "Density Integration"
    __name__ = "Density Standardize Worker"
    __enter_event__ = "entry"
    __exit_event__ = "exit"

    def _standardize_occupancy(
        self, data_df: DataFrame, mtr_date: DateTime, mtr_cfg: Dict
    ) -> Meter:
        """Standardize occupancy value"""
        if not bool(len(data_df)):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        start_date = truncate(mtr_date, level="hour")
        end_date = start_date.add(minutes=59, seconds=59)

        entry_count = len(data_df[data_df["events"] == self.__enter_event__])
        exit_count = len(data_df[data_df["events"] == self.__exit_event__])

        occupancy = max(entry_count - exit_count, 0)

        def _getter(
            xdata: DataFrame,  # pylint:disable=unused-argument
        ) -> Optional[str]:
            return occupancy, start_date, end_date

        return self._standardize_generic(data_df, mtr_cfg, _getter)

    @staticmethod
    def date_repr(dt_str: str):
        """Get date string reprezentation and timestamp"""
        data = parse(dt_str, tz_info="UTC")
        data_timestamp = int(data.timestamp())
        data_hr_str = format_date(
            truncate(data, level="hour"), CFG.PROCESSING_DATE_FORMAT
        )
        return data_hr_str, data_timestamp

    def _get_data_df(self, data: str) -> DataFrame:
        raw_df = read_csv(StringIO(data))
        raw_df = raw_df.drop_duplicates()
        raw_df["ActivityID"] = raw_df["ActivityID"].astype(float)
        raw_df[["hour", "timestamp"]] = list(map(self.date_repr, raw_df.NDT))
        raw_df.sort_values(by=["timestamp"], inplace=True, ascending=False)
        raw_df["events"] = list(
            map(lambda x: x.split("-")[-1].strip().lower(), raw_df.EventName)
        )
        raw_df = raw_df.reset_index()
        return raw_df

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""

        if not raw_file_obj.body:
            self._th_logger.error("Recieved empty standardize data. Skipping")
            return []

        data_df = self._get_data_df(raw_file_obj.body)

        # TODO: Redesign worker to process all meter points in parallel
        standardized_files = []
        while not raw_file_obj.timestamps.empty():
            mtr_hr = raw_file_obj.timestamps.get()
            while not raw_file_obj.meters.empty():
                mtr_cfg = raw_file_obj.meters.get()
                meter_type = mtr_cfg.type.strip().lower().replace(" ", "_")
                stndrdz_mthd_nm = f"_standardize_{meter_type}"
                stndrdz_func = getattr(self, stndrdz_mthd_nm, "")
                if not callable(stndrdz_func):
                    #  TODO: Add loging here
                    continue

                try:
                    meter = stndrdz_func(
                        data_df=data_df,
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
            raw_file_obj.timestamps.task_done()
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
