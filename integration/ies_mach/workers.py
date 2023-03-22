""" Ies Mach Workers module"""

import time
import uuid
from abc import abstractmethod
from collections import Counter
from http import HTTPStatus
from json import JSONDecodeError, dumps
from queue import Queue
from typing import Any, Dict, List, Optional, Sequence

import polars as pl
import requests
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pendulum import DateTime

import common.settings as CFG
from common.bucket_helpers import get_missed_standardized_files, require_client
from common.data_representation.standardized.meter import Meter
from common.date_utils import GapDatePeriod, format_date, parse, truncate
from common.logging import Logger, ThreadPoolExecutorLogger
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.ies_mach.data_structures import DataFile, StandardizedFile
from integration.ies_mach.exceptions import (
    EmptyDataInterruption,
    EmptyResponse,
    LoadFromConnectorAPI,
)

RUN_GAPS_PARALLEL = True
RUN_FETCH_PARALLEL = True
RUN_STANDARDIZE_PARALLEL = True


# TODO: @todo Must be moved to a base class
class GapsDetectionWorker(BaseFetchWorker):
    """IES Mach get missed hours worker functionality"""

    __created_by__ = "IES Mach Missed Hours Worker"
    __description__ = "IES Mach Integration"
    __name__ = "IES Mach Missed Hours Worker"
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
        """Retrieving missed data points."""
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
                date_range=self._expected_hours
            )
            if mtr_msd_poll_hrs:
                for mt_hr in mtr_msd_poll_hrs:
                    try:
                        self._missed_hours_cache[mt_hr].put(mtr_cfg)
                    except KeyError:
                        self._missed_hours_cache[mt_hr] = Queue()
                        self._missed_hours_cache[mt_hr].put(mtr_cfg)

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
        """Run loop Entrypoint"""
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
    """Irisys fetch worker functionality"""

    __created_by__ = "Irisys Fetch Connector"
    __description__ = "Irisys Integration"
    __name__ = "Irisys Fetch Worker"

    __fetch_url__ = "https://api.iesmach.com/v1/intervals/query.json"

    __api_date_formats__ = {
        "buildinglocal": "YYYY-MM-DDTHH:mm",
        "utc": "YYYY-MM-DDTHH:mm[Z]",
    }

    __api_default_date_format__ = "YYYY-MM-DDTHH:mm[Z]"

    __max_retry_count__ = 3
    __retry_delay__ = 0.5
    __max_idle_run_count__ = 5
    __request_timeout__ = 60

    __raw_meter_hour_col__ = "RawDates"
    __raw_occupancy_col__ = "Occupancy"
    __drop_columns__ = ["statusId"]

    __df_date_times_col__ = "Dates"
    __df_aligned_date_times_col__ = "AlignedDates"
    __df_adjusted_date_times_col__ = "AdjustedDates"
    __df_aligned_adjusted_date_times_col__ = "AlignedAdjustedDates"
    __df_timestamp_coll__ = "Timestamp"
    __df_meter_hour_col__ = "MeterHour"
    __df_meter_end_hour_col__ = "MeterEndHour"

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
        self._auth_token: Optional[str] = ""

    # TODO: @todo MUt be moved in base class/ The same code ia in Willow
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
        data = self._retry_load_json_data(
            bucket=self._config.extra.raw.bucket,
            path=self._config.extra.raw.path,
            filename=filename,
            logs=logs,
            client=storage_client,
        )

        if not data or not data[0].get("intervals", []):
            raise LoadFromConnectorAPI(
                f"The raw file 'gs://{self._config.extra.raw.bucket}/"
                f"{self._config.extra.raw.path}/{filename}'"
                "is empty. Skipping"
            )
        return data

    # TODO: @todo Potential candidate to be in the base class
    # The similar code used in Openweather and Willow api
    def _request_data(
        self, url: str, json_params: dict, headers: str
    ) -> Optional[Dict]:
        result, retry_count, delay = None, 0, 0.5
        data = None
        while retry_count < self.__max_retry_count__:
            try:
                result = requests.post(
                    url,
                    json=json_params,
                    headers=headers,
                    timeout=self.__request_timeout__,
                )
                if result.status_code == HTTPStatus.OK.value:
                    data = result.json()
                    break
                retry_count += 1
                delay *= retry_count
                time.sleep(delay)
            except (requests.ConnectionError, requests.ConnectTimeout):
                retry_count += 1
                delay *= retry_count
                time.sleep(delay)
            except JSONDecodeError as err:
                self._th_logger.error(
                    f"Response does not contain json data due to the reason '{err}'"
                )
        check = not result or not data or not data[0].get("intervals", [])
        if check or result.status_code != HTTPStatus.OK.value:
            raise EmptyResponse(
                "Cannot run request corectly. "
                f"Response status code is {result.status_code}. "
                f"Response message is '{result.text}'. Response parameters: - "
                f"{json_params}; "
            )
        return data

    def __df_rename_columns(self, data_df: pl.LazyFrame) -> pl.LazyFrame:
        return data_df.lazy().rename(
            {
                "datetime": self.__raw_meter_hour_col__,
                "value": self.__raw_occupancy_col__,
            }
        )

    def __df_drop_columns(
        self, data_df: pl.LazyFrame, columns: str | Sequence[str]
    ) -> pl.LazyFrame:
        return data_df.lazy().drop(columns)

    # TODO: Refactiring candidate
    @staticmethod
    def __df_remove_dublicates(data_df: pl.LazyFrame) -> pl.LazyFrame:
        return data_df.lazy().unique()

    @staticmethod
    def __df_set_type_inline(
        data_df: pl.LazyFrame, column: str, pl_type: Any
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            pl.col(column).cast(pl_type, strict=False).keep_name()
        )

    @staticmethod
    def __df_set_date_type(
        data_df: pl.LazyFrame, date_col: str, dest_col: str, tz_info: str = "UTC"
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (pl.col(date_col).apply(lambda y: parse(y, tz_info=tz_info))).alias(
                dest_col
            )
        )

    @staticmethod
    def __df_get_meter_hour(
        data_df: pl.LazyFrame, date_col: str, dest_col: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (
                pl.col(date_col).apply(
                    lambda y: format_date(y, CFG.PROCESSING_DATE_FORMAT)
                )
            ).alias(dest_col)
        )

    @staticmethod
    def __df_get_meter_end_hour(
        data_df: pl.LazyFrame, date_col: str, dest_col: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (
                pl.col(date_col).apply(
                    lambda y: format_date(
                        y.add(minutes=59, seconds=59), CFG.PROCESSING_DATE_FORMAT
                    )
                )
            ).alias(dest_col)
        )

    @staticmethod
    def __df_get_timestamp(
        data_df: pl.LazyFrame, date_col: str, dest_col: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (pl.col(date_col).apply(lambda y: int(y.timestamp()))).alias(dest_col)
        )

    @staticmethod
    def __df_truncate_dates(
        data_df: pl.LazyFrame, date_col: str, dest_col: str, level: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (pl.col(date_col).apply(lambda y: truncate(y, level=level))).alias(dest_col)
        )

    def __df_adjust_dates(
        self, data_df: pl.LazyFrame, date_col: str, dest_col: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (pl.col(date_col).apply(self._adjust_meter_date)).alias(dest_col)
        )

    def __df_sort_by_group(
        self, data_df: pl.LazyFrame, column: str, group: str, reverse: bool = False
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(pl.col(column).sort(reverse).over(group))

    def __df_take_one_from_group(
        self, data_df: pl.LazyFrame, group: str, keep_last: bool = False
    ) -> pl.LazyFrame:
        if keep_last:
            return data_df.lazy().groupby(group).agg(pl.all().last())
        return data_df.lazy().groupby(group).agg(pl.all().first())

    def _get_data_pl_df(self, data: List[Dict[str, str | float | int]]) -> pl.LazyFrame:
        if not data:
            raise EmptyDataInterruption("Recieved Empty Data")

        mtr_df = pl.from_dicts(data).lazy()

        # NOTE: The IES Mach allows to get a few different occupancy values
        # for the same date point. To fix this issue we try to take rows with
        # maximum occupancy for each data point

        return (
            mtr_df.pipe(self.__df_rename_columns)
            .pipe(self.__df_drop_columns, self.__drop_columns__)
            .pipe(self.__df_set_type_inline, self.__raw_occupancy_col__, pl.Int64)
            .pipe(self.__df_remove_dublicates)
            .pipe(
                self.__df_set_date_type,
                self.__raw_meter_hour_col__,
                self.__df_date_times_col__,
            )
            .pipe(
                self.__df_truncate_dates,
                self.__df_date_times_col__,
                self.__df_aligned_date_times_col__,
                "hour",
            )
            .pipe(
                self.__df_sort_by_group,
                self.__raw_occupancy_col__,
                self.__raw_meter_hour_col__,
                True,
            )
            .pipe(self.__df_take_one_from_group, self.__raw_meter_hour_col__, False)
            .pipe(
                self.__df_adjust_dates,
                self.__df_date_times_col__,
                self.__df_adjusted_date_times_col__,
            )
            .pipe(
                self.__df_truncate_dates,
                self.__df_adjusted_date_times_col__,
                self.__df_aligned_adjusted_date_times_col__,
                "hour",
            )
            .pipe(
                self.__df_get_meter_hour,
                self.__df_aligned_adjusted_date_times_col__,
                self.__df_meter_hour_col__,
            )
            .pipe(
                self.__df_get_timestamp,
                self.__df_adjusted_date_times_col__,
                self.__df_timestamp_coll__,
            )
            .pipe(
                self.__df_get_meter_end_hour,
                self.__df_aligned_adjusted_date_times_col__,
                self.__df_meter_end_hour_col__,
            )
            .sort([self.__df_timestamp_coll__])
        )

    def _df_map_reduce(self, data_df: pl.LazyFrame) -> pl.LazyFrame:
        return data_df.lazy().pipe(
            self.__df_take_one_from_group, self.__df_meter_hour_col__, True
        )

    def fetch_consumer(
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

        possible_errors = (EmptyResponse, requests.exceptions.JSONDecodeError)

        filename = format_date(truncate(self._run_time), CFG.PROCESSING_DATE_FORMAT)

        start_date, *_, end_date = sorted(list(self._missed_hours_queue.keys()))

        try:
            try:
                data = self._load_from_file(
                    filename=filename, storage_client=storage_client, logs=logs
                )
                add_to_update = False
            except LoadFromConnectorAPI as err:
                self._th_logger.info(
                    f"Canot load the local file due to the reason '{err}'"
                    " Loading from the IES Mach API"
                )
                start_date = truncate(start_date, level="hour")
                end_date = truncate(end_date, level="hour").add(minutes=59, seconds=59)

                lcl_tmz = self.__api_date_formats__.get(
                    self._config.time_zone.strip().lower(),
                    self.__api_default_date_format__,
                )

                data = self._request_data(
                    url=self.__fetch_url__,
                    json_params={
                        "start": format_date(start_date, lcl_tmz),
                        "end": format_date(end_date, lcl_tmz),
                        "responseTimeZone": self._config.time_zone,
                        "datapoints": int(self._config.datapoint_id),
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Basic {self._config.auth_token}",
                    },
                )
                add_to_update = True
        except possible_errors as err:
            self._th_logger.error(
                f"Cannot fetch data for period from '{start_date}' to '{end_date}'"
                f" due to the error '{err}'. Skipping."
            )
            return None

        data_intervals = data[0].get("intervals", [])

        if not bool(len(data_intervals)):
            self._th_logger.error(
                f"Recieved empty data for the perion from '{start_date}' to "
                f"'{end_date}'. Skipping."
            )
            return None

        data_df = self._get_data_pl_df(data_intervals)
        data_df = self._df_map_reduce(data_df)

        file_info = DataFile(
            file_name=filename,
            bucket=self._config.extra.raw.bucket,
            path=self._config.extra.raw.path,
            body=dumps(data, indent=4, sort_keys=True),
        )
        self._shadow_fetched_files_queue.put(file_info)
        if add_to_update:
            self._add_to_update(file_info, self._fetch_update_file_buffer)

        data = (
            data_df.select(
                [
                    self.__df_aligned_date_times_col__,
                    self.__df_aligned_adjusted_date_times_col__,
                    self.__df_meter_hour_col__,
                    self.__df_meter_end_hour_col__,
                    self.__raw_occupancy_col__,
                ]
            )
            .collect()
            .to_dicts()
        )

        for rec in data:
            mtr_hr = rec.get(self.__df_aligned_date_times_col__)
            mtr_cfgs = self._missed_hours_queue.get(mtr_hr)
            if mtr_hr and mtr_cfgs:
                data_file = DataFile(
                    file_name=rec.get(self.__df_meter_hour_col__),
                    bucket=self._config.extra.raw.bucket,
                    path=self._config.extra.raw.path,
                    body=rec,
                    meters=mtr_cfgs,
                )
                data_file.timestamps.put(mtr_hr)
                self._fetched_files_queue.put(data_file)
            else:
                self._th_logger.warning(
                    f"Canot find '{mtr_hr}' in missed hours list. Skipping"
                )

        return None

    def run(self, run_time: DateTime) -> None:
        """Run loop entry point"""
        self.configure(run_time)

        self._run_consumers(
            [
                (self.fetch_consumer, [require_client()]),
            ],
            run_parallel=False,
        )

        self._run_consumers(
            [
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
    """IES Mach Standardization Worker."""

    __created_by__ = "IES Mach Connector"
    __description__ = "IES Mach Integration"
    __name__ = "IES Mach Standardize Worker"

    __raw_occupancy_col__ = "Occupancy"
    __df_aligned_date_times_col__ = "AlignedDates"
    __df_aligned_adjusted_date_times_col__ = "AlignedAdjustedDates"
    __df_meter_hour_col__ = "MeterHour"
    __df_meter_end_hour_col__ = "MeterEndHour"

    def _standardize_occupancy(
        self, data_row: Dict, mtr_date: DateTime, mtr_cfg: Dict
    ) -> Meter:
        """Standardize electric value"""
        if not bool(len(data_row)):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        def _getter(xdata: List[Dict[str, Any]]) -> Optional[str]:
            return (
                xdata.get(self.__raw_occupancy_col__),
                xdata.get(self.__df_meter_hour_col__),
                xdata.get(self.__df_meter_end_hour_col__),
            )

        return self._standardize_generic(data_row, mtr_cfg, _getter)

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""

        if not raw_file_obj.body:
            self._th_logger.error("Recived empty standardize data. Skipping")
            return []

        raw_data = raw_file_obj.body

        # # TODO: Redesign worker to process all meter points in parallel
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
