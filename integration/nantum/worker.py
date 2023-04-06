""" Nantum Workers module"""

import binascii
import hashlib
import hmac
import time
import urllib
import urllib.parse
import uuid
from abc import abstractmethod
from base64 import b64encode
from collections import Counter
from http import HTTPStatus
from json import dumps
from queue import Queue
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl
import requests
from dataclass_factory import Factory, Schema
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import get_missed_standardized_files, require_client
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate
from common.logging import Logger, ThreadPoolExecutorLogger
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.nantum.data_structures import (
    DataFile,
    FetchFile,
    RawFetchFile,
    StandardizedFile,
)
from integration.nantum.exceptions import AuthError, EmptyDataInterruption

RUN_GAPS_PARALLEL = True
RUN_FETCH_PARALLEL = True
RUN_STANDARDIZE_PARALLEL = True


class GapsDetectionWorker(BaseFetchWorker):
    """Nantum get missed hours worker functionality"""

    __created_by__ = "Nantum Missed Hours Worker"
    __description__ = "Nantum Integration"
    __name__ = "Nantum Missed Hours Worker"
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

    def configure(self, run_time: DateTime) -> None:
        self._run_time = run_time
        self._clear_queue(self._meters_queue)
        self._missed_hours_cache.clear()
        for mtr_cfg in self._config.meters:
            self._meters_queue.put(mtr_cfg)

    def missed_hours_consumer(
        self,
        storage_client: Client,
        start_date: DateTime,
        logs: Queue,  # pylint:disable=unused-argument
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Get missed data points"""
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
                start_date=start_date,
                bucket_name=mtr_cfg.standardized.bucket,
                bucket_path=mtr_cfg.standardized.path,
                range_hours=self._config.gap_regeneration_window,
                client=storage_client,
            )
            if mtr_msd_poll_hrs:
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
                self._th_logger.info(f"Meter {mtr_cfg.meter_name} is up to date.")
            self._meters_queue.task_done()

    def run(self, start_date: DateTime, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            [(self.missed_hours_consumer, [require_client(), start_date])],
            run_parallel=RUN_GAPS_PARALLEL,
        )

    # TODO: should be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class FetchWorker(BaseFetchWorker):
    """Nantum base worker functionality"""

    __created_by__ = "Nantum Connector"
    __description__ = "Nantum Integration"
    __name__ = "Nantum Worker"
    __oauth_signature_method__ = "HMAC-SHA1"
    __oauth_version__ = "1.0"
    __oauth_token__ = ""
    __method__ = "GET"

    __base_url__ = (
        "https://api-hub.nantum.io/clients/{company_id}/sites/{site}/{metric}"
    )

    __auth_errors__ = (
        HTTPStatus.UNAUTHORIZED.value,
        HTTPStatus.FORBIDDEN.value,
    )

    __date_format__ = "YYYY-MM-DD"
    __fetch_file_name_tmpl__ = "{base_file_name}_{idx}"
    __max_idle_run_count__ = 5

    __doc_day_start_shift_hours__ = 3
    __doc_day_duration_hours__ = 26
    __request_timeout__ = 60

    __five_minutes_res_days__ = 14
    __ten_minutes_res_days__ = 31
    __fifteen_minutes_res_days__ = 62
    __half_hour_res_days__ = 200

    __five_minutes_res__ = "5m"
    __ten_minutes_res__ = "10m"
    __fifteen_minutes_res__ = "15"
    __half_hour_res__ = "30m"
    __one_hour_res__ = "1h"

    __raw_meter_hour_col__ = "Response_dates"
    __raw_value_col__ = "RawValue"
    __df_date_times_col__ = "Dates"
    __df_aligned_date_times_col__ = "AlignedDates"
    __df_adjusted_date_times_col__ = "AdjustedDates"
    __df_aligned_adjusted_date_times_col__ = "AlignedAdjustedDates"
    __df_timestamp_col__ = "Timestamp"
    __df_meter_hour_col__ = "MeterHour"
    __df_meter_end_hour_col__ = "MeterEndHour"

    __df_occupancy_col__ = "Ocuppancy"
    __df_electricity_col__ = "Electricity"

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
        self._lock: Lock = Lock()
        self._raw_fetch_queue = Queue()
        self._fetch_counter = Counter()

        self._missed_hours_cache: Optional[Set] = None
        self._tmp_task_q: Queue = Queue()

        self._fetch_counter: Counter = Counter()
        self._base_name: Optional[str] = None

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

    @staticmethod
    def _create_signature(secret_key: str, signature_base_string: str) -> bytearray:
        encoded_string = signature_base_string.encode()
        encoded_key = secret_key.encode()
        hashed = hmac.new(encoded_key, encoded_string, hashlib.sha1).hexdigest()
        return b64encode(binascii.unhexlify(hashed)).decode()

    def _create_parameter_string(self, oauth_nonce: str, oauth_timestamp: str) -> str:
        return (
            f"oauth_consumer_key={self._config.client_key}"
            f"&oauth_nonce={oauth_nonce}"
            f"&oauth_signature_method={self.__oauth_signature_method__}"
            f"&oauth_timestamp={oauth_timestamp}"
            f"&oauth_token={self.__oauth_token__}"
            f"&oauth_version={self.__oauth_version__}"
        )

    def _get_headers(self) -> dict:
        oauth_nonce, oauth_timestamp = str(uuid.uuid4()), str(int(time.time()))

        encoded_parameter_string = urllib.parse.quote(
            self._create_parameter_string(oauth_nonce, oauth_timestamp), safe=""
        )

        base_url = self.__base_url__.format(
            company_id=self._config.company_id,
            site=self._config.site,
            metric=self._config.metric,
        )

        encoded_base_string = (
            f"{self.__method__}&{urllib.parse.quote(base_url, safe='')}"
        )

        oauth_signature = self._create_signature(
            f"{self._config.client_secret}&",
            f"{encoded_base_string}&{encoded_parameter_string}",
        )

        headers = {
            "Content-Type": "application/json",
            "Company": self._config.company_name,
            "Authorization": (
                f"OAuth oauth_consumer_key={self._config.client_key},"
                f"oauth_token={self.__oauth_token__},"
                f"oauth_signature_method={self.__oauth_signature_method__},"
                f"oauth_timestamp={oauth_timestamp},"
                f"oauth_nonce={oauth_nonce},"
                f"oauth_signature={oauth_signature},"
                f"oauth_version={self.__oauth_version__}"
            ),
        }

        return headers

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time=run_time)
        self._clear_queue(self._tmp_task_q)
        self._missed_hours_cache: Optional[Set] = set(
            list(self._missed_hours_queue.keys())
        )
        self._fetch_counter.clear()
        self._base_name = format_date(self._run_time, CFG.PROCESSING_DATE_FORMAT)

    def _request_data(self, url: str, params: dict, headers: str) -> Tuple:
        result, retry_count, delay = None, 0, 0.5

        while retry_count < self.__max_retry_count__:
            try:
                result = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.__request_timeout__,
                )
                if result.status_code == HTTPStatus.OK.value:
                    break
                if result.status_code in self.__auth_errors__:
                    raise AuthError()
            except (requests.ConnectionError, requests.ConnectTimeout, AuthError):
                retry_count += 1
                delay *= retry_count
                time.sleep(delay)
        return result

    def _get_response_resolution(self, period_in_days: int) -> str:
        """Ger Nentum Response granularity"""
        if period_in_days <= self.__five_minutes_res_days__:
            return self.__five_minutes_res__

        if (
            self.__five_minutes_res_days__
            < period_in_days
            <= self.__ten_minutes_res_days__
        ):
            return self.__ten_minutes_res__

        if (
            self.__ten_minutes_res_days__
            < period_in_days
            <= self.__fifteen_minutes_res_days__
        ):
            return self.__fifteen_minutes_res__

        if (
            self.__fifteen_minutes_res_days__
            < period_in_days
            <= self.__half_hour_res_days__
        ):
            return self.__half_hour_res__

        return self.__one_hour_res__

    def __df_rename_columns(self, data_df: pl.LazyFrame) -> pl.LazyFrame:
        return data_df.lazy().rename(
            {
                "time": self.__raw_meter_hour_col__,
                "value": self.__raw_value_col__,
            }
        )

    @staticmethod
    def __df_set_type_inline(
        data_df: pl.LazyFrame, column: str, pl_type: Any
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            pl.col(column).cast(pl_type, strict=False).keep_name()
        )

    # TODO: Refactiring candidate
    @staticmethod
    def __df_remove_dublicates(data_df: pl.LazyFrame) -> pl.LazyFrame:
        return data_df.lazy().unique()

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
    def __df_get_timestamp(
        data_df: pl.LazyFrame, date_col: str, dest_col: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (pl.col(date_col).apply(lambda y: int(y.timestamp()))).alias(dest_col)
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

    def _get_data_pl_df(self, data: List[Dict[str, str | float | int]]) -> pl.LazyFrame:
        if not data:
            raise EmptyDataInterruption("Recieved Empty Data")

        mtr_df = pl.from_dicts(data).lazy()

        return (
            mtr_df.pipe(self.__df_rename_columns)
            .pipe(self.__df_set_type_inline, self.__raw_value_col__, pl.Float64)
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
                self.__df_timestamp_col__,
            )
            .pipe(
                self.__df_get_meter_end_hour,
                self.__df_aligned_adjusted_date_times_col__,
                self.__df_meter_end_hour_col__,
            )
            .sort([self.__df_timestamp_col__])
        )

    def __df_take_one_from_group(
        self, data_df: pl.LazyFrame, group: str, keep_last: bool = False
    ) -> pl.LazyFrame:
        if keep_last:
            return data_df.lazy().groupby(group).agg(pl.all().last())
        return data_df.lazy().groupby(group).agg(pl.all().first())

    def _df_get_occupancy(
        self, data_df: pl.LazyFrame, raw_value_col: str, occupancy_col: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(pl.col(raw_value_col).alias(occupancy_col))

    def _df_get_previous_data(
        self,
        data_df: pl.LazyFrame,
        shift_columns: str | List[str],
        over_columns: Optional[str | List[str]] = None,
        period: int = 1,
        fill_value: int = 0,
        prefix: str = "prev_",
    ) -> pl.LazyFrame:
        if over_columns is not None:
            return data_df.lazy().with_columns(
                pl.col(shift_columns)
                .shift_and_fill(period, fill_value)
                .over(over_columns)
                .prefix(prefix)
            )
        return data_df.lazy().with_columns(
            pl.col(shift_columns).shift_and_fill(period, fill_value).prefix(prefix)
        )

    def _df_get_occupancy(
        self, data_df: pl.LazyFrame, raw_value_col: str, occupancy_col: str
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(pl.col(raw_value_col).alias(occupancy_col))

    def _df_get_electricity(
        self,
        data_df: pl.LazyFrame,
        value_col: str,
        prev_value_col: str,
        consumption_col: str,
    ) -> pl.LazyFrame:
        return data_df.lazy().with_columns(
            (pl.col(value_col) - pl.col(prev_value_col)).alias(consumption_col)
        )

    def _df_drop_first_rows(
        self, data_df: pl.LazyFrame, offset: int = 0, length: Optional[int] = None
    ) -> pl.LazyFrame:
        return data_df.lazy().slice(offset, length)

    def _df_map_reduce(self, data_df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            data_df.lazy()
            .pipe(self.__df_take_one_from_group, self.__df_meter_hour_col__, True)
            .sort([self.__df_timestamp_col__])
            .pipe(
                self._df_get_occupancy,
                self.__raw_value_col__,
                self.__df_occupancy_col__,
            )
            .pipe(self._df_get_previous_data, shift_columns=self.__raw_value_col__)
            .pipe(
                self._df_get_electricity,
                value_col=self.__raw_value_col__,
                prev_value_col=f"prev_{self.__raw_value_col__}",
                consumption_col=self.__df_electricity_col__,
            )
            .pipe(self._df_drop_first_rows, offset=1)
        )

    def fetch(self) -> None:
        """Fetch data"""
        if not bool(len(self._missed_hours_queue)):
            self._logger.warning(
                "Missed hours queue is empty. Maybe data up to date.",
            )
            return None

        missed_hours = self._missed_hours_queue.keys()
        start_date = truncate(min(missed_hours).subtract(days=1), level="hour")
        end_date = truncate(max(missed_hours).add(days=1), level="hour")
        period = end_date - start_date

        url = self.__base_url__.format(
            company_id=self._config.company_id,
            site=self._config.site,
            metric=self._config.metric,
        )

        query_date_gte = format_date(start_date, self.__date_format__)
        query_date_lt = format_date(end_date, self.__date_format__)
        query_resolution = self._get_response_resolution(period.in_days())

        try:
            result = self._request_data(
                url=url,
                params={
                    "query[device_id]": self._config.sensor_id,
                    "query[date][gte]=": query_date_gte,
                    "query[date][lt]=": query_date_lt,
                    "options[daily]": "false",
                    "options[resolution]": query_resolution,
                    "options[flatten]": "true",
                },
                headers=self._get_headers(),
            )
        except (requests.ConnectionError, requests.ConnectTimeout, AuthError) as err:
            self._logger.error(
                f"Cannot fetch data due to the error '{err}'.",
            )
            return None
        if result and result.status_code != HTTPStatus.OK:
            self._logger.error(
                f"Cannot fetch data due to the error '{result.text}'.",
            )

        raw_data = result.json()
        if not raw_data or not raw_data["docs"][0].get("readings", []):
            self._th_logger.warning(
                f"Recieved empty data for the query[device_id]="
                f"'{self._config.sensor_id}'; query[date][gte]={query_date_gte}"
                f"; query[date][lt]={query_date_lt}; options[daily]=flae;"
                f"options[resolution]={query_resolution}; "
            )
            return None

        file_info = RawFetchFile(
            file_name=self._base_name,
            bucket=self._config.extra.raw.bucket,
            path=self._config.extra.raw.path,
            body=dumps(self._factory.dump(raw_data), sort_keys=True, indent=4),
        )

        self._shadow_fetched_files_queue.put(file_info)
        self._add_to_update(file_info, self._fetch_update_file_buffer)

        data_df = self._get_data_pl_df(raw_data["docs"][0].get("readings", []))
        data_df = self._df_map_reduce(data_df).collect()

        if not bool(len(data_df)):
            self._th_logger.warning(
                "Cannot calculate data based on th eraw data located in "
                f"gs://{self._config.extra.raw.bucket}/"
                f"{self._config.extra.raw.path}/{self._base_name}"
            )
            return None

        for mtr_hr, mtr_cfgs in self._missed_hours_queue.items():
            mtr_fl = FetchFile(
                data_df=data_df,
                meters=mtr_cfgs,
            )
            mtr_fl.timestamps.put(mtr_hr)
            self._fetched_files_queue.put(mtr_fl)

        return None

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self.fetch()

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
    """Nantum Standardization Worker."""

    __created_by__ = "Nantum Connector"
    __description__ = "Nantum Integration"
    __name__ = "Nantum Standardize Worker"

    __df_meter_hour_col__ = "MeterHour"
    __df_meter_end_hour_col__ = "MeterEndHour"

    __df_occupancy_col__ = "Ocuppancy"
    __df_electricity_col__ = "Electricity"

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
        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

    def _standardize_middleware(
        self, data_df: pl.DataFrame, mtr_date: str, column_name: str, mtr_cfg: Any
    ) -> Optional[Meter]:
        data_row = (
            data_df.filter(
                pl.col(self.__df_meter_hour_col__).apply(lambda x: x == mtr_date)
            )
            .select(
                [
                    self.__df_meter_hour_col__,
                    self.__df_meter_end_hour_col__,
                    column_name,
                ]
            )
            .to_dicts()
        )

        if not bool(len(data_row)):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        def _getter(xdata: List[Dict[str, Any]]) -> Optional[str]:
            return (
                xdata.get(column_name),
                xdata.get(self.__df_meter_hour_col__),
                xdata.get(self.__df_meter_end_hour_col__),
            )

        return self._standardize_generic(data_row[0], mtr_cfg, _getter)

    def _standardize_occupancy(
        self,
        data_df: pl.DataFrame,
        mtr_date: str,
        mtr_cfg: Any,
    ) -> Optional[Meter]:
        return self._standardize_middleware(
            data_df=data_df,
            mtr_date=mtr_date,
            mtr_cfg=mtr_cfg,
            column_name=self.__df_occupancy_col__,
        )

    def _standardize_electric(
        self,
        data_df: pl.DataFrame,
        mtr_date: str,
        mtr_cfg: Any,
    ) -> Optional[Meter]:

        return self._standardize_middleware(
            data_df=data_df,
            mtr_date=mtr_date,
            mtr_cfg=mtr_cfg,
            column_name=self.__df_electricity_col__,
        )

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""
        # TODO: Redesign worker to process all meter points in parallel
        standardized_files = []

        if not bool(len(raw_file_obj.data_df)):
            self._th_logger.error("Recived empty standardize data. Skipping")
            return []

        while not raw_file_obj.timestamps.empty():
            mtr_hr = raw_file_obj.timestamps.get()

            meter_hour = format_date(
                self._adjust_meter_date(truncate(mtr_hr, level="hour")),
                CFG.PROCESSING_DATE_FORMAT,
            )
            while not raw_file_obj.meters.empty():
                mtr_cfg = raw_file_obj.meters.get()
                meter_type = mtr_cfg.type.strip().lower().replace(" ", "_")
                stndrdz_mthd_nm = f"_standardize_{meter_type}"
                stndrdz_func = getattr(self, stndrdz_mthd_nm, "")
                if not callable(stndrdz_func):
                    #  TODO: Add loging here
                    continue

                meter = stndrdz_func(
                    data_df=raw_file_obj.data_df,
                    mtr_date=meter_hour,
                    mtr_cfg=mtr_cfg,
                )
                if meter:
                    standardized_files.append(
                        StandardizedFile(
                            file_name=format_date(
                                meter_hour, CFG.PROCESSING_DATE_FORMAT
                            ),
                            bucket=mtr_cfg.standardized.bucket,
                            path=mtr_cfg.standardized.path,
                            meter=meter,
                            body=meter.as_str(),
                            cfg=mtr_cfg,
                        )
                    )
            raw_file_obj.timestamps.task_done()

        return standardized_files

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
