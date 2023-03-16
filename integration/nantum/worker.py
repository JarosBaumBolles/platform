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
from typing import Any, List, Optional, Set, Tuple

import requests
from dataclass_factory import Factory, Schema
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pandas import DataFrame
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import get_missed_standardized_files, require_client
from common.data_representation.standardized.meter import Meter
from common.date_utils import date_range, format_date, parse, truncate
from common.logging import Logger, ThreadPoolExecutorLogger
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.nantum.data_structures import (
    DataFile,
    DocFetchFile,
    NantumResponse,
    RawFetchFile,
    StandardizedFile,
)
from integration.nantum.exceptions import AuthError

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

        url = self.__base_url__.format(
            company_id=self._config.company_id,
            site=self._config.site,
            metric=self._config.metric,
        )

        try:
            result = self._request_data(
                url=url,
                params={
                    "query[device_id]": self._config.sensor_id,
                    "query[date][gte]=": format_date(start_date, self.__date_format__),
                    "query[date][lt]=": format_date(end_date, self.__date_format__),
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
        data = self._factory.load(raw_data, NantumResponse)

        for doc in data.docs:
            self._tmp_task_q.put(doc)

        file_info = RawFetchFile(
            file_name=self._base_name,
            bucket=self._config.extra.raw.bucket,
            path=self._config.extra.raw.path,
            body=dumps(self._factory.dump(raw_data), sort_keys=True, indent=4),
        )

        self._shadow_fetched_files_queue.put(file_info)
        self._add_to_update(file_info, self._fetch_update_file_buffer)
        return None

    def unbundle_fetch_data_consumer(
        self,
        logs: Queue,  # pylint:disable=unused-argument
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Split fetched data into chunk"""
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

            doc = self._tmp_task_q.get()

            start_date = truncate(
                parse(doc.date).add(hours=self.__doc_day_start_shift_hours__),
                level="hour",
            )
            end_date = start_date.add(hours=self.__doc_day_duration_hours__)

            doc_dates_t = sorted(
                list(date_range(start_date=start_date, end_date=end_date))
            )

            doc_dates = set(doc_dates_t)
            missed_hours = self._missed_hours_cache.intersection(doc_dates)

            for mt_hr in missed_hours:
                mtr_cfgs = self._missed_hours_queue.get(mt_hr)
                if mtr_cfgs:
                    mtr_fl = DocFetchFile(
                        doc=doc,
                        meters=mtr_cfgs,
                    )
                    mtr_fl.timestamps.put(mt_hr)
                    self._fetched_files_queue.put(mtr_fl)
                else:
                    self._th_logger.warning(
                        f"Cannot find related meter for hour '{mt_hr}'. Skip"
                    )

            self._tmp_task_q.task_done()

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self.fetch()

        self._run_consumers(
            [
                (self.unbundle_fetch_data_consumer, []),
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
        self, data: DataFrame, column_name: str, mtr_cfg: Any
    ) -> Optional[Meter]:
        if data:
            start_date = truncate(parse(data[0]["meter_hour"]), level="hour")
            end_date = start_date.add(minutes=59, seconds=59)
            return self._standardize_generic(
                data=data,
                getter=lambda xdata: (xdata[0][column_name], start_date, end_date),
                mtr_cfg=mtr_cfg,
            )
        return None

    def _standardize_occupancy(
        self,
        data: DataFrame,
        #  mtr_date: DateTime,
        mtr_cfg: Any,
    ) -> Optional[Meter]:
        return self._standardize_middleware(
            data=data,
            # mtr_date=mtr_date,
            mtr_cfg=mtr_cfg,
            column_name="occupancy",
        )

    def _standardize_electric(
        self,
        data: DataFrame,
        # mtr_date: DateTime,
        mtr_cfg: Any,
    ) -> Optional[Meter]:
        return self._standardize_middleware(
            data=data,
            # mtr_date=mtr_date,
            mtr_cfg=mtr_cfg,
            column_name="consumption",
        )

    def date_repr(self, dt_str: str):
        """Get date string reprezentation and timestamp"""
        real_date = parse(dt_str, tz_info="UTC")
        data = truncate(real_date, level="hour")

        data_hr_str = format_date(data, CFG.PROCESSING_DATE_FORMAT)

        time_shift = self._config.timestamp_shift.get("shift", None)
        kwargs = self._config.timestamp_shift.get("shift_hours", {})
        mtr_data_hr_str = data_hr_str
        if time_shift in ("add", "subtruct"):
            if kwargs and any(kwargs.values()):
                if time_shift == "add":
                    mtr_data_hr_str = format_date(
                        data.add(**kwargs), CFG.PROCESSING_DATE_FORMAT
                    )
                else:
                    mtr_data_hr_str = format_date(
                        data.subtract(**kwargs), CFG.PROCESSING_DATE_FORMAT
                    )
        return data_hr_str, mtr_data_hr_str, int(real_date.timestamp())

    def _get_data_df(self, data: dict) -> DataFrame:
        raw_df = DataFrame.from_dict(data)
        raw_df[["hour", "meter_hour", "timestamp"]] = list(
            map(self.date_repr, raw_df.time)
        )
        raw_df.sort_values(by=["timestamp"], inplace=True)

        raw_df["raw_consumption"] = raw_df.groupby(["hour"])["value"].transform(max)
        raw_df.loc[:, "occupancy"] = raw_df["raw_consumption"]

        raw_df = raw_df.drop_duplicates(
            subset=["hour", "raw_consumption", "occupancy"], keep="last"
        )

        raw_df.loc[:, "consumption"] = raw_df["raw_consumption"].diff(periods=1)
        raw_df["consumption"].fillna(raw_df["value"], inplace=True)
        return raw_df

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""
        # json_data = loads(raw_file_obj.body)
        # TODO: Redesign worker to process all meter points in parallel
        standardized_files = []

        raw_data = self._get_data_df(self._factory.dump(raw_file_obj.doc.readings))

        while not raw_file_obj.timestamps.empty():
            mtr_hr = raw_file_obj.timestamps.get()
            meter_hour = format_date(
                truncate(parse(mtr_hr), level="hour"), CFG.PROCESSING_DATE_FORMAT
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
                    data=raw_data.loc[raw_data["hour"] == meter_hour].to_dict(
                        "records"
                    ),
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
