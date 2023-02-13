""" Willow Workers module"""
import time
import uuid
from abc import abstractmethod
from collections import Counter
from http import HTTPStatus
from json import JSONDecodeError, dumps, loads
from queue import Queue
from typing import Any, Dict, List, Optional

import requests
from dataclass_factory import Factory, Schema
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pandas import DataFrame
from pendulum import DateTime

import common.settings as CFG
from common.bucket_helpers import get_missed_standardized_files, require_client
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate
from common.logging import Logger, ThreadPoolExecutorLogger
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.willow.data_structures import DataFile, StandardizedFile
from integration.willow.exceptions import (
    AuthtorizeException,
    EmptyDataInterruption,
    EmptyResponse,
    LoadFromConnectorAPI,
)

RUN_GAPS_PARALLEL = True
RUN_FETCH_PARALLEL = False
RUN_STANDARDIZE_PARALLEL = True


class GapsDetectionWorker(BaseFetchWorker):
    """Willow get missed hours worker functionality"""

    __created_by__ = "Willow Missed Hours Worker"
    __description__ = "Willow Integration"
    __name__ = "Willow Missed Hours Worker"
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
        logs: Queue,  # pylint:disable=unused-argument
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Get list of missed hours"""
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
            )
            if mtr_msd_poll_hrs:
                for mtr_hr in mtr_msd_poll_hrs:
                    # TODO: @totdo Fix to add ability to use multiple configs per few hours
                    # See Wattime as the example
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

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time=run_time)
        self._run_consumers(
            (
                self.missed_hours_consumer, [require_client()]
            ),
            run_parallel=RUN_GAPS_PARALLEL
        )

    # TODO: should be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class FetchWorker(BaseFetchWorker):
    """Willow fetch worker functionality"""

    __created_by__ = "Willow Fetch Connector"
    __description__ = "Willow Integration"
    __name__ = "Willow Fetch Worker"

    __fetch_url__ = "https://api.willowinc.com/v2/sites/{}/points/{}/trendlog"
    __auth_url__ = "https://api.willowinc.com/v2/oauth2/token"
    __api_date_format__ = "YYYY-MM-DD HH:mm"

    __max_retry_count__ = 3
    __retry_delay__ = 0.5
    __max_idle_run_count__ = 5

    __request_timeout__ = 120

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
        data = self._retry_load_json_data(
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

    # TODO: @todo Potential candidate to be in the base class
    # The similar code used in Openweather api
    def _request_data(self, url: str, params: dict, headers: str) -> Optional[Dict]:
        result, retry_count, delay = None, 0, 0.5
        data = None
        while retry_count < self.__max_retry_count__:
            try:
                result = requests.get(
                    url,
                    params=params,
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

        if not result or not data or result.status_code != HTTPStatus.OK.value:
            raise EmptyResponse(
                "Cannot run request corectly"
                f"Response status code is {result.status_code}. "
                f"Response message is {result.text}. Response parameters: - "
                f"{params}; "
            )
        return data

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time=run_time)
        try:
            self._auth_token = self._get_access_token()
        except AuthtorizeException as err:
            self._logger.error(f"Failed Willow authorization with error {err}")
            self._auth_token = ""

    def _get_access_token(self):
        """Get access token"""
        if not all(x for x in (self._config.client_id, self._config.client_secret)):
            raise AuthtorizeException(
                "Configuration is not run or provided empty credential"
            )

        retry_count, delay, token = 0, 1, ""

        while retry_count < self.__max_retry_count__:
            try:
                resp = requests.post(
                    self.__auth_url__,
                    json={
                        "clientId": self._config.client_id,
                        "clientSecret": self._config.client_secret,
                    },
                    timeout=self.__request_timeout__,
                )
                if resp.status_code == HTTPStatus.OK.value:
                    token = resp.json().get("accessToken")
                    break
                retry_count += 1
                delay = retry_count * self.__retry_delay__
                time.sleep(delay)
                                
            except (requests.ConnectionError, requests.ConnectTimeout):
                retry_count += 1
                delay = retry_count * self.__retry_delay__
                time.sleep(delay)

        return token

    def fetch_consumer(  # pylint:disable=too-many-locals
        self,
        storage_client: Client,
        logs: Queue,
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Consumer used to download data from the Willow API"""
        if not bool(len(self._missed_hours_queue)):
            self._th_logger.warning(
                "Missed hours queue is empty. Maybe data up to date.",
            )
            return None

        if not self._auth_token:
            self._th_logger.error(
                "The Acces Token is not acquired.",
            )
            return None

        empty_run_count = 0
        possible_errors = (EmptyResponse, requests.exceptions.JSONDecodeError)
        fetch_url = self.__fetch_url__.format(
            self._config.site_id, self._config.point_id
        )
        while True:
            if not bool(len(self._missed_hours_queue)):
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
            else:
                mtr_hr, mtr_cfgs = self._missed_hours_queue.popitem()
                filename = format_date(mtr_hr, CFG.PROCESSING_DATE_FORMAT)
                try:
                    try:
                        data = self._load_from_file(
                            filename=filename, storage_client=storage_client, logs=logs
                        )
                        add_to_update = False
                    except LoadFromConnectorAPI as err:
                        self._th_logger.info(
                            f"Canot load the local file due to the reason '{err}'"
                            " Loading from the WatTime API"
                        )
                        actual_date = truncate(mtr_hr, level="hour")
                        end_date = format_date(
                            actual_date.add(minutes=59, seconds=59),
                            self.__api_date_format__,
                        )
                        start_date = format_date(
                            actual_date.subtract(minutes=59, seconds=59),
                            self.__api_date_format__,
                        )

                        data = self._request_data(
                            url=fetch_url,
                            params={
                                "startDate": start_date,
                                "endDate": end_date,
                            },
                            headers={"Authorization": f"Bearer {self._auth_token}"},
                        )
                        add_to_update = True
                except possible_errors as err:
                    self._th_logger.error(
                        f"Cannot fetch data for '{mtr_hr}' due to the error '{err}'."
                        " Skipping."
                    )
                    continue

                file_info = DataFile(
                    file_name=filename,
                    bucket=self._config.extra.raw.bucket,
                    path=self._config.extra.raw.path,
                    body=dumps(data, indent=4, sort_keys=True),
                    meters=mtr_cfgs,
                )
                file_info.timestamps.put(mtr_hr)

                self._fetched_files_queue.put(file_info)
                self._shadow_fetched_files_queue.put(file_info)
                if add_to_update:
                    self._add_to_update(file_info, self._fetch_update_file_buffer)

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            [
                (self.fetch_consumer, [require_client()]),
                (self.save_fetched_files_worker, []),
            ],
            run_parallel=RUN_FETCH_PARALLEL
        )
        self.finalize_fetch_update_status()
        self._run_consumers(
            [
                (self.save_fetch_status_worker, []),
            ],
            run_parallel=RUN_FETCH_PARALLEL
        )
        self._logger.info("Fetching has been done.")

    # TODO: should be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class StandrdizeWorker(BaseStandardizeWorker):
    """Willow Standardization Worker."""

    __created_by__ = "Willow Connector"
    __description__ = "Willow Integration"
    __name__ = "Willow Standardize Worker"

    @staticmethod
    def date_repr(dt_str: str):
        """Get date string reprezentation and timestamp"""
        data = parse(dt_str, tz_info="UTC")
        data_timestamp = int(data.timestamp())
        data_hr_str = format_date(
            truncate(data, level="hour"), CFG.PROCESSING_DATE_FORMAT
        )
        return data_hr_str, data_timestamp

    def _get_data_df(self, data: Dict) -> DataFrame:
        raw_df = DataFrame.from_dict(data.get("data", []))
        raw_df.rename(columns={"timestamp": "date"}, inplace=True)
        raw_df["value"] = raw_df["value"].astype(float)
        raw_df[["hour", "timestamp"]] = list(map(self.date_repr, raw_df.date))
        raw_df.sort_values(by=["timestamp"], inplace=True, ascending=False)
        res = raw_df.groupby("hour")["value"].agg(["min", "max"])
        raw_df = raw_df.merge(res, how="left", on="hour")
        raw_df = raw_df.reset_index()
        raw_df["consumption"] = raw_df["max"] - raw_df["min"]
        return raw_df

    def _standardize_electric(
        self, data_df: DataFrame, mtr_date: DateTime, mtr_cfg: Dict
    ) -> Meter:
        """Standardize electric value"""
        if not bool(len(data_df)):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        start_date = truncate(mtr_date, level="hour")
        end_date = start_date.add(minutes=59, seconds=59)

        mtr_df = data_df[
            data_df.hour == format_date(mtr_date, CFG.PROCESSING_DATE_FORMAT)
        ]

        if not bool(len(mtr_df)):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        def _getter(xdata: DataFrame) -> Optional[str]:
            return xdata["consumption"].values[0], start_date, end_date

        return self._standardize_generic(mtr_df, mtr_cfg, _getter)

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""

        if not raw_file_obj.body:
            self._th_logger.error("Recived empty standardize data. Skipping")
            return []

        json_data = loads(raw_file_obj.body)
        # TODO: Redesign worker to process all meter points in parallel
        standardized_files = []

        raw_data = self._get_data_df(json_data)
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
                        data_df=raw_data,
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
            run_parallel=RUN_STANDARDIZE_PARALLEL
        )
        self.finalize_standardize_update_status()
        self._run_consumers(
            [
                (self.save_standardize_status_worker, [])
            ],
            run_parallel=RUN_STANDARDIZE_PARALLEL
        )
