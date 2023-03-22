""" OpenWeather Workers module"""

import uuid
from abc import abstractmethod
from collections import Counter
from http import HTTPStatus
from json import dumps, loads
from math import floor
from queue import Queue
from time import time
from typing import Any, List, Optional

import requests
from dataclass_factory import Factory, Schema
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from pandas import DataFrame
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import (
    file_exists,
    get_missed_standardized_files,
    require_client,
)
from common.data_representation.standardized.meter import Meter
from common.date_utils import GapDatePeriod, format_date, parse, truncate
from common.logging import Logger, ThreadPoolExecutorLogger
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.openweather.data_structures import DataFile, StandardizedFile

RUN_GAPS_PARALLEL = True
RUN_FETCH_PARALLEL = True
RUN_STANDARDIZE_PARALLEL = True


class GapsDetectionWorker(BaseFetchWorker):
    """Openweather get missed hours worker functionality"""

    __created_by__ = "OpenWeather Missed Hours Worker"
    __description__ = "OpenWeather Integration"
    __name__ = "OpenWeather Missed Hours Worker"
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
                bucket_name=mtr_cfg.standardized.bucket,
                bucket_path=mtr_cfg.standardized.path,
                range_hours=self._config.gap_regeneration_window,
                client=storage_client,
                date_range=self._expected_hours,
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
                self._th_logger.info(
                    f"Meter {mtr_cfg.meter_name} is up to date.",
                )
            self._meters_queue.task_done()

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            [(self.missed_hours_consumer, [require_client()])],
            run_parallel=RUN_GAPS_PARALLEL,
        )

    # TODO: should be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class FetchWorker(BaseFetchWorker):
    """Openweather fetch worker functionality"""

    __created_by__ = "OpenWeather Fetch Connector"
    __description__ = "OpenWeather Integration"
    __name__ = "OpenWeather Fetch Worker"

    __method__ = "GET"
    __api_url__ = "https://api.openweathermap.org/data/3.0/onecall/timemachine"

    __max_retry_count__ = 3
    __retry_delay__ = 0.5
    __max_idle_run_count__ = 5
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

        self._missed_hours_queue: ExpiringDict = missed_hours

        self._raw_fetch_queue = Queue()
        self._fetch_counter = Counter()

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

    def _request_data(
        self, client: Client, filename: str, dt_time: DateTime, logs: Queue
    ) -> dict:
        result, retry_count, delay = {}, 0, self.__retry_delay__

        while retry_count < self.__max_retry_count__:

            is_file_exists = file_exists(
                client=client,
                bucket=self._config.extra.raw.bucket,
                subdirectory=self._config.extra.raw.path,
                file_name=filename,
            )

            if is_file_exists:
                self._th_logger.warning(
                    f"The File {self._config.extra.raw.bucket}/"
                    f"{self._config.extra.raw.path}/{filename} alredy exists."
                    "Loading from bucket"
                )
                return self._load_json_data(
                    client=client,
                    bucket=self._config.extra.raw.bucket,
                    path=self._config.extra.raw.path,
                    filename=filename,
                    logs=logs,
                )

            try:
                r_data = floor(dt_time.in_timezone("UTC").timestamp())
                result = requests.get(
                    self.__api_url__,
                    params={
                        "lat": self._config.city_coordinates_latitude,
                        "lon": self._config.city_coordinates_longitude,
                        "dt": r_data,
                        "appid": self._config.app_id,
                        "units": "imperial",
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=self.__request_timeout__,
                )
                if result.status_code == HTTPStatus.OK.value:
                    result = result.json()
                    break
                retry_count += 1
                delay *= retry_count
                time.sleep(delay)

            except (
                requests.ConnectionError,
                requests.ConnectTimeout,
                requests.exceptions.JSONDecodeError,
            ) as err:
                retry_count += 1
                delay *= retry_count

                self._th_logger.error(
                    f"Recieved error '{err} during requesting data.' ",
                    f"lat - {self._config.city_coordinates_latitude}; "
                    f"lon - {self._config.city_coordinates_longitude}; "
                    f"dt - {r_data}",
                )

                time.sleep(delay)

        return result

    def fetch_consumer(
        self,
        storage_client: Client,
        logs: Queue,  # pylint:disable=unused-argument
        worker_idx: str,  # pylint:disable=unused-argument
    ) -> None:
        """Fetch data Consumer"""
        if not bool(len(self._missed_hours_queue)):
            self._th_logger.warning(
                "Missed hours queue is empty. Maybe data up to date.",
            )
            return None
        empty_run_count = 0
        while True:
            if not bool(len(self._missed_hours_queue)):
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
            else:
                mtr_hr, mtr_cfgs = self._missed_hours_queue.popitem()
                filename = format_date(mtr_hr, CFG.PROCESSING_DATE_FORMAT)
                data = self._request_data(
                    client=storage_client, filename=filename, dt_time=mtr_hr, logs=logs
                )
                if not data:
                    self._th_logger.error(f"Recieved empty repose for '{mtr_hr}'.")
                    continue

                file_info = DataFile(
                    file_name=filename,
                    bucket=self._config.extra.raw.bucket,
                    path=self._config.extra.raw.path,
                    body=dumps(data, sort_keys=True, indent=4),
                    meters=mtr_cfgs,
                )
                file_info.timestamps.put(mtr_hr)

                self._fetched_files_queue.put(file_info)
                self._shadow_fetched_files_queue.put(file_info)
                self._add_to_update(file_info, self._fetch_update_file_buffer)

    def run(self, run_time: DateTime) -> None:
        """Run loop entrypoint"""
        self.configure(run_time)
        self._run_consumers(
            [
                (self.fetch_consumer, [require_client()]),
                (self.save_fetched_files_worker, []),
            ]
        )
        self.finalize_fetch_update_status()
        self._run_consumers(
            [
                (self.save_fetch_status_worker, []),
            ]
        )
        self._logger.info("Fetching has been done.")

    # TODO: Shoud be removed
    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class StandardizeWorker(BaseStandardizeWorker):
    """Openweather Standardization Worker."""

    __created_by__ = "OpenWeather Connector"
    __description__ = "OpenWeather Integration"
    __name__ = "OpenWeather Standardize Worker"

    def _standardize_middleware(
        self, meter_type: str, data: DataFrame, mtr_date: DateTime, mtr_cfg: Any
    ) -> Optional[Meter]:
        if data and data.get("data", []):
            start_date = mtr_date
            end_date = start_date.add(minutes=59, seconds=59)

            def _getter(xdata: dict) -> str:
                use = xdata.get("data", [{}])[0].get(meter_type, None)

                return use, start_date, end_date

            return self._standardize_generic(
                data=data,
                getter=_getter,
                mtr_cfg=mtr_cfg,
            )

        # TODO: Should be added exception to log empty data event
        return None

    def _standardize_ambient_temperature(
        self, data: dict, mtr_date: DateTime, mtr_cfg: dict
    ) -> Meter:
        """Standardize openweather_temperature value"""
        return self._standardize_middleware(
            meter_type="temp", data=data, mtr_date=mtr_date, mtr_cfg=mtr_cfg
        )

    def _standardize_ambient_wind_speed(
        self, data: dict, mtr_date: DateTime, mtr_cfg: dict
    ) -> Meter:
        """Standardize openweather_wind_speed value"""
        return self._standardize_middleware(
            meter_type="wind_speed", data=data, mtr_date=mtr_date, mtr_cfg=mtr_cfg
        )

    def _standardize_ambient_wind_direction(
        self, data: dict, mtr_date: DateTime, mtr_cfg: dict
    ) -> Meter:
        """Standardize openweather_wind_direction value"""
        return self._standardize_middleware(
            meter_type="wind_deg", data=data, mtr_date=mtr_date, mtr_cfg=mtr_cfg
        )

    def _standardize_ambient_humidity(
        self, data: dict, mtr_date: DateTime, mtr_cfg: dict
    ) -> Meter:
        """Standardize openweather_humidity value"""
        return self._standardize_middleware(
            meter_type="humidity", data=data, mtr_date=mtr_date, mtr_cfg=mtr_cfg
        )

    def _standardize_ambient_cloud_cover(
        self, data: dict, mtr_date: DateTime, mtr_cfg: dict
    ) -> Meter:
        """Standardize openweather_cloudiness value"""
        return self._standardize_middleware(
            meter_type="clouds", data=data, mtr_date=mtr_date, mtr_cfg=mtr_cfg
        )

    def _standardize_ambient_real_feel_temperature(
        self, data: dict, mtr_date: DateTime, mtr_cfg: dict
    ) -> Meter:
        """Standardize openweather_temperature_feels_like value"""
        return self._standardize_middleware(
            meter_type="feels_like", data=data, mtr_date=mtr_date, mtr_cfg=mtr_cfg
        )

    def _standardize_ambient_dew_point(
        self, data: dict, mtr_date: DateTime, mtr_cfg: dict
    ) -> Meter:
        """Standardize openweather_dew_point value"""
        return self._standardize_middleware(
            meter_type="dew_point", data=data, mtr_date=mtr_date, mtr_cfg=mtr_cfg
        )

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""
        json_data = loads(raw_file_obj.body)
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

                meter_hour = truncate(parse(mtr_hr), level="hour")
                meter = stndrdz_func(
                    data=json_data,
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
            ]
        )
        self.finalize_standardize_update_status()
        self._run_consumers([(self.save_standardize_status_worker, [])])
