""" WatTime Workers module"""
import time
from abc import abstractmethod
from collections import Counter
from http import HTTPStatus
from json import dumps, loads
from queue import Queue
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests
from pendulum import DateTime, Period
from requests.auth import HTTPBasicAuth

from common import settings as CFG
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate
from integration.base_integration import (
    BaseFetchWorker,
    BaseStandardizeWorker,
    EmptyRawFile,
)
from integration.wattime.data import DataFile, StandardizedFile
from integration.wattime.exceptions import AuthtorizeException, EmptyResponse
from expiringdict import ExpiringDict
from google.cloud.storage import Client
import uuid
from common.logging import Logger, ThreadPoolExecutorLogger
from common.bucket_helpers import require_client
from common.bucket_helpers import get_missed_standardized_files
from dataclass_factory import Factory, Schema
from integration.wattime.exceptions import LoadFromWattime
from collections import defaultdict



class MarginalEmGapsDetectionWorker(BaseFetchWorker):
    """Wattime Marginal get missed hours worker functionality"""
    __created_by__ = "Wattime Marginal Missed Hours Worker"
    __description__ = "Wattime Marginal Integration"
    __name__ = "Wattime Marginal Missed Hours Worker"
    __max_idle_run_count__ = 5
    
    def __init__(
        self,
        missed_hours_cache: ExpiringDict,
        config: Any,
    ) -> None:
        self._missed_hours_cache = missed_hours_cache
        self._config = config
        self._meters_queue = Queue()
        self._trace_id: str = uuid.uuid4()
        self._logger = Logger(
            description=self.__description__, 
            trace_id=self._trace_id
        )        
        self._th_logger = ThreadPoolExecutorLogger(
            description=self.__description__, 
            trace_id=self._trace_id
        )        

    def configure(self) -> None:
        self._clear_queue(self._meters_queue)
        self._missed_hours_cache.clear()
        for mtr_cfg in self._config.meters:
            self._meters_queue.put(mtr_cfg)        


    def missed_hours_consumer(
        self,
        storage_client: Client,
        logs: Queue,
        worker_idx: str
    ) -> None:
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
                client=storage_client
            )       
            if mtr_msd_poll_hrs:
                # missed_hours.put((mtr_cfg, mtr_msd_poll_hrs))
                for hr in mtr_msd_poll_hrs:
                    try:
                        self._missed_hours_cache[hr].put(mtr_cfg)
                    except KeyError:
                        self._missed_hours_cache[hr] = Queue()
                        self._missed_hours_cache[hr].put(mtr_cfg)

                self._th_logger.info(
                    f"Found {len(mtr_msd_poll_hrs)} in '{mtr_cfg.meter_name}' "
                    "meter."
                )            
            else:
                self._th_logger.info(f"Meter {mtr_cfg.meter_name} is up to date.")
            self._meters_queue.task_done()        


    def run(self) -> None:
        self.configure()
        self._run_consumers(
            (
                self.missed_hours_consumer,
                [require_client()]
            )
        )

class WatTimeBaseFetchWorker(BaseFetchWorker):
    """Wattime base worker functionality"""

    __description__ = "Wattime Marginal Emissions"
    __auth_url__ = "https://api2.watttime.org/v2/login"
    __fetch_url__ = "https://api2.watttime.org/v2/"
    __request_timeout__ = 60

    __max_retry_count__ = 3
    __retry_delay__ = 0.5

    def authorize(self) -> str:
        """Retrive Wattime token"""
        if not all(x for x in (self._config.username, self._config.password)):
            raise AuthtorizeException(
                "Configuration is not run or provided empty credential"
            )
        retry_count, delay = 0, 1

        while retry_count < self.__max_retry_count__:
            try:
                resp = requests.get(
                    self.__auth_url__,
                    auth=HTTPBasicAuth(self._config.username, self._config.password),
                    timeout=self.__request_timeout__,
                )
                if resp.status_code == HTTPStatus.OK.value:
                    token = resp.json().get("token")
                    break
                raise AuthtorizeException(
                    f"Autorization failed with server status `{resp.status_code}`"
                    f" and response `{resp.text}`."
                )
            except (requests.ConnectionError, requests.ConnectTimeout):
                retry_count += 1
                delay = retry_count * __retry_delay__
                time.sleep(delay)
        return token

    def _request_data(
        self, 
        url: str, 
        params: dict, 
        token: Optional[str]
    ) -> Tuple:
        retry_count, delay = 0, 0.5
        auth_errors = (
            HTTPStatus.UNAUTHORIZED.value,
            HTTPStatus.FORBIDDEN.value,
        )
        result = None
        # From requests version 2.26 url params encoding is forced without
        # ablity to prevent. See more details at
        # https://github.com/psf/requests/issues/5964#issuecomment-949013046
        # At the same time Wattime doesn't support encoded parameters in a queue
        # So we must to build full url by self

        query = ""
        if params:
            query_params = list(map(lambda x: f"{x[0]}={x[1]}", params.items()))
            query = "&".join(query_params)

        full_url = f"{url}?{query}" if query else url
        data = {}
        while retry_count < self.__max_retry_count__:
            try:
                result = requests.get(
                    full_url,
                    params={},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=self.__request_timeout__,
                )
                self._th_logger.debug(
                    "Recieved response with empty data. "
                    f"Response status code is {result.status_code}. "
                    f"Response message is {result.text}. Response parameters: - "
                    f"{params}; "
                )            
                if result.status_code == HTTPStatus.OK.value:
                    data = result.json()
                    break
                if result.status_code in auth_errors:
                    token = self.authorize()
                    retry_count += 1
                    delay = retry_count
                    time.sleep(delay)
            except (requests.ConnectionError, requests.ConnectTimeout):
                retry_count += 1
                delay = retry_count
                time.sleep(delay)
        
        if not data or result.status_code != HTTPStatus.OK.value:
            raise EmptyResponse(
                "Cannot run request corectly"
                f"Response status code is {result.status_code}. "
                f"Response message is {result.text}. Response parameters: - "
                f"{params}; "          
            )
            
        return data, token

    def _load_from_file(
        self, 
        filename: str, 
        storage_client: Client, 
        logs: Queue
    ) -> Optional[Dict]:
        fl_exists = self._is_file_exists(
            client=storage_client,
            bucket=self._config.extra.raw.bucket,
            path=self._config.extra.raw.path,
            file_name=filename,
        )
        if not fl_exists:
            raise LoadFromWattime(
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
            client=storage_client
        )

        if not data:
            raise LoadFromWattime(
                f"The raw file 'gs://{self._config.extra.raw.bucket}/"
                f"{self._config.extra.raw.path}/{filename}'"
                "is empty. Skipping"
            )
        return data

    @abstractmethod
    def run_fetch_worker(self, logs: Queue, worker_idx: int) -> None:
        """Run fetch worker"""


class WatTimeBaseStandrdizeWorker(BaseStandardizeWorker):
    """Wattime base standardize worker functionality"""

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

                try:
                    meter = stndrdz_func(json_data, mtr_hr, mtr_cfg)
                    if meter:
                        standardized_files.append(
                            StandardizedFile(
                                file_name=format_date(
                                    mtr_hr, 
                                    CFG.PROCESSING_DATE_FORMAT
                                ),
                                bucket=mtr_cfg.standardized.bucket,
                                path=mtr_cfg.standardized.path,
                                meter=meter,
                                body=meter.as_str(),
                                cfg=mtr_cfg,
                            )
                        )                     
                except EmptyRawFile:
                    raise EmptyRawFile( # pylint:disable=raise-missing-from
                        f"Detected empty body in the RawFile 'gs://{mtr_cfg.bucket}/'"
                        f"{mtr_cfg.path}/{raw_file_obj.file_name}"
                    )
                raw_file_obj.meters.task_done()    
            raw_file_obj.timestamps.task_done()
        return standardized_files  

    def run(self, run_time: DateTime) -> None:
        self.configure(run_time)

        self._run_consumers(
            [
                (self.run_standardize_worker, []),
                (self.save_standardized_files_worker, [])
            ]
        )
        self.finalize_standardize_update_status()
        self._run_consumers([(self.save_standardize_status_worker, [])])

class MarginalEmFetchWorker(WatTimeBaseFetchWorker):
    """Wattime Marginal Emmision Fetch Worker"""

    __created_by__ = "Wattime Marginal Emissions Fetch Connector"
    __description__ = "Wattime Marginal Emissions Fetch"
    __name__ = "Wattime Marginal Emissions Fetch Worker"

    __fetch_url__ = "https://api2.watttime.org/v2/data"

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

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )  

    def fetch_consumer(
        self, 
        storage_client: Client,
        logs: Queue, 
        worker_idx: str
    ) -> None:  
        if not len(self._missed_hours_queue):
            self._th_logger.warning(
                "Missed hours queue is empty. Maybe data up to date."
            )
            return None            
        empty_run_count = 0
        possible_errors = (
            AuthtorizeException, 
            EmptyResponse, 
            requests.exceptions.JSONDecodeError
        ) 
        try:
            token = self.authorize()
        except AuthtorizeException:
            self._th_logger.error(
                f"Cannot fetch data due to the error '{err}'. Exit."
            )            
            return None

        while True:
            if not len(self._missed_hours_queue):
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
            else:
                mtr_hr, mtr_cfgs = self._missed_hours_queue.popitem()
                filename = format_date(mtr_hr, CFG.PROCESSING_DATE_FORMAT)
                add_to_update = False, 
                try:
                    try:
                        data = self._load_from_file(
                            filename=filename, 
                            storage_client=storage_client, 
                            logs=logs
                        )
                        add_to_update = False
                    except LoadFromWattime as err:
                        self._th_logger.info(
                            f"Canot load the local file due to the reason '{err}'"
                            " Loading from th eWatTime API"
                        )
                        dt = format_date(
                            mtr_hr, 
                            CFG.PROCESSING_DATE_FORMAT
                        )
                        data, token = self._request_data(
                            url=self.__fetch_url__,
                            params={
                                "ba": self._config.grid_regions_name,
                                "starttime": dt,
                                "endtime": dt,
                            },
                            token=token,
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
        self.configure(run_time)
        self._run_consumers(
            [
                (
                    self.fetch_consumer, 
                    [require_client()]
                ),
                (self.save_fetched_files_worker, [])
            ]
        )
        self.finalize_fetch_update_status()
        self._run_consumers([(self.save_fetch_status_worker, []),])
        self._logger.info("Fetching has been done.")

class MarginalEmStandrdizeWorker(WatTimeBaseStandrdizeWorker):
    """Wattime Standardization Worker."""
    __created_by__ = "Wattime Marginal Emissions Connector"
    __description__ = "Wattime Integration"
    __name__ = "Wattime Marginal Emissions Connector "

    def _standardize_marginal_grid_emissions(
        self, 
        data: Dict, 
        mtr_date: DateTime, 
        mtr_cfg: Any
    ) -> Meter:
        """Standardize Marginal Grid Emissions value"""

        if not data:
            raise EmptyRawFile("Empty data")
        start_date = mtr_date
        end_date = start_date.add(minutes=59, seconds=59)

        def _getter(xdata: List[Dict[str, Any]]) -> Optional[str]:
            return xdata[0]["value"], start_date, end_date

        return self._standardize_generic(data, mtr_cfg, _getter)

    

class AverageEmGapsDetectionWorker(BaseFetchWorker):
    """Openweather get missed hours worker functionality"""
    __created_by__ = "OpenWeather Missed Hours Worker"
    __description__ = "OpenWeather Integration"
    __name__ = "OpenWeather Missed Hours Worker"
    __max_idle_run_count__ = 5
    __default_delay_hours__ = 12
    
    def __init__(
        self,
        missed_hours_cache: ExpiringDict,
        config: Any,
    ) -> None:
        self._missed_hours_cache = missed_hours_cache
        self._config = config
        self._meters_queue = Queue()
        self._trace_id: str = uuid.uuid4()
        self._logger = Logger(
            description=self.__description__, 
            trace_id=self._trace_id
        )        
        self._th_logger = ThreadPoolExecutorLogger(
            description=self.__description__, 
            trace_id=self._trace_id
        )        

    def configure(self) -> None:
        self._clear_queue(self._meters_queue)
        self._missed_hours_cache.clear()
        for mtr_cfg in self._config.meters:
            self._meters_queue.put(mtr_cfg)      

    def missed_hours_consumer(
        self,
        storage_client: Client,
        logs: Queue,
        worker_idx: str
    ) -> None:
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
            mtr_msd_poll_hrs = sorted (
                get_missed_standardized_files(
                    bucket_name=mtr_cfg.standardized.bucket,
                    bucket_path=mtr_cfg.standardized.path,
                    range_hours=self._config.gap_regeneration_window,
                    client=storage_client
                )
            )     
            if not mtr_msd_poll_hrs:
                self._th_logger.info(f"Meter {mtr_cfg.meter_name} is up to date.")
                continue

            delta = mtr_msd_poll_hrs[-1] - mtr_msd_poll_hrs[0]
            if delta.in_hours() <= self.__default_delay_hours__:
                self._th_logger.warning(
                    f"The delay {delta.in_hours()} hours is less or equal to "
                    f"minimal endpoint delay {self.__default_delay_hours__}"
                    f" hours. Skipping."
                )
                continue            


            # Group range by month to make one wattime call with month range maximum
            missed_hrs_index = defaultdict(list)
            for hour in mtr_msd_poll_hrs:
                missed_hrs_index[hour.month].append(hour)

            for index, hours in missed_hrs_index.items():
                if len(hours) == 1:
                    key = tuple(hour[0], hours[0].add(hours=1))
                else:
                    key = tuple(sorted(hours))
            
                self._missed_hours_cache.setdefault(key, Queue())
                self._missed_hours_cache[key].put(mtr_cfg)

            self._th_logger.info(
                f"Found {len(mtr_msd_poll_hrs)} in '{mtr_cfg.meter_name}' meter.",
            )            
                
            self._meters_queue.task_done()

    def run(self) -> None:
        self.configure()
        self._run_consumers(
            (
                self.missed_hours_consumer,
                [require_client()]
            )
        )                  

class AverageEmFetchWorker(WatTimeBaseFetchWorker):
    """Wattime Marginal Emmision Fetch Worker

    In accordance with documentation
    https://averageemissions.docs.apiary.io/#reference/0/average-emissions-data

    Obtain historical average emissions for a given region.
    The data is published shortly after the source data becomes available.
    This is typically within an hour in the US but may occur overnight in
    some cases. It is sometimes longer in Europe, for example up to a week
    in Sweden.Use the standard WattTime /login endpoint to obtain an access
    token.We recommend pulling no more than a month of data per query.

    Obtain historical average emissions for a given region. The most
    recent data provided is 12 or more hours old - we do not provide
    realtime average emissions data.
    Based on real behavior we cannot determine actual data range,
    because delay could be various from call to call.

    Summing up to determaine missed hours range we should to run the
    following steps:
        1. Use standard previous missed hour range.
        2. Apply logic to check if minimum delta between end and start data
        is greater than 12 hours
        3. Split miised hours in data ranges not more than 1 month
        5. Fetch al miised point
        6. Save all fetched pints in separate raw file
        7. Standardize all fetched raw files using usual approach
    """

    __created_by__ = "Wattime Average Emissions Fetch Connector"
    __description__ = "Wattime Average Emissions Fetch"
    __name__ = "Wattime Average Emissions Fetch Worker"

    __fetch_url__ = "https://api2.watttime.org/v2/avgemissions"

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

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )  
        self._fetch_counter = Counter()
        self._fetch_lock: Lock = Lock()

        self._base_filename: Optional[str] = None

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time=run_time)
        self._fetch_counter.clear()
        self._base_filename = format_date(
            self._run_time,
            CFG.PROCESSING_DATE_FORMAT
        )

    def fetch_consumer(self, logs: Queue, worker_idx: str) -> None:  
        if not len(self._missed_hours_queue):
            self._th_logger.warning(
                "Missed hours queue is empty. Maybe data up to date."
            )
            return None            
        empty_run_count = 0
        possible_errors = (
            AuthtorizeException, 
            EmptyResponse, 
            requests.exceptions.JSONDecodeError
        ) 
        try:
            token = self.authorize()
        except AuthtorizeException:
            self._th_logger.error(
                f"Cannot fetch data due to the error '{err}'. Exit."
            )            
            return None
        while True:
            if not len(self._missed_hours_queue):
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
            else:
                mtr_hours, mtr_cfgs = self._missed_hours_queue.popitem()
                mtr_cfg_list = []
                # TODO: Should Be redesigned to use threadsafety cached approach                
                while not mtr_cfgs.empty():
                    mtr_cfg_list.append(mtr_cfgs.get())
                    mtr_cfgs.task_done()
                try:
                    start_date = format_date(
                        mtr_hours[0], CFG.PROCESSING_DATE_FORMAT
                    )
                    end_date = format_date(
                        mtr_hours[-1], CFG.PROCESSING_DATE_FORMAT
                    )  

                    data, token = self._request_data(
                        url=self.__fetch_url__,
                        params={
                            "ba": self._config.grid_regions_name,
                            "starttime": start_date,
                            "endtime": end_date,
                        },
                        token=token,
                    )
                    add_to_update = True
                except possible_errors as err:
                    self._th_logger.error(
                        f"Cannot fetch data for '{mtr_hr}' due to the error '{err}'."
                        " Skipping."
                    )
                    continue   

                for point in data:
                    point_data = truncate(
                        parse(point.get("point_time")), 
                        level="hour"
                    )
                    if point_data not in mtr_hours:
                        self._th_logger.error(
                            f"Found excess date in '{point_data}' hour in response."
                            " Skipping."
                        )
                        continue
                    with self._lock:
                        filename = self.__fetch_file_name_tmpl__.format(
                            base_file_name=self._base_filename,
                            idx=self._fetch_counter["file_id"],
                        )
                        self._fetch_counter["file_id"] += 1

                    file_info = DataFile(
                        file_name=filename,
                        bucket=self._config.extra.raw.bucket,
                        path=self._config.extra.raw.path,
                        body=dumps([point], indent=4, sort_keys=True),
                    )
                    file_info.timestamps.put(point_data)
                    for cfg in mtr_cfg_list:
                        file_info.meters.put(cfg)

                    self._fetched_files_queue.put(file_info)
                    self._shadow_fetched_files_queue.put(file_info)
                    self._add_to_update(file_info, self._fetch_update_file_buffer)
                                                         
    def run(self, run_time: DateTime) -> None:
        self.configure(run_time)
        self._run_consumers(
            [
                (self.fetch_consumer, []),
                (self.save_fetched_files_worker, [])
            ]
        )
        self.finalize_fetch_update_status()
        self._run_consumers([(self.save_fetch_status_worker, []),])
        self._logger.info("Fetching has been done.")


class AverageEmStandrdizeWorker(WatTimeBaseStandrdizeWorker):
    """Wattime Standardization Worker."""
    __created_by__ = "Wattime Marginal Emissions Connector"
    __description__ = "Wattime Integration"
    __name__ = "Wattime Marginal Emissions Connector "

    def _standardize_average_grid_emissions(
        self, 
        data: Dict, 
        mtr_date: DateTime, 
        mtr_cfg: Any
    ) -> Meter:
        """Standardize Marginal Grid Emissions value"""

        if not data:
            raise EmptyRawFile("Empty data")
        start_date = mtr_date
        end_date = start_date.add(minutes=59, seconds=59)

        def _getter(xdata: List[Dict[str, Any]]) -> Optional[str]:
            return xdata[0]["value"], start_date, end_date

        return self._standardize_generic(data, mtr_cfg, _getter)
