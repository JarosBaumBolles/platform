""" Density Workers module"""

import binascii
import hashlib
import hmac
import time
import urllib
import urllib.parse
import uuid
from base64 import b64encode
from collections import Counter
from http import HTTPStatus
from json import dumps, loads
from queue import Queue
from threading import Lock
from typing import Any, List, Optional, Tuple, Dict

import requests
from dataclass_factory import Factory, Schema
from pandas import DataFrame
from pendulum import DateTime

from common import settings as CFG
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker

from expiringdict import ExpiringDict
import requests
from common.thread_pool_executor import run_thread_pool_executor
from common.bucket_helpers import (
    file_exists, 
    get_missed_standardized_files, 
    require_client
)
from google.cloud.storage import Client
from common.logging import Logger, ThreadPoolExecutorLogger
import uuid
import common.settings as CFG
from math import floor
import requests
from integration.density.exception import (
    LoadFromConnectorAPI, 
    EmptyResponse,
    EmptyDataInterruption
)
from integration.density.data_structture import DataFile, StandardizedFile


# TODO: @todo Potensial candidate to be in base class. The same code is in Openweather
class GapsDetectionWorker(BaseFetchWorker):
    """Density get missed hours worker functionality"""
    __created_by__ = "Density Missed Hours Worker"
    __description__ = "Density Integration"
    __name__ = "Density Missed Hours Worker"
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
                for hr in mtr_msd_poll_hrs:
                    try:
                        self._missed_hours_cache[hr].put(mtr_cfg)
                    except KeyError:
                        self._missed_hours_cache[hr] = Queue()
                        self._missed_hours_cache[hr].put(mtr_cfg)

                self._th_logger.info(
                    f"Found {len(mtr_msd_poll_hrs)} in '{mtr_cfg.meter_name}' "
                    "meter.",
                )            
            else:
                self._th_logger.info(
                    f"Meter {mtr_cfg.meter_name} is up to date.",
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


class FetchWorker(BaseFetchWorker):
    """Density fetch worker functionality"""

    __created_by__ = "Density Fetch Connector"
    __description__ = "Density Integration"
    __name__ = "Density Fetch Worker"

    __fetch_url__ = "https://api.density.io/v2/spaces/{}/counts"
    __density_api_date_format__ = "YYYY-MM-DD[T]HH:mm:ss[Z]"
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
            default_schema=Schema(
                trim_trailing_underscore=False, 
                skip_internal=False
            )
        )

    # TODO: @todo MUST be moved to base class after rirst relise
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
            client=storage_client
        )

        if not data:
            raise LoadFromConnectorAPI(
                f"The raw file 'gs://{self._config.extra.raw.bucket}/"
                f"{self._config.extra.raw.path}/{filename}'"
                "is empty. Skipping"
            )
        return data   

    # TODO: @todo Potential candidate to be in the base class
    # The similar code used in OPenweather api
    def _request_data(
        self, 
        url: str, 
        params: dict, 
        headers: str
    ) -> Optional[Dict]:
        result, retry_count, delay = None, 0, 0.5
        data = None
        while retry_count < self.__max_retry_count__:
            try:
                result = requests.get(
                    url,
                    params=params,
                    headers=headers,
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

    # TODO: @todo THe same about miving to base class 
    def fetch_consumer(
        self, 
        storage_client: Client,        
        logs: Queue, 
        worker_idx: str
    ) -> None:  
        if not len(self._missed_hours_queue):
            self._th_logger.warning(
                "Missed hours queue is empty. Maybe data up to date.",
            )
            return None            
        empty_run_count = 0
        possible_errors = (requests.exceptions.JSONDecodeError,)
        fetch_url = self.__fetch_url__.format(self._config.space_id)

        while True:
            if not len(self._missed_hours_queue):
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
            else:
                mtr_hr, mtr_cfgs = self._missed_hours_queue.popitem()

                start_date = truncate(parse(mtr_hr), level="hour")    

                filename = format_date(start_date, CFG.PROCESSING_DATE_FORMAT)
                try:
                    try:
                        data = self._load_from_file(
                            filename=filename, 
                            storage_client=storage_client, 
                            logs=logs
                        )
                        add_to_update = False
                    except LoadFromConnectorAPI as err:
                        self._th_logger.info(
                            f"Canot load the local file due to the reason '{err}'"
                            " Loading from the WatTime API"
                        )
                        data = self._request_data(
                            url=fetch_url,
                            params={
                                "start_time": format_date(
                                    start_date,
                                    self.__density_api_date_format__
                                ),
                                "end_time": format_date(
                                    start_date.add(minutes=59, seconds=59),
                                    self.__density_api_date_format__
                                ),
                            },
                            headers={
                                "Authorization": f"Bearer {self._config.token}"
                            },
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


class StandrdizeWorker(BaseStandardizeWorker):
    """Density Standardization Worker."""
    __created_by__ = "Density Connector"
    __description__ = "Density Integration"
    __name__ = "Density Standardize Worker"


    def _standardize_occupancy(
        self, 
        data: Dict, 
        mtr_date: DateTime,        
        mtr_cfg: Dict
    ) -> Meter:
        """Standardize occupancy value"""
        if not data:
            raise EmptyRawData(
                f"Cannot standardize '{mtr_cfg.meter_name}' in point '{mtr_date}'"
                " due to the empty data"
            )

        start_date = truncate(mtr_date, level="hour")
        end_date = start_date.add(minutes=59, seconds=59)          

        def _getter(xdata):
            payload = xdata.get("results", [])

            if not payload:
                raise EmptyDataInterruption(
                    f"Cannot standardize '{mtr_cfg.meter_name}' in point '{mtr_date}'"
                    " due to the empty data"
                )
            return payload[-1]["count"], start_date, end_date
        return self._standardize_generic(data, mtr_cfg, _getter)

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""

        if not raw_file_obj.body:
            self._th_logger.error(
                "Recieved empty standardize data. Skipping"
            )
            return []

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
                    meter = stndrdz_func(
                        data=json_data,
                        mtr_date=mtr_hr,
                        mtr_cfg=mtr_cfg,
                    )
                except EmptyDataInterruption as err:
                    self._th_logger.error(
                        f"Cannot standardize meter {mtr.cfg.meter_name}"
                        f" point '{mtr_hr}' due to the error '{err}'"
                    )
                    continue

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
                        cfg=mtr_cfg
                    )
                )
                raw_file_obj.meters.task_done()
            raw_file_obj.timestamps.task_done()
        return standardized_files

    # TODO: @todo Possible candite to be in base class. Or boiler plate code   
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