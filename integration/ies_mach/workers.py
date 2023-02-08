""" Ies Mach Workers module"""

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
from pandas import DataFrame, concat
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
from integration.irisys.data_structures import DataFile, StandardizedFile
from integration.irisys.exceptions import (
    EmptyResponse,
    LoadFromConnectorAPI,
    EmptyDataInterruption
)


# TODO: @todo Must be moved to a base class
class GapsDetectionWorker(BaseFetchWorker):
    """IES Mach get missed hours worker functionality"""
    __created_by__ = "IES Mach Missed Hours Worker"
    __description__ = "IES Mach Integration"
    __name__ = "IES Mach Missed Hours Worker"
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
    """Irisys fetch worker functionality"""

    __created_by__ = "Irisys Fetch Connector"
    __description__ = "Irisys Integration"
    __name__ = "Irisys Fetch Worker"

    __fetch_url__ = "https://api.iesmach.com/v1/intervals/query.json"
    __time_zone__ = "BuildingLocal"
    __api_date_format__ = "YYYY-MM-DDTHH:mm"

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
        self._auth_token: Optional[str] = ""

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, 
            skip_internal=False)
        )

    # TODO: @todo MUt be moved in base class/ The same code ia in Willow
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
    # The similar code used in Openweather and Willow api
    def _request_data(
        self, 
        url: str, 
        json_params: dict, 
        headers: str
    ) -> Optional[Dict]:
        result, retry_count, delay = None, 0, 0.5
        data = None
        while retry_count < self.__max_retry_count__:
            try:
                result = requests.post(
                    url,
                    json=json_params,
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
                "Cannot run request corectly. "
                f"Response status code is {result.status_code}. "
                f"Response message is '{result.text}'. Response parameters: - "
                f"{params}; "          
            )
        return data   

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
        possible_errors = (
            EmptyResponse, 
            requests.exceptions.JSONDecodeError
        )

        while True:
            if not len(self._missed_hours_queue):
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
            else:
                mtr_hr, mtr_cfgs = self._missed_hours_queue.popitem()
                filename = format_date(mtr_hr, CFG.PROCESSING_DATE_FORMAT)
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
                            " Loading from the IES Mach API"
                        )
                        start_date = truncate(parse(mtr_hr), level="hour")
                        end_date = start_date.add(minutes=59, seconds=59)

                        data = self._request_data(
                            url=self.__fetch_url__,
                            json_params={
                                "start": format_date(
                                    start_date, 
                                    self.__api_date_format__
                                ),
                                "end": format_date(
                                    end_date, self.__api_date_format__
                                ),
                                "responseTimeZone": self.__time_zone__,
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
    """IES Mach Standardization Worker."""
    __created_by__ = "IES Mach Connector"
    __description__ = "IES Mach Integration"
    __name__ = "IES Mach Standardize Worker"

    @staticmethod
    def date_repr(row):
        """Get date string reprezentation and timestamp"""
        data = parse(row.datetime, tz_info="UTC")
        data_hr =  truncate(
            data,
            level="hour"
        )
        row["timestamp"] = int(data.timestamp())
        row["hour"] = format_date(data_hr, CFG.PROCESSING_DATE_FORMAT)
        return row

    def _get_data_df(self, data: List) -> DataFrame:
        if not data:
            raise EmptyDataInterruption(
                f"Recieved Empty Data"
            )

        dt_fs = list(
            map(
                lambda x: DataFrame.from_dict(x.get("intervals", {})), 
                data
            )            
        )

        if len(dt_fs) == 1:
            raw_df = dt_fs[0]
        else:
            raw_df = concat(
                map(
                    lambda x: DataFrame.from_dict(x.get("intervals", {})), 
                    data
                )
            )
        if not len(raw_df):
            raise EmptyDataInterruption(
                f"Can not load the data '{data}' in a Dataframe"
            )            
        raw_df = raw_df.apply(self.date_repr, axis=1)
        raw_df.sort_values(by=["timestamp"], inplace=True, ascending=False)

        return raw_df      

    def _standardize_occupancy(
        self, 
        data_df: DataFrame, 
        mtr_date: DateTime,
        mtr_cfg: Dict
    ) -> Meter:
        """Standardize electric value"""  
        if not len(data_df):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        start_date = truncate(mtr_date, level="hour")
        end_date = start_date.add(minutes=59, seconds=59) 

        mtr_hr = format_date(start_date, CFG.PROCESSING_DATE_FORMAT)

        # fiter dataframe to operate only with relatad data
        mtr_df = data_df[data_df.hour == mtr_hr]

        max_timestamp = mtr_df.timestamp.max()

        data_row = mtr_df[mtr_df.timestamp == max_timestamp].to_dict("records")

        if not len(data_row):
            raise EmptyDataInterruption(
                f"Recieved Empty Data for the meter point {mtr_date}"
            )

        def _getter(xdata: List[Dict[str, Any]]) -> Optional[str]:
            return xdata[0]["value"], start_date, end_date

        return self._standardize_generic(data_row, mtr_cfg, _getter)            

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""

        if not raw_file_obj.body:
            self._th_logger.error(
                "Recived empty standardize data. Skipping"
            )
            return []

        json_data = loads(raw_file_obj.body)

        # TODO: Redesign worker to process all meter points in parallel
        standardized_files = []    
        try:        
            raw_data = self._get_data_df(json_data)
        except EmptyDataInterruption as err:
            self._th_logger.error(
                f"Can not load meter data to a Dataframe due to the err '{err}'"
            )
            return []

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