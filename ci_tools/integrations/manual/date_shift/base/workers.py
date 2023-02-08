""" Nantum Workers module"""

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
from typing import Any, List, Optional, Tuple, Set

import requests
from dataclass_factory import Factory, Schema
from pandas import DataFrame
from pendulum import DateTime

from common import settings as CFG
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate, date_range
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.nantum.data_structures import (
    DataFile, 
    NantumResponse, 
    RawFetchFile,
    DocFetchFile,
    StandardizedFile
)
from pendulum import DateTime
from expiringdict import ExpiringDict
from google.cloud.storage import Client
from common.logging import Logger, ThreadPoolExecutorLogger
from common.bucket_helpers import (
    file_exists, 
    get_missed_standardized_files, 
    require_client
)
from common.data_representation.standardized.meter import (
    Meter as MeterValue, 
    StandardizedMeterException
)
from pathlib import Path

from ci_tools.integrations.manual.date_shift.base.data_structures import (
    RawFetchFile,
    StandardizedFile
)

class FetchWorker(BaseFetchWorker):
    """Base worker functionality"""

    __created_by__ = "Date Shift Connector"
    __description__ = "Date shift Integration"
    __name__ = "Date Shift Worker"

    __max_idle_run_count__ = 45

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
        self._meters_q = Queue()

        self._missed_hours_cache: Optional[Set] = None
        self._tmp_task_q: Queue = Queue()
        self._raw_fetch_queue = Queue()

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

    

    def configure(self, run_time: DateTime) -> None:
        super().configure(run_time=run_time)
        self._clear_queue(self._tmp_task_q)
        self._clear_queue(self._meters_q)
        self._clear_queue(self._raw_fetch_queue)

        for mtr_info in self._config.meters:
            self._meters_q.put(mtr_info)


    def get_list_meter_files_consumer(
        self,
        logs: Queue,
        worker_idx: str
    ) -> None:
        if self._meters_q.empty():
            self._th_logger.warning("Meters queue is empty.")
            return None    
        empty_run_count = 0        
        while True:
            if self._meters_q.empty():
                if empty_run_count == self.__max_idle_run_count__:
                    break
                empty_run_count += 1
                continue
            mtr_info = self._meters_q.get()

            for fl_item in Path(mtr_info.standardized.path).iterdir():
                if fl_item.is_file():
                    fl_path = fl_item.resolve()
                    fl = RawFetchFile(
                        file_name=fl_path.name,
                        file_path=fl_path.parent,                         
                    )
                    fl.mtr_cfgs.put(mtr_info)
                    self._tmp_task_q.put(fl)
            self._meters_q.task_done() 
 
    def load_meter_files_consumer(
        self,
        logs: Queue,
        worker_idx: str
    ) -> None:
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
            mtr_info = self._tmp_task_q.get()
            while not mtr_info.mtr_cfgs.empty():
                mtr_cfg = mtr_info.mtr_cfgs.get()
                meter = MeterValue()
                meter.read_from_file(
                    str(
                        mtr_info.file_path.joinpath(mtr_info.file_name)
                    )
                )
                mtr = StandardizedFile(
                    file_name=mtr_info.file_name,
                    path=mtr_info.file_path,
                    meter=meter,
                    cfg=mtr_cfg            
                )

                self._fetched_files_queue.put(mtr)                
                mtr_info.mtr_cfgs.task_done()
            self._tmp_task_q.task_done()


    def run(self, run_time: DateTime) -> None:
        self.configure(run_time)

        self._run_consumers(
            [
                (self.get_list_meter_files_consumer, []),
                (self.load_meter_files_consumer, []),
            ]
        )

        self._logger.info("Fetching has been done.")


class StandrdizeWorker(BaseStandardizeWorker):
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
            default_schema=Schema(
                trim_trailing_underscore=False, 
                skip_internal=False
            )
        )


    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""
        # json_data = loads(raw_file_obj.body)
        # TODO: Redesign worker to process all meter points in parallel

        mtr = raw_file_obj.meter
        cfg = raw_file_obj.cfg
        
        start_date = mtr.start_time
        
        fl_date = parse(raw_file_obj.file_name)

        if fl_date != start_date:
            return []
        
        time_shift = self._config.timestamp_shift.get("shift", None)
        kwargs = self._config.timestamp_shift.get("shift_hours", {})
        if time_shift in ("add", "subtruct"):
            if kwargs and any(kwargs.values()):
                if time_shift == "add":
                    start_date = start_date.add(**kwargs)
                else:
                    start_date = start_date.subtract(**kwargs)

        mtr.start_time = start_date
        mtr.end_time = start_date.add(minutes=59, seconds=59)

        return [
                StandardizedFile(
                file_name=raw_file_obj.file_name,
                path=str(raw_file_obj.path),
                bucket="",
                meter=mtr,
                body=mtr.as_str().decode("utf-8"),
                cfg=cfg
            )
        ]

        return standardized_files
    # =========================================================================
    def _save_files_worker(
        self,
        files_queue: Queue,
        logs: Queue,
        worker_idx: str,
    ) -> None:
        if files_queue.empty():
            self._th_logger.warning(
                f"[Saving data]. The files queue is empty. Shutdown worker.",
            )
            return None
        empty_run_count = 0
        while True:
            if files_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                file_info = files_queue.get()
                filepath = Path(file_info.path).joinpath(file_info.file_name)

                with open(filepath, 'w', encoding="utf-8") as fl:
                    fl.write(file_info.body)

                self._th_logger.info(
                    f"Saved file '{filepath}'.",
                )
                files_queue.task_done()    
    # =========================================================================
    def run(self, run_time: DateTime) -> None:
        self.configure(run_time)

        self._run_consumers(
            [
                (self.run_standardize_worker, []),
                (self.save_standardized_files_worker, [])
            ],
            run_parallel=False
        )
        self.finalize_standardize_update_status()

        self._logger.info("")
        self._run_consumers([(self.save_standardize_status_worker, [])])

