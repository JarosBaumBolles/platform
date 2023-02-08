"""Base dispatchers functionality"""
import uuid
from collections.abc import MutableMapping
from functools import reduce
from json import dump, loads
from json.decoder import JSONDecodeError
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud import pubsub_v1
from google.cloud.storage.bucket import Bucket
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import get_buckets_list, get_file_contents
from common.date_utils import parse, parse_timezone
from common.elapsed_time import elapsed_timer
from common.logging import Logger


class UnexpectedAction(Exception):
    """Exception class specific to this package."""


class BaseDispatcher:
    """Base Dispatcher"""

    __description__ = "BASE DISPATCHER"
    __local_path__ = CFG.LOCAL_PATH
    __local_cfg_name_tmpl__ = "local_config.json"

    def __init__(self, env_tz_info=CFG.ENVIRONMENT_TIME_ZONE):
        self._publisher = pubsub_v1.PublisherClient()
        self._run_time: Optional[DateTime] = None
        self._trace_id = str(uuid.uuid4())
        self.env_tz_info = parse_timezone(env_tz_info)
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)
        self._payloads: Queue = Queue()

    def run(self) -> None:
        """Run dispatcher loop"""
        self._run_time = parse(tz_info=self.env_tz_info)

    def _save_payload_locally(self):
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving payload locally.")

            if self._payloads.empty():
                self._logger.warning(
                    "There is no payload required for the call "
                    f"`{CFG.DW_UPDATE_FUNCTION_NAME}` connector. Exit."
                )
                return None

            while not self._payloads.empty():
                payload = self._payloads.get()
                json_cfg = {
                    "data": payload,
                }

                filename = self.__local_cfg_name_tmpl__.format(
                    participant_id=json_cfg["data"]["extra"]["participant_id"]
                )

                file_path = self.__local_path__.joinpath(filename)
                with open(file_path, "w", encoding="UTF-8") as cfg_fl:
                    dump(json_cfg, cfg_fl, indent=4)
            self._logger.debug(
                "Saved payload locally.", extra={"labels": {"elapsed_time": elapsed()}}
            )

    def _load_json_data(
        self,
        bucket: str,
        path: str,
        filename: str,
    ) -> Union[bytes, str]:
        with elapsed_timer() as elapsed:
            path = f'{path.lstrip("/")}'
            fl_path = f"{path}/{filename}"
            self._logger.debug(f"Loading file {bucket}/{fl_path},")
            try:
                data = loads(
                    get_file_contents(
                        bucket_name=bucket,
                        blob_path=fl_path,
                    )
                )
            except JSONDecodeError as err:
                data = {}
                self._logger.error(
                    f"Cannot encode raw file to JSON due to the error {err}.",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )
            self._logger.debug(
                f"Loaded file {bucket}/{fl_path},",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
            return data

    def _rec_merge(
        self, dict_1: Dict[str, Any], dict_2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update two dicts of dicts recursively,
        if either mapping has leaves that are non-dicts,
        the second's leaf overwrites the first's.
        """
        for key, value in dict_1.items():
            if key in dict_2:
                # this next check is the only difference!
                if all(isinstance(e, MutableMapping) for e in (value, dict_2[key])):
                    dict_2[key] = self._rec_merge(value, dict_2[key])
                # we could further check types and merge as appropriate here.
        dict_3 = dict_1.copy()
        dict_3.update(dict_2)
        return dict_3

    def _merge_dicts(self, dicts: Tuple[Dict[str, Any]]) -> Dict[str, Any]:
        return reduce(self._rec_merge, dicts)

    def get_buckets(self) -> List[Bucket]:
        """Get buckets list to process."""
        with elapsed_timer() as elapsed:
            self._logger.info("Retrieving buckets list")
            buckets = get_buckets_list(project=CFG.PROJECT, preffix=CFG.BUCKET_PREFFIX)

            if CFG.DEBUG:
                self._logger.debug(
                    "Debug is enabled. "
                    "Filter buckets list to run only buckets from allowed list"
                    f" '{{{CFG.DEBUG_BUCKETS}}}'"
                )
                buckets = filter(lambda x: x.name in CFG.DEBUG_BUCKETS, buckets)

            buckets = list(buckets)

            self._logger.info(
                f"The '{len(buckets)}' buckets planned to process.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

            return buckets
