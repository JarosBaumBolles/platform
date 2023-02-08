"""Utillty used for creating update files from the scratch"""
import base64
import queue
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from json import JSONDecodeError, dumps
from pathlib import Path
from typing import List, Optional

from dataclass_factory import Factory
from google.api_core import exceptions as gcp_except
from google.cloud import storage as gcp_storage

from common import settings as CFG
from common.bucket_helpers import (
    delete_blobs_in_directory,
    list_blobs_with_prefix,
    require_client,
)
from common.date_utils import format_date
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.thread_pool_executor import run_thread_pool_executor
from integration.base_integration import BasePushConnector, StorageInfo


# TODO: Duplicate code/ Should be refactored
class MalformedConfig(Exception):
    """Exception class specific to this package."""


@dataclass
class ExtraMeterInfo:
    """Extra Meter info"""

    timezone: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class MeterInfo:
    """Meter Info config"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    extra: ExtraMeterInfo = field(default_factory=ExtraMeterInfo)


@dataclass
class ExtraInfo:
    """Extra Info config"""

    participant_id: int = -1


@dataclass
class RunConfig:
    """Run Config"""

    meters: Optional[List[MeterInfo]] = None
    extra: ExtraInfo = field(default_factory=ExtraInfo)


class RegenerateStandardizeUpdateFiles(BasePushConnector):
    """Contains functionality to load Orion data"""

    __created_by__ = "Standardize Update Regeneration"
    __description__ = "STANDARDIZE UPDATE REGENERATION"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory: Factory = Factory()
        self._storage_client: gcp_storage.client.Client = require_client()
        self._config: Optional[RunConfig] = None
        self._update_files_to_remove: queue.Queue = queue.Queue()
        self._standardize_files_locations: queue.Queue = queue.Queue()
        self._standardize_files_list = defaultdict(queue.Queue)

        self._update_files: queue.Queue = queue.Queue()

    def configure(self, conf_data: bytes) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Loading configuration.")
            try:
                js_config = self.parse_base_configuration(conf_data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")

                self._config = self._factory.load(js_config, RunConfig)
            except (ValueError, TypeError, JSONDecodeError) as err:
                raise MalformedConfig from err

            self._logger.debug(
                "Loaded participant.", extra={"labels": {"elapsed_time": elapsed()}}
            )

    def _delete_updates(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Preparing updates to deletion.")
            for mtr in self._config.meters:
                obj_path = Path(mtr.extra.standardized.path).joinpath(
                    self.__update_prefix__
                )
                self._update_files_to_remove.put(
                    {
                        "bucket": mtr.extra.standardized.bucket,
                        "preffix": f"{str(obj_path).rstrip('/')}/",
                    }
                )

            self._delete_objects(
                objects_queue=self._update_files_to_remove,
                worker_replica=self._update_files_to_remove.qsize() + 1,
            )

            self._logger.debug(
                "Completed Standardize update files deletion.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _delete_objects(
        self,
        objects_queue: queue.Queue,
        worker_replica: int,
    ) -> None:
        if objects_queue.empty():
            self._logger.warning("List of objects for deletion is empty. Exit.")
            return None
        errors = queue.Queue()
        del_futures = run_thread_pool_executor(
            workers=[
                (
                    self._delete_objects_worker,
                    [
                        self._storage_client,
                        objects_queue,
                        errors,
                    ],
                )
            ],
            worker_replica=worker_replica,
        )

        for res in del_futures:
            try:
                res.result()
            except Exception as err:  # pylint: disable=broad-except
                self._logger.error(f"Received error '{err}' during files deletion")

        if not errors.empty():
            self._logger.error(
                f"During files deletion excepted {errors.qsize()} errors"
            )
            while not errors.empty():
                err = errors.get()
                self._logger.error(f"FILES DELETION ERROR: {err}")
                errors.task_done()
        return None

    @staticmethod
    def _delete_objects_worker(
        client: gcp_storage.client.Client,
        objects_to_delete: queue.Queue,
        deletion_errors: queue.Queue,
        worker_idx,  # pylint:disable=unused-argument
    ) -> None:
        empty_run_count = 0
        while True:
            if objects_to_delete.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                obj_info = objects_to_delete.get()
                try:
                    delete_blobs_in_directory(
                        bucket_name=obj_info["bucket"],
                        preffix=obj_info["preffix"],
                        quiet=True,
                        client=client,
                    )
                except gcp_except.NotFound as err:
                    deletion_errors.put(
                        f"Cannot delete object "
                        f"{obj_info['bucket']}/{obj_info['preffix']} "
                        f" due to the error {err}. Object does not exists "
                        f"or deletion is aborted due to permissions settings."
                    )
                objects_to_delete.task_done()

    @staticmethod
    def _scan_standardized_files_worker(
        client: gcp_storage.client.Client,
        locations_queue: queue.Queue,
        object_storage: defaultdict,
        errors_queue: queue.Queue,  # pylint:disable=unused-argument
        worker_idx: int,  # pylint:disable=unused-argument
    ) -> None:
        empty_run_count = 0
        while True:
            if locations_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                location = locations_queue.get()
                blobs = list_blobs_with_prefix(
                    client=client,
                    bucket_name=location["bucket"],
                    prefix=location["preffix"],
                )
                blobs_queue = object_storage[location["preffix"]]
                for blob in blobs:
                    blobs_queue.put(
                        {
                            "bucket": location["bucket"],
                            "preffix": location["preffix"],
                            "filename": Path(blob.name).name,
                        }
                    )
                locations_queue.task_done()

    def scan_standardized_files(self) -> None:
        """Get list of standardized files"""
        with elapsed_timer() as elapsed:
            self._logger.info("Scanning standadized files")

            for mtr in self._config.meters:
                self._standardize_files_locations.put(
                    {
                        "meter_uri": mtr.meter_uri,
                        "bucket": mtr.extra.standardized.bucket,
                        "preffix": mtr.extra.standardized.path,
                    }
                )

            errors = queue.Queue()
            self._scan_standardized_files_worker(
                self._storage_client,
                self._standardize_files_locations,
                self._standardize_files_list,
                errors,
                "zsrv",
            )

            list_futures = run_thread_pool_executor(
                workers=[
                    (
                        self._scan_standardized_files_worker,
                        [
                            self._storage_client,
                            self._standardize_files_locations,
                            self._standardize_files_list,
                            errors,
                        ],
                    )
                ],
                worker_replica=self._standardize_files_locations.qsize() + 1,
            )

            for res in list_futures:
                try:
                    res.result()
                except Exception as err:  # pylint: disable=broad-except
                    print(
                        f"ERROR: {self.__description__}: Received error "
                        f"'{err}' during standardizing files sanning."
                    )

            if not errors.empty():
                self._logger.error(
                    f"During files searching excepted {errors.qsize()} errors"
                )
                while not errors.empty():
                    err = errors.get()
                    self._logger.error(f"FILES LIST ERROR: " f"{err}")
                    errors.task_done()

            self._logger.info(
                "Completed scanning standadized files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    @staticmethod
    def _generate_update_files_worker(  # pylint:disable=too-many-arguments
        standardized_files_queue: queue.Queue,
        update_files_queue: queue.Queue,
        run_date: str,
        chunk_size: int,
        update_prefix: str,
        update_filename_tmpl: str,
        worker_idx: int,  # pylint:disable=unused-argument
    ) -> None:
        empty_run_count = 0
        cnt = 0
        files = queue.Queue(maxsize=chunk_size)

        def put_in_update_queue(
            file_name: str,
            filebody: queue.Queue,
        ) -> None:
            body = list(filebody.queue)
            if body:
                update_files_queue.put(
                    {
                        "bucket": body[0]["bucket"],
                        "preffix": (f"{body[0]['path'].rstrip('/')}/{update_prefix}"),
                        "filename": file_name,
                        "file_body": dumps({"files": list(filebody.queue)}),
                    }
                )

        while True:
            if standardized_files_queue.empty():
                if empty_run_count == 3:
                    if not files.empty():
                        put_in_update_queue(
                            file_name=update_filename_tmpl.format(
                                update_prefix=update_prefix,
                                cnt=cnt,
                                run_date=run_date,
                            ),
                            filebody=files,
                        )
                    break
                empty_run_count += 1
                # TODO: Add storing last data portion
            else:
                file_info = standardized_files_queue.get()
                try:
                    fl_data = {
                        "bucket": file_info["bucket"],
                        "path": file_info["preffix"],
                        "filename": file_info["filename"],
                    }
                    files.put_nowait(fl_data)
                except queue.Full:
                    put_in_update_queue(
                        file_name=update_filename_tmpl.format(
                            update_prefix=update_prefix,
                            cnt=cnt,
                            run_date=run_date,
                        ),
                        filebody=files,
                    )
                    cnt += 1
                    files = queue.Queue(maxsize=chunk_size)
                    files.put_nowait(fl_data)

    def _generate_standardize_update_files(self) -> None:

        st_files_keys = list(self._standardize_files_list.keys())

        if not st_files_keys:
            self._logger.warning("Standardized files absent. Exit.")
            return None

        with elapsed_timer() as elapsed:
            self._logger.debug("Generating Standardize update files.")

            workers = []
            run_date_str = format_date(
                self._run_time.in_timezone("UTC"),
                CFG.PROCESSING_DATE_FORMAT,
            )

            for fl_queue in self._standardize_files_list.values():
                workers.append(
                    (
                        self._generate_update_files_worker,
                        [
                            fl_queue,
                            self._update_files,
                            run_date_str,
                            CFG.DW_LOAD_FILES_BUCKET_LIMIT,
                            self.__update_prefix__,
                            CFG.UPDATE_FILENAME_TMPL,
                        ],
                    )
                )
            list_futures = run_thread_pool_executor(workers=workers, worker_replica=1)

            for res in list_futures:
                try:
                    res.result()
                except Exception as err:  # pylint: disable=broad-except
                    self._logger.error(
                        f"Received error '{err}' during udate files generation"
                    )

            self._logger.debug(
                "Generated standardize update files.",
                extra={"labels": {"elapsed_tiem": elapsed()}},
            )
            return None

    def _save_standardize_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving generated standardize update files")
            if not self._update_files.qsize():
                self._logger.warning("Updates files absent. Exit.")
                return None

            self._pool_upload_blobs_to_storage(
                self._update_files,
                worker_replica=20,
            )
            self._logger.debug(
                "Saved generated standardize update files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
            return None

    def run(self) -> None:
        super().run()
        self._delete_updates()
        self.scan_standardized_files()
        self._generate_standardize_update_files()
        self._save_standardize_update_status()

    def fetch(self, working_directory: tempfile.TemporaryDirectory) -> None:
        """Fetch Integration data"""

    def standardize(self, working_directory: tempfile.TemporaryDirectory) -> None:
        """Standardize Fetched data"""


def main(event, context) -> None:  # pylint:disable=unused-argument
    """Entry point"""
    with elapsed_timer() as elapsed:
        main_logger = Logger(
            name="STANDARDIZE UPDATE REGENERATION",
            level="DEBUG",
            description="STANDARDIZE UPDATE REGENERATION",
            trace_id=uuid.uuid4(),
        )
        connector = RegenerateStandardizeUpdateFiles(
            env_tz_info=CFG.ENVIRONMENT_TIME_ZONE
        )
        connector.configure(event)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":

    import json

    DEBUG_LOGGER = Logger(
        name="EXPORT PUBLIC DATA",
        level="DEBUG",
        description="EXPORT PUBLIC DATA",
        trace_id=uuid.uuid4(),
    )
    DEBUG_LOGGER.error("=" * 40)

    import debugpy

    debugpy.listen(5678)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()

    for participant_id in CFG.DEBUG_PARTICIPANTS:
        file_path = Path(CFG.LOCAL_PATH).joinpath(
            f"dbload_{participant_id}_payload.json"
        )

        if not file_path.exists():
            DEBUG_LOGGER.error(f"Payload file {file_path} doesn't exists. Skipping")
            continue

        with open(file_path, "r", encoding="utf-8") as exmpl_fl:
            cfg = json.load(exmpl_fl)

        json_config = dumps(cfg).encode("utf-8")
        event_sample = {"data": base64.b64encode(json_config)}

        main(event=event_sample, context=None)

    DEBUG_LOGGER.error("=" * 40)
