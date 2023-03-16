""" Base functionality of JBB integrations"""
# pylint: disable=logging-fstring-interpolation
import base64
import hashlib
import queue
import tempfile
import uuid
from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import islice
from json import dumps, loads
from json.decoder import JSONDecodeError
from pathlib import Path
from queue import Queue
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from dataclass_factory import Factory, Schema
from expiringdict import ExpiringDict
from google.cloud import exceptions as gcp_eceptions
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import (
    get_file_contents,
    list_blobs_with_prefix,
    move_blob,
    upload_file_to_bucket,
)
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, parse_timezone
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.thread_pool_executor import run_thread_pool_executor
from integration.base_integration.config import StorageInfo
from integration.base_integration.exceptions import (
    ConfigValidationError,
    MalformedConfig,
)

MAX_ID_VALUE = 9223372036854775807
DEFAULT_WORKER_REPLICA_AMOUNT = 20

T = TypeVar("T")


# TODO: should be refactored to use common realization from
# base_integration.config MeterCfg
@dataclass
class MeterConfig:
    """Meter Config"""

    meter_uri: str = ""
    meter_type: str = ""
    storage: StorageInfo = field(default_factory=StorageInfo)


class BaseAbstractConnnector:
    """Base Integration"""

    __metaclass__ = ABCMeta

    def __init__(self, env_tz_info: str) -> None:
        self.env_tz_info = parse_timezone(env_tz_info)
        self._run_time: Optional[DateTime] = None

    def run(self, **kwargs) -> None:  # pylint:disable=unused-argument
        """Run loop entrypoint"""
        self._run_time = parse(tz_info=self.env_tz_info)

    @abstractmethod
    def save_update_status(self):
        """Save staus after update"""

    @abstractmethod
    def _before_configuration(self, data: bytes) -> Optional[Any]:
        """Actions before config parser"""

    @abstractmethod
    def _after_configuration(self, config: T, *args, **kwargs) -> Optional[Any]:
        """Actions after config parser"""

    @abstractmethod
    def _config_validation(self, *args, **kwargs) -> bool:
        """Validate config after"""

    @abstractmethod
    def configure(self, data: bytes) -> None:
        """Parse configuration"""


class BaseConnector(BaseAbstractConnnector):
    """Integrations base functional"""

    __created_by__ = "Base Connector"
    __description__ = "Base Integration"
    __name__ = "Base connector"
    __update_prefix__ = "updates"
    __missed_hours_len__ = 100000

    def __init__(self, env_tz_info: str) -> None:
        super().__init__(env_tz_info=env_tz_info)
        self._run_time: Optional[DateTime] = None
        self._trace_id = str(uuid.uuid4())
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

        self._config: Optional[Any] = None

        self._missed_hours = ExpiringDict(
            max_len=self.__missed_hours_len__, max_age_seconds=3600
        )

        self._fetched_files_q: Queue = Queue()
        self._fetch_update_q: Queue = Queue()

        self._standardized_files: Queue = Queue()
        self._standardized_update_files: Queue = Queue()
        self._standardized_files_count: Counter = Counter()

        self._gaps_worker: Optional[Any] = None
        self._fetch_worker: Optional[Any] = None
        self._standardize_worker: Optional[Any] = None

    @staticmethod
    def split_into_chunks(seq: Iterable[List[Any]], size: int) -> Tuple[List[Any]]:
        """Split given iterable object ito chunks"""
        return (seq[pos : pos + size] for pos in range(0, len(seq), size))

    def get_unique_id(self, value: str) -> int:
        """Generate unique id based on meter config path

        Args:
            value (str): string value

        Returns:
            int: ID
        """

        # Known pylint issue https://github.com/PyCQA/pylint/issues/4039
        value = hashlib.shake_256(  # pylint: disable=too-many-function-args
            value.encode("utf-8")
        ).hexdigest(6)
        _uuid = int(value, 16)
        if _uuid > MAX_ID_VALUE:
            self._logger.warning(
                f"Generated meter id '{_uuid}' more than allowed "
                "value {MAX_ID_VALUE}. Meter config is located at {uuid}"
            )
            _uuid = _uuid & MAX_ID_VALUE
        return _uuid

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
                    f"Cannot encode file 'gs://{bucket}/{fl_path}' to JSON "
                    f"due to the error {err}.",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )
            except NotFound as err:
                data = {}
                self._logger.error(
                    f"Cannot load file due to the error {err}.",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )
            self._logger.debug(
                f"Loaded file {bucket}/{fl_path},",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
            return data

    def _load_text_data(
        self,
        bucket: str,
        path: str,
        filename: str,
    ) -> Union[bytes, str]:
        with elapsed_timer() as elapsed:
            path = f'{path.lstrip("/")}'
            fl_path = f"{path}/{filename}"
            self._logger.debug(f"Loading file {bucket}/{fl_path}.")
            data = get_file_contents(
                bucket_name=bucket,
                blob_path=fl_path,
            )
            self._logger.debug(
                f"Loaded file {bucket}/{fl_path},",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
            return data

    # TODO: Should be removed. All standardize logic should be in
    # standardize workers
    def _standardize_generic(
        self, data: dict, mtr_cfg: dict, getter: Callable
    ) -> Meter:

        meter = Meter()

        meter.created_date = self._run_time
        meter.start_time = mtr_cfg["start_date"]
        meter.end_time = mtr_cfg["end_date"]
        meter.created_by = self.__created_by__
        meter.usage = getter(data)
        meter.meter_uri = mtr_cfg.get("meter_uri", "N/A")
        return meter

    @staticmethod
    def _split_to_chunks(seq: Iterable[Any], size: int) -> Iterable[Any]:
        """Split iterable object into chunks by the given size"""
        itr = iter(seq)
        return iter(lambda: tuple(islice(itr, size)), ())

    @abstractmethod
    def configure(self, data: bytes) -> None:
        """Parse configuration"""

    def _before_configuration(self, data: bytes) -> dict:
        """Before configuration action"""
        try:
            return loads(base64.b64decode(data["data"]))["data"]
        except (ValueError, TypeError) as err:
            raise MalformedConfig(  # pylint:disable=raise-missing-from
                f"Config is malformed due to the reason '{err}'"
            )

    def _after_configuration(self, config: T, *args, **kwargs) -> T:
        """Actions after config parser"""
        return config

    def _config_validation(self, *args, **kwargs) -> None:
        """Validate config after"""

    # TODO @todo Try to use descriptors here to use more pythonic way
    def _config_factory(self, data: Dict, class_: Type[T]) -> T:
        """Parse configuration"""
        try:
            cfg = self._before_configuration(data)
            if not cfg:
                raise MalformedConfig("Recieved empty raw configuration data.")
            cfg["timestamp_shift"] = loads(
                cfg.get("timestamp_shift", "{}").replace("'", '"')
            )

            config = self._factory.load(cfg, class_)
            config = self._after_configuration(config)
            self._config_validation()
        except (ValueError, TypeError, JSONDecodeError, ConfigValidationError) as err:
            raise MalformedConfig from err
        else:
            return config

    def _configure_workers(
        self,
        gaps_cls: Optional[type] = None,
        fetch_cls: Optional[type] = None,
        standardize_cls: Optional[type] = None,
    ) -> None:

        if gaps_cls is not None:
            self._gaps_worker = gaps_cls(
                missed_hours_cache=self._missed_hours, config=self._config
            )

        if fetch_cls is not None:
            self._fetch_worker = fetch_cls(
                missed_hours=self._missed_hours,
                fetched_files=self._fetched_files_q,
                fetch_update=self._fetch_update_q,
                config=self._config,
            )

        if standardize_cls is not None:
            self._standardize_worker = standardize_cls(
                raw_files=self._fetched_files_q,
                standardized_files=self._standardized_files,
                standardize_update=self._standardized_update_files,
                config=self._config,
            )

    def get_missed_hours(self) -> None:
        """Get list of missed hours"""
        # Standardized data stored separately by type meter. It means that it
        # possible to miss different hours for each meter in a list.
        # As the result we need to check each given meter for misssed hour
        # and build index (relation) between missedd hour and related meters

        self._logger.info("Matching missed hour.")
        with elapsed_timer() as elapsed:
            self._gaps_worker.run(self._run_time)

        self._logger.debug(
            "Matched missed hour.", extra={"labels": {"elapsed_time": elapsed()}}
        )

    def fetch(self) -> None:
        """Fetch data"""
        with elapsed_timer() as ellapsed:
            self._logger.info("Fetching data.")
            self._fetch_data()
            self._logger.debug(
                "Fetched missed hours.", extra={"labels": {"elapsed_time": ellapsed()}}
            )

    def _fetch_data(self) -> None:
        """Integration Fetch logic"""
        self._logger.info(f"Fetching `{self.__name__}` data")

        if self._fetch_worker is None:
            self._logger.error("The 'configure' method must be run before. Complete.")
            return None

        self._fetch_worker.run(self._run_time)
        return None

    def standardize(self) -> None:
        """Standardize meter data"""
        with elapsed_timer() as elapsed:
            self._logger.info("Start standardizing of fetched data.")
            if self._standardize_worker is None:
                self._logger.error(
                    "The 'configure' method must be run before. Complete."
                )
                return None
            self._standardize_worker.run(self._run_time)

            self._logger.info(
                "Completed data standardization.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

        return None

    def run(self, **kwargs) -> None:
        super().run()
        if kwargs.get("run_gaps_check", True):
            self.get_missed_hours()
        if kwargs.get("run_fetch", True):
            self.fetch()
        if kwargs.get("run_standardize", True):
            self.standardize()


class BasePushConnector(BaseConnector):
    """Push Integrations Base functionality"""

    __created_by__ = "Base Push Connector"
    __description__ = "Base Push Integration"
    __max_chunk_size__ = 1000
    __raw_to_process_prefix__ = ""
    __raw_processed_preffix__ = "/processed"
    __raw_junk_files_preffix__ = "/junk"
    __raw_file_name_preff__ = "raw-data-"
    __default_worker_replica_amount__ = DEFAULT_WORKER_REPLICA_AMOUNT
    __update_prefix__ = CFG.UPDATE_PREFIX
    __update_filename_tmpl__ = CFG.UPDATE_FILENAME_TMPL

    def __init__(self, env_tz_info: str) -> None:
        super().__init__(env_tz_info=env_tz_info)
        self._fetched_files: List[Dict[str, Any]] = []
        self._fetched_junk_files: List[Dict[str, Any]] = []
        self._standardized_files: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._standardized_files_count: Counter = Counter()
        self._config: Optional[Any] = None

    @staticmethod
    def _download_blobs_worker(
        worker_queue: queue.Queue,
        destination_path: Path,
        destination_queue: queue.Queue,
        worker_idx: int,
    ) -> None:
        empty_run_count = 0
        while True:
            if worker_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                b_file = worker_queue.get()
                try:
                    filename = Path(b_file.name).name
                    fl_path = destination_path.joinpath(filename)
                    b_file.download_to_filename(fl_path)
                    destination_queue.put((filename, fl_path))
                except gcp_eceptions.NotFound as err:
                    # TODO: Redesign to use log queue
                    print(
                        f"ERROR: WORKER {worker_idx}: "
                        f"Cannot download file {b_file.name} due to the "
                        f"error '{err}'"
                    )
                finally:
                    worker_queue.task_done()

    def _download_blobs_to_filename_pool(
        self,
        blobs: List[storage.blob.Blob],
        dest_directory: Path,
        worker_replica: int = DEFAULT_WORKER_REPLICA_AMOUNT,
    ) -> queue.Queue:

        fl_queue = queue.Queue()
        fl_dnwld = queue.Queue()

        for b_file in blobs:
            fl_queue.put(b_file)

        futures = run_thread_pool_executor(
            workers=[
                (
                    self._download_blobs_worker,
                    [
                        fl_queue,  # worker_queue
                        dest_directory,  # destination_path
                        fl_dnwld,  # destination_queue
                    ],
                )
            ],
            worker_replica=worker_replica,
        )

        for res in futures:
            try:
                res.result()
            except Exception as err:  # pylint: disable=broad-except
                self._logger.error(f"Received error '{err}' during downloading blobs")

        return fl_dnwld

    @staticmethod
    def move_blobs_worker(
        worker_queue: queue.Queue,
        worker_idx: int,  # pylint: disable=unused-argument
    ) -> None:
        """Internal worker used for move blobs"""
        empty_run_count = 0
        while True:
            if worker_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                fl_info = worker_queue.get()
                blob_name = f"{fl_info['preffix'].rstrip('/')}/{fl_info['filename']}"
                new_blob_name = (
                    f"{fl_info['destination_preffix'].rstrip('/')}/"
                    f"{fl_info['destination_filename']}"
                )
                move_blob(
                    bucket_name=fl_info["bucket"],
                    blob_name=blob_name,
                    destination_bucket=fl_info["destination_bucket"],
                    new_blob_name=new_blob_name,
                    quiet=True,
                )
                worker_queue.task_done()

    def _move_files_pool(
        self,
        mv_files: List[Dict[str, Any]],
        worker_replica: int = DEFAULT_WORKER_REPLICA_AMOUNT,
    ) -> None:

        with elapsed_timer() as elapsed:
            self._logger.debug("Moving files.")

            if not mv_files:
                self._logger.error("Absent list files. Exiting")
            else:

                mv_fl_queue = queue.Queue()
                for mv_fl in mv_files:
                    mv_fl_queue.put(mv_fl)

                futures = run_thread_pool_executor(
                    workers=[
                        (
                            self.move_blobs_worker,
                            [
                                mv_fl_queue,  # worker_queue
                            ],
                        )
                    ],
                    worker_replica=worker_replica,
                )

                for res in futures:
                    try:
                        res.result()
                    except Exception as err:  # pylint: disable=broad-except
                        self._logger.error(
                            f"Received error '{err}' during moving files"
                        )

            self._logger.debug(
                f"Completed moving files. Processed {len(mv_files)} files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    @staticmethod
    def upload_blobs_worker(
        worker_queue: queue.Queue,
        worker_idx: int,  # pylint: disable=unused-argument
    ) -> None:
        """Upload file worker"""
        empty_run_count = 0
        while True:
            if worker_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                fl_info = worker_queue.get()
                upload_file_to_bucket(
                    bucket_name=fl_info["bucket"],
                    blob_path=fl_info["preffix"],
                    file_name=fl_info["filename"],
                    blob_text=fl_info["file_body"],
                )
                worker_queue.task_done()

    def _get_workers_amount(
        self, queue_size: int, max_worker_amount: Optional[int] = None
    ) -> int:
        """Calculate effective workers amount"""

        # In a general calculating the number of workers in the thread poll
        # executor to achieve the best performance are the art of your fingertips.
        # So we here use a very poor approach based on avoiding idle workers as
        # much as possible. Workers amount is equal with queue_size if the queue
        # size is less them some empiric maximum number or the maximum number shoul
        # be taken instead
        return min(
            queue_size, self.__default_worker_replica_amount__ or max_worker_amount
        )

    def _pool_upload_blobs_to_storage(
        self,
        st_files: Union[queue.Queue, List[Dict[str, Any]]],
        worker_replica: int = DEFAULT_WORKER_REPLICA_AMOUNT,
    ) -> None:

        with elapsed_timer() as elapsed:
            self._logger.debug("Uploading files.")

            if not st_files:
                self._logger.error("Absent files. Exiting")
                return None

            if not isinstance(st_files, queue.Queue):
                upld_fl_queue = queue.Queue()
                for st_fl in st_files:
                    upld_fl_queue.put(st_fl)
            else:
                upld_fl_queue = st_files
            size = upld_fl_queue.qsize()

            futures = run_thread_pool_executor(
                workers=[
                    (
                        self.upload_blobs_worker,
                        [
                            upld_fl_queue,  # worker_queue
                        ],
                    )
                ],
                worker_replica=worker_replica,
            )

            for res in futures:
                try:
                    res.result()
                except Exception as err:  # pylint: disable=broad-except
                    self._logger.error(
                        f"Received error '{err}' during uploading " "standardized files"
                    )

            self._logger.debug(
                f"Completed uploading data. Processed {size} files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
        return None

    def _fetch_files(  # pylint:disable=too-many-arguments
        self,
        bucket: str,
        preffix: str,
        working_directory: tempfile.TemporaryDirectory,
        worker_replica: int = DEFAULT_WORKER_REPLICA_AMOUNT,
    ) -> None:

        source_preffix = f"{preffix}{self.__raw_to_process_prefix__}"
        destination_preffix = f"{preffix}{self.__raw_processed_preffix__}"

        files_to_process = list_blobs_with_prefix(
            bucket_name=bucket,
            prefix=f"{source_preffix.rstrip('/')}/{self.__raw_file_name_preff__}",
            force_dir=False,
        )

        wrk_dir_path = Path(working_directory)

        files_dwnld = self._download_blobs_to_filename_pool(
            blobs=files_to_process,
            dest_directory=wrk_dir_path,
            worker_replica=self._get_workers_amount(
                len(files_to_process),
                max(worker_replica, self.__default_worker_replica_amount__),
            ),
        )

        while not files_dwnld.empty():
            filename, local_path = files_dwnld.get()
            self._fetched_files.append(
                {
                    "bucket": bucket,
                    "preffix": source_preffix,
                    "filename": filename,
                    "destination_bucket": bucket,
                    "destination_preffix": destination_preffix,
                    "destination_filename": filename,
                    "local": local_path,
                }
            )
            files_dwnld.task_done()

    def _move_processed_fetched_files(
        self, worker_replica: int = DEFAULT_WORKER_REPLICA_AMOUNT
    ) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Start Moving files.")

            if not self._fetched_files:
                self._logger.error(
                    "Cannot start moving of procesed fetched files due to the "
                    "empty fetched files."
                )
            else:
                self._move_files_pool(
                    mv_files=self._fetched_files,
                    worker_replica=self._get_workers_amount(
                        len(self._fetched_files),
                        max(worker_replica, self.__default_worker_replica_amount__),
                    ),
                )

            if self._fetched_junk_files:
                self._logger.warning(
                    f"During sndardization found {len(self._fetched_junk_files)}."
                    " Moving in junk files."
                )
                self._move_files_pool(
                    mv_files=self._fetched_junk_files,
                    worker_replica=self._get_workers_amount(
                        len(self._fetched_junk_files),
                        max(worker_replica, self.__default_worker_replica_amount__),
                    ),
                )

            self._logger.debug(
                "Moving files completed.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _save_fetch_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Start Moving files.")
            files_to_save = []
            if not self._fetched_files:
                self._logger.error(
                    "Cannot start moving of procesed fetched files due to the "
                    "empty fetched files."
                )
            else:
                filename = format_date(self._run_time, CFG.PROCESSING_DATE_FORMAT)
                update_path = (
                    f"{self._config.extra.raw.path.rstrip('/')}/"
                    f"{self.__update_prefix__}"
                )

                json_data = {"files": []}
                count, chunk_idx = Counter(), 1
                for ftch_fl in self._fetched_files:
                    json_data["files"].append(
                        {
                            "bucket": ftch_fl["destination_bucket"],
                            "preffix": ftch_fl["destination_preffix"],
                            "filename": ftch_fl["destination_filename"],
                        }
                    )
                    count[chunk_idx] += 1
                    if count[chunk_idx] >= self.__max_chunk_size__:
                        files_to_save.append(
                            {
                                "bucket": self._config.extra.raw.bucket,
                                "preffix": update_path,
                                "filename": f"{self.__update_prefix__}_{filename}_{chunk_idx}",
                                "file_body": dumps(json_data),
                            }
                        )
                        count[chunk_idx] = 0
                        chunk_idx += 1
                        json_data = {"files": []}
                else:  # pylint: disable=useless-else-on-loop
                    if json_data["files"] and count[chunk_idx]:
                        files_to_save.append(
                            {
                                "bucket": self._config.extra.raw.bucket,
                                "preffix": update_path,
                                "filename": f"{self.__update_prefix__}_{filename}_{chunk_idx}",
                                "file_body": dumps(json_data),
                            }
                        )
                self._pool_upload_blobs_to_storage(
                    files_to_save,
                    worker_replica=self._get_workers_amount(
                        len(files_to_save),
                        self.__default_worker_replica_amount__,
                    ),
                )
            self._logger.debug(
                "Saved update status for fetched files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    @staticmethod
    def _generate_update_files_worker(  # pylint: disable=too-many-arguments
        standardized_files_queue: queue.Queue,
        update_files_queue: queue.Queue,
        run_date: str,
        chunk_size: int,
        update_prefix: str,
        update_filename_tmpl: str,
        worker_idx: int,  # pylint: disable=unused-argument
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

    def _save_standardize_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving update status for statandardized files.")

            updates_files_queue = queue.Queue()
            run_date_str = format_date(
                self._run_time.in_timezone("UTC"),
                CFG.PROCESSING_DATE_FORMAT,
            )
            workers = []
            for _, mtr_files in self._standardized_files.items():
                files_queue = queue.Queue()
                for mtr_fl in mtr_files:
                    files_queue.put_nowait(mtr_fl)
                workers.append(
                    (
                        self._generate_update_files_worker,
                        [
                            files_queue,
                            updates_files_queue,
                            run_date_str,
                            CFG.DW_LOAD_FILES_BUCKET_LIMIT,
                            self.__update_prefix__,
                            self.__update_filename_tmpl__,
                        ],
                    )
                )

            list_futures = run_thread_pool_executor(workers=workers, worker_replica=1)

            for res in list_futures:
                try:
                    res.result()
                except Exception as err:  # pylint: disable=broad-except
                    self._logger.error(
                        f"Received error '{err}' during call "
                        f"{CFG.DB_LOAD_FUNCTION_NAME}"
                    )

            self._pool_upload_blobs_to_storage(
                updates_files_queue,
                worker_replica=updates_files_queue.qsize() + 1,
            )
            self._logger.debug(
                "Saved update status for statandardized files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _upload_standardized_to_buckets(
        self,
        st_files: Dict[str, List[Dict[str, Any]]],
        worker_replica: int = DEFAULT_WORKER_REPLICA_AMOUNT,
    ) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Uploading standardized files.")
            data = sum(st_files.values(), [])
            self._pool_upload_blobs_to_storage(
                st_files=data,
                worker_replica=self._get_workers_amount(len(data), worker_replica),
            )
            self._logger.debug(
                "Uploaded standardized files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def save_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Saving update status.")
            self._save_standardize_update_status()
            self._move_processed_fetched_files()
            self._save_fetch_update_status()
            self._logger.info(
                "Saved update status.", extra={"labels": {"elapsed_time": elapsed()}}
            )

    @abstractmethod
    def configure(self, data: bytes) -> None:
        """Parse configuration"""


class BasePullConnector(BaseConnector):
    """Base Pull integration. Contains general functionality"""

    __created_by__ = "Base Pull Connector"
    __description__ = "Base Pull Integration"
    __max_chunk_size__ = 200
    __update_prefix__ = CFG.UPDATE_PREFIX
    __update_filename_tmpl__ = CFG.UPDATE_FILENAME_TMPL

    def __init__(self, env_tz_info: str) -> None:
        super().__init__(env_tz_info=env_tz_info)
        self._fetched_files: Dict[str, Set[Any]] = defaultdict(set)
        self._standardized_files: Dict[str, List[Any]] = defaultdict(list)

        self._cfg_fetch: Optional[Any] = None  # should be replaced in child
        self._cfg_meters: Optional[Any] = None  # should be replaced in child
        self._config: Optional[Any] = None  # should be replaced in child

    def _save_fetched_data(
        self, blob_text: str, filename: str, meter_name: str
    ) -> None:
        try:
            upload_file_to_bucket(
                self._config.extra.raw.bucket,
                dumps(blob_text),
                blob_path=self._config.extra.raw.path,
                file_name=filename,
            )
        except GoogleCloudError as err:
            self._logger.error(
                f"Cannot save meter '{meter_name}' file "
                f"{self._config.raw.bucket}/{self._config.extra.raw.path}"
                f"/{filename} due to the error '{err}'"
            )
        else:
            self._fetched_files[meter_name].add(
                (self._config.extra.raw.bucket, self._config.extra.raw.path, filename)
            )

    def _save_standardized_data(
        self, data: Meter, filename: str, mtr_cfg: MeterConfig
    ) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving standardized data.")
            try:
                upload_file_to_bucket(
                    mtr_cfg.standardized.bucket,
                    data.as_str(),
                    blob_path=mtr_cfg.standardized.path,
                    file_name=filename,
                )
            except GoogleCloudError as err:
                self._logger.error(
                    "Cannot save standardized meter data of "
                    f"{mtr_cfg.type} and hour '{filename}' by path "
                    f"{mtr_cfg.standardized.bucket}/{mtr_cfg.standardized.path}"
                    f"/{filename} due to the error '{err}'"
                )
            else:
                self._standardized_files[mtr_cfg.type.strip().lower()].append(
                    (
                        mtr_cfg.standardized.bucket,
                        mtr_cfg.standardized.path,
                        filename,
                        data,
                    )
                )
            self._logger.debug(
                f"Saved standardized data into {mtr_cfg.standardized.bucket}/"
                f"{mtr_cfg.standardized.path}/{filename}.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _upload_update_status_file(
        self, raw_data: dict, str_bucket: str, str_path: str, filename: str
    ) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug(
                f"Saving update status to file {str_bucket}/{str_path}/{filename}"
            )
            try:
                upload_file_to_bucket(
                    self._config.extra.raw.bucket,
                    dumps(raw_data),
                    blob_path=str_path,
                    file_name=filename,
                )
            except GoogleCloudError as err:
                self._logger.error(
                    f"Cannot save update status for '{filename}' hour in "
                    f"{str_bucket}/{str_path}/{filename} due to the error "
                    f"'{err}'"
                )
            self._logger.debug(
                f"Saved update status to file {str_bucket}/{str_path}/" f"{filename}.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _save_fetch_update_status(self) -> None:
        # TODO: Spagetti Redesign
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving fetch update status.")

            cache = set()
            json_data = {"files": []}

            filename = format_date(self._run_time, CFG.PROCESSING_DATE_FORMAT)

            for ft_info in self._fetched_files.values():
                for ft_fl_info in ft_info:
                    if ft_fl_info not in cache:
                        json_data["files"].append(
                            {
                                "bucket": ft_fl_info[0],
                                "path": ft_fl_info[1],
                                "filename": ft_fl_info[2],
                            }
                        )
                        cache.add(ft_fl_info)

            if json_data["files"]:
                self._upload_update_status_file(
                    raw_data=json_data,
                    str_bucket=self._config.extra.raw.bucket,
                    str_path=(
                        f"{self._config.extra.raw.path}/{self.__update_prefix__}"
                    ),
                    filename=filename,
                )
            else:
                self._logger.warning("Fetch update status is empty.")
        self._logger.debug(
            f"Saved fetch update status for '{filename}' hour in "
            f"{self._config.extra.raw.bucket}.",
            extra={"labels": {"elapsed_time": elapsed()}},
        )

    def _get_meter_config_by_name(self, mtr_name: str) -> Optional[Any]:
        for mtr_cfg in self._config.meters:
            if mtr_cfg.meter_name.strip().lower() == mtr_name.strip().lower():
                return mtr_cfg
        return None

    def _get_meter_config_by_type(self, mtr_type: str) -> Optional[Any]:
        for mtr_cfg in self._config.meters:
            self._logger.debug(f"meter type is - {mtr_cfg.type}")
            if mtr_cfg.type.strip().lower() == mtr_type.strip().lower():
                return mtr_cfg
        return None

    def _save_standardize_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving standardize update status.")
            filename = format_date(self._run_time, CFG.PROCESSING_DATE_FORMAT)
            self._logger.debug(
                f"Standardized files items - {list(self._standardized_files.keys())}"
            )
            for mtr_type, mtr_files in self._standardized_files.items():
                mtr_cfg = self._get_meter_config_by_type(mtr_type)
                if not mtr_cfg:
                    self._logger.error(
                        "Cannot find meter config for the given type "
                        f"{mtr_type}. Skipping"
                    )
                    continue
                cache = set()
                json_data = {"files": []}
                for mtr_fl_info in mtr_files:
                    key = mtr_fl_info[:-1]
                    if key not in cache:
                        json_data["files"].append(mtr_fl_info)
                        cache.add(key)

                if json_data["files"]:
                    chunks = self.split_into_chunks(
                        json_data["files"], self.__max_chunk_size__
                    )
                    save_path = Path(mtr_cfg.standardized.path).joinpath(
                        self.__update_prefix__
                    )
                    for indx, chunk in enumerate(chunks, 1):
                        data = {"amounts": len(chunk), "files": [], "updates": []}

                        for bucket, path, filename, mtr in chunk:
                            data["files"].append(
                                {"bucket": bucket, "path": path, "filename": filename}
                            )

                            data["updates"].append(
                                {
                                    "data": float(mtr.usage),
                                    "ref_hour_id": int(
                                        format_date(
                                            mtr.start_time, CFG.HOUR_ID_DATE_FORMAT
                                        )
                                    ),
                                    "ref_meter_id": int(mtr.meter_id),
                                    "ref_participant_id": int(
                                        self._config.extra.participant_id
                                    ),
                                }
                            )

                        self._upload_update_status_file(
                            raw_data=data,
                            str_bucket=mtr_cfg.standardized.bucket,
                            str_path=str(save_path),
                            filename=self.__update_filename_tmpl__.format(
                                update_prefix=self.__update_prefix__,
                                cnt=indx,
                                run_date=filename,
                            ),
                        )
                else:
                    self._logger.warning("Fetch update status is empty.")
            self._logger.debug(
                "Saving standardize update status.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    # TODO: Should be commissioned after migration on latest worker approach
    def save_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Saving update status.")
            self._save_fetch_update_status()
            self._save_standardize_update_status()
            self._logger.info(
                "Saved update status.", extra={"labels": {"elapsed_time": elapsed()}}
            )

    @abstractmethod
    def configure(self, data: bytes) -> None:
        pass
