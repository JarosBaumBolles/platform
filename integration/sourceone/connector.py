""" SourceOne integration module"""

import uuid
from abc import abstractmethod
from collections import Counter
from json import dumps, load
from json.decoder import JSONDecodeError
from pathlib import Path
from queue import Queue
from typing import Optional

from dataclass_factory import Factory

from common import settings as CFG
from common.bucket_helpers import get_missed_standardized_files
from common.date_utils import format_date, truncate
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.thread_pool_executor import run_thread_pool_executor
from integration.base_integration import BasePushConnector
from integration.sourceone.config import SourceoneCfg
from integration.sourceone.data_structures import FetchPayload
from integration.sourceone.worker import FetchWorker, StandrdizeWorker


class MalformedConfig(Exception):
    """Exception class specific to this package."""


class SourceOneBaseConnector(BasePushConnector):
    """Base functionalty of Sourceone connectors"""

    __created_by__ = "Sourceone Connector"
    __description__ = "Sourceone Integration"
    __name__ = "Sourceone"
    __created_by__ = "Sourceone Connector"
    __description__ = "Sourceone Integration"
    __workers_amount__ = 15

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()
        self._is_fetch_run: bool = False
        self._missed_hours = Queue()
        self._config: Optional[SourceoneCfg] = None

        # TODO: SHOULD be moved to the BasePullConnector and propagated to
        # the other pull integrations

        self._fetched_files_q: Queue = Queue()
        self._fetch_update_q: Queue = Queue()
        self._fetch_worker: Optional[FetchWorker] = None
        self._standardized_files: Queue = Queue()
        self._standardized_update_files: Queue = Queue()
        self._standardized_files_count: Counter = Counter()
        self._standardize_worker: Optional[StandrdizeWorker] = None

    def fetch(self) -> None:  # pylint:disable=arguments-differ
        with elapsed_timer() as ellapsed:
            self._logger.info("Fetching data.")
            self._fetch_data()
            self._logger.debug(
                "Fetched missed hours.", extra={"labels": {"elapsed_time": ellapsed()}}
            )

    @abstractmethod
    def _fetch_data(self) -> None:
        """Integration Fetch logic"""

    def configure(self, conf_data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            try:
                js_config = self.parse_base_configuration(conf_data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")
                self._config = self._factory.load(js_config, SourceoneCfg)
            except (ValueError, TypeError, JSONDecodeError) as err:
                raise MalformedConfig from err

            self._logger.debug(
                "Loaded configuration.",
                extra={
                    "labels": {
                        "elapsed_teime": elapsed(),
                    }
                },
            )

    def run(self):
        super().run()

        self.get_missed_hours()
        self.fetch()
        self.standardize()
        self.save_update_status()

    # TODO: Should be moved to base class during moving all integrations in
    # paralell approach
    def process_worker_run_results(self, futures, logs) -> None:
        """Process worker statuses and collected logs"""
        for res in futures:
            try:
                res.result()
            except Exception as err:  # pylint: disable=broad-except
                self._logger.error(
                    f"ERROR: Received error '{err}' during downloading blobs"
                )

        while not logs.empty():
            level, _, message = logs.get()
            self._logger.log(level, message)
            logs.task_done()

    def save_update_status(self) -> None:
        self._save_fetch_update_status()
        self._save_standardize_update_status()

    def _save_fetch_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Saving fetching data update status")

            logs = Queue()

            futures = run_thread_pool_executor(
                workers=[
                    (
                        self._fetch_worker.save_fetch_status_worker,
                        [logs],
                    )
                ],
                worker_replica=self.__workers_amount__,
            )

            self.process_worker_run_results(futures, logs)

            self._logger.info(
                "Saved fetching update status.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _save_standardize_update_status(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info(f"Saving {self.__name__} standardized data update status")

            logs = Queue()
            futures = run_thread_pool_executor(
                workers=[
                    (
                        self._standardize_worker.save_standardize_status_worker,
                        [logs],
                    )
                ],
                worker_replica=self.__workers_amount__,
            )

            self.process_worker_run_results(futures, logs)

            self._logger.info(
                f"Saved {self.__name__} standardizing update status.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    @abstractmethod
    def get_missed_hours(self) -> None:
        """Get missed files"""

    @abstractmethod
    def standardize(self) -> None:  # pylint:disable=arguments-differ
        """Standardize fetched data"""


class SourceOneConnector(SourceOneBaseConnector):
    """EcoStruxture Integration"""

    __created_by__ = "SourceOne Connector"
    __description__ = "SourceOne Integration"

    __raw_files_pool_size__ = 100

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()
        self._is_fetch_run: bool = False
        self._config: Optional[SourceoneCfg] = None

    def configure(self, conf_data: bytes) -> None:
        super().configure(conf_data)

        self._fetch_worker = FetchWorker(
            missed_hours=self._missed_hours,
            fetched_files=self._fetched_files_q,
            fetch_update=self._fetch_update_q,
            config=self._config,
        )

        self._standardize_worker = StandrdizeWorker(
            raw_files=self._fetched_files_q,
            standardized_files=self._standardized_files,
            standardize_update=self._standardized_update_files,
            config=self._config,
        )

    def get_missed_hours(self) -> None:
        """
        Check connector standardize files for missing files
        representing polling responses.
        """
        self._logger.info("Matching missed hours.")
        with elapsed_timer() as elapsed:
            file_name = format_date(self._run_time, CFG.PROCESSING_DATE_FORMAT)
            counter = Counter()
            for mtr_cfg in self._config.meters:
                mtr_msd_poll_hrs = sorted(
                    get_missed_standardized_files(
                        start_date=truncate(self._run_time, level="hour"),
                        bucket_name=mtr_cfg.standardized.bucket,
                        bucket_path=mtr_cfg.standardized.path,
                        range_hours=self._config.gap_regeneration_window,
                    ),
                    reverse=True,
                )

                if mtr_msd_poll_hrs:
                    self._missed_hours.put(
                        FetchPayload(
                            file_name=file_name,
                            meter_cfg=mtr_cfg,
                            meters_hours=mtr_msd_poll_hrs,
                        )
                    )
                    counter["count"] += len(mtr_msd_poll_hrs)

            self._logger.debug(
                "Matched missed hours",
                extra={
                    "statistic": {"missed hours amount": counter["count"]},
                    "labels": {"elapsed_time": elapsed()},
                },
            )

    def _fetch_data(self) -> None:
        """Integration Fetch logic"""
        with elapsed_timer() as elapsed:
            self._logger.info(f"Fetching `{self.__name__}` data")

            if self._fetch_worker is None:
                self._logger.error(
                    "The 'configure' method must be run before. Complete."
                )
                return None

            logs = Queue()

            self._fetch_worker.configure(self._run_time)

            workers = (
                self._fetch_worker.run_fetch_worker,
                self._fetch_worker.save_fetched_files_worker,
            )

            for worker in workers:
                futures = run_thread_pool_executor(
                    workers=[
                        (
                            worker,
                            [logs],
                        )
                    ],
                    worker_replica=self.__workers_amount__,
                )
                self.process_worker_run_results(futures, logs)

            self._logger.info(
                f"Completed {self.__name__} data fetching.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

        return None

    # TODO: Should be moved to base class during integrations migration into
    # paralell procesing

    def standardize(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Start standardizing of fetched data.")
            if self._standardize_worker is None:
                self._logger.error(
                    "The 'configure' method must be run before. Complete."
                )
                return None

            logs = Queue()
            self._standardize_worker.configure(self._run_time)

            workers = (
                self._standardize_worker.run_standardize_worker,
                self._standardize_worker.save_standardized_files_worker,
            )

            for worker in workers:
                futures = run_thread_pool_executor(
                    workers=[
                        (
                            worker,
                            [logs],
                        )
                    ],
                    worker_replica=self.__workers_amount__,
                )
                self.process_worker_run_results(futures, logs)

            self._standardize_worker.finalize_standardize_update_status()

            self._logger.info(
                "Completed data standardization.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
        return None


def main(event, context) -> None:  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="debug",
        level="DEBUG",
        description="SOURCEONE DEBUG",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        connector = SourceOneConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":
    import base64

    CONNECTOR_NAME = "sourceone"
    METERS_AMOUNHT = 1

    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description=f"{CONNECTOR_NAME} DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info("Running SOURCEONE integrations")

    import debugpy

    debugpy.listen(CFG.DEBUG_PORT)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()
    with elapsed_timer() as dbg_elapsed:
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            for call_idx in range(METERS_AMOUNHT):
                payload_file = CFG.LOCAL_PATH.joinpath(
                    f"participant_payload_{participant_id}_connector_"
                    f"{CONNECTOR_NAME}_{call_idx}.json"
                )
                if not Path(payload_file).exists():
                    debug_logger.warning(
                        f"Payload file '{payload_file}' does not exists. Skipping"
                    )
                    continue

                with open(payload_file, "r", encoding="utf-8") as exmpl_fl:
                    cfg = load(exmpl_fl)

                json_config = dumps(cfg).encode("utf-8")
                event_sample = {"data": base64.b64encode(json_config)}
                main(event=event_sample, context=None)
        debug_logger.info(
            "Completed integration", extra={"labels": {"elapsed_time": dbg_elapsed()}}
        )
