"""Nantum Date Shift integration."""
import base64
import uuid
from collections import Counter
from json import JSONDecodeError, dumps, load, loads
from queue import Queue
from typing import Optional

from dataclass_factory import Factory
from expiringdict import ExpiringDict

from ci_tools.integrations.manual.date_shift.base.workers import (
    FetchWorker,
    StandardizeWorker,
)
from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector, MalformedConfig
from integration.nantum.config import NantumCfg


class NantumBaseConnector(BasePullConnector):
    """Base functionalty of Nantum connectors"""

    __created_by__ = "Nantum Shift Meter Date Local Connector"
    __description__ = "Nantum Integration"
    __name__ = "Nantum Shift Meter Date Local Connector"

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            try:
                js_config = self._before_configuration(data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")
                self._config = self._factory.load(js_config, NantumCfg)

                self._config.timestamp_shift = loads(
                    self._config.timestamp_shift.replace("'", '"')
                )
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
        self.fetch()
        self.standardize()


class NantumConnector(NantumBaseConnector):
    """Natum Shift Connector"""

    __name__ = "Nantum Shift  Connector"
    __today_run_hour__ = 23
    __today_shift_hour__ = 4

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()

        self._config: Optional[NantumCfg] = None
        self._missed_hours = ExpiringDict(max_len=2000, max_age_seconds=3600)

        self._fetched_files_q: Queue = Queue()
        self._fetch_update_q: Queue = Queue()
        self._fetch_worker: Optional[FetchWorker] = None

        self._standardized_files: Queue = Queue()
        self._standardized_update_files: Queue = Queue()
        self._standardized_files_count: Counter = Counter()
        self._standardize_worker: Optional[StandardizeWorker] = None

    def configure(self, data: bytes) -> None:
        super().configure(data)
        self._fetch_worker = FetchWorker(
            missed_hours=self._missed_hours,
            fetched_files=self._fetched_files_q,
            fetch_update=self._fetch_update_q,
            config=self._config,
        )

        self._standardize_worker = StandardizeWorker(
            raw_files=self._fetched_files_q,
            standardized_files=self._standardized_files,
            standardize_update=self._standardized_update_files,
            config=self._config,
        )

    def fetch(self) -> None:
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
        else:
            self._fetch_worker.run(self._run_time)

    def standardize(self) -> None:
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


def main(event, context):  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="NANTUM INTEGRATION",
        level="DEBUG",
        description="NANTUM INTEGRATION",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        connector = NantumConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":
    CONNECTOR_NAME = "nantum"
    METERS_AMOUNHT = 2
    DEBUG_LOGGER = Logger(
        name="debug",
        level="DEBUG",
        description="NANTUM DEBUG",
        trace_id=uuid.uuid4(),
    )
    DEBUG_LOGGER.info("Running NANTUM integrations")
    # import debugpy

    # debugpy.listen(CFG.DEBUG_PORT)
    # debugpy.wait_for_client()  # blocks execution until client is attached
    # debugpy.breakpoint()
    with elapsed_timer() as dbg_elapsed:
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            for call_idx in range(METERS_AMOUNHT):
                payload_file = CFG.LOCAL_PATH.joinpath(
                    f"participant_payload_{participant_id}_connector_"
                    f"{CONNECTOR_NAME}_{call_idx}.json"
                )
                if not payload_file.exists():
                    DEBUG_LOGGER.warning(
                        f"Payload file '{payload_file}' does not exists. Skipping"
                    )
                    continue

                with open(payload_file, "r", encoding="utf-8") as exmpl_fl:
                    cfg = load(exmpl_fl)

                json_config = dumps(cfg).encode("utf-8")
                event_sample = {"data": base64.b64encode(json_config)}
                main(event=event_sample, context=None)
        DEBUG_LOGGER.info(
            "Completed integration", extra={"labels": {"elapsed_time": dbg_elapsed()}}
        )
