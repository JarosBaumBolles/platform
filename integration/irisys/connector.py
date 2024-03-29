"""Irisys Integration"""
import base64
import uuid
from collections import Counter
from json import JSONDecodeError, dumps, load
from queue import Queue
from typing import Optional

from dataclass_factory import Factory
from expiringdict import ExpiringDict

from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.irisys.config import IrisysCfg
from integration.irisys.exceptions import MalformedConfig
from integration.irisys.workers import (
    FetchWorker,
    GapsDetectionWorker,
    StandardizeWorker,
)


class IrisysConnector(BasePullConnector):
    """Irisys Integration"""

    __created_by__ = "Irisys Connector"
    __description__ = "Irisys Integration"
    __name__ = "Irisys Connector"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()

        self._missed_hours = ExpiringDict(max_len=2000, max_age_seconds=3600)
        self._config: Optional[IrisysCfg] = None
        self._gaps_worker: Optional[GapsDetectionWorker] = None

        self._fetched_files_q: Queue = Queue()
        self._fetch_update_q: Queue = Queue()
        self._fetch_worker: Optional[FetchWorker] = None

        self._standardized_files: Queue = Queue()
        self._standardized_update_files: Queue = Queue()
        self._standardized_files_count: Counter = Counter()
        self._standardize_worker: Optional[StandardizeWorker] = None

    # TODO: Review ability to move this method in base class
    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            try:
                js_config = self._before_configuration(data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")

                self._config = self._factory.load(js_config, IrisysCfg)
            except (ValueError, TypeError, JSONDecodeError) as err:
                raise MalformedConfig from err

            self._gaps_worker = GapsDetectionWorker(
                missed_hours_cache=self._missed_hours, config=self._config
            )

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

        self._logger.debug(
            "Loaded configuration.",
            extra={
                "labels": {
                    "elapsed_teime": elapsed(),
                }
            },
        )

    # TODO: @todo After comletion check posibility move to some base class
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

    # TODO: @todo Redesign. Move to base class
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

    # TODO: @todo Candidate to be in a base class
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

    def run(self):
        super().run()
        self.get_missed_hours()
        self.fetch()
        self.standardize()
        print("")


def main(event, context):  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="IRISYS INTEGRATION",
        level="DEBUG",
        description="IRISYS INTEGRATION",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as main_elapsed:
        connector = IrisysConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info(
            "Completed.", extra={"labels": {"elapsed_time": main_elapsed()}}
        )


if __name__ == "__main__":
    CONNECTOR_NAME = "irisys"
    METERS_AMOUNT = 2
    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description="IRISYS DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info("Running IRISYS integrations")

    import debugpy

    debugpy.listen(CFG.DEBUG_PORT)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()

    with elapsed_timer() as debug_elapsed:
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            for fl_idx in range(METERS_AMOUNT):
                payload_file = CFG.LOCAL_PATH.joinpath(
                    f"participant_payload_{participant_id}_connector_"
                    f"{CONNECTOR_NAME}_{fl_idx}.json"
                )
                if not payload_file.exists():
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
                "Completed integration",
                extra={"labels": {"elapsed_time": debug_elapsed()}},
            )
