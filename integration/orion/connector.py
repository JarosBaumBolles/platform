"""Orion Integration functionality"""
import base64
import uuid
from json import dumps, load
from pathlib import Path
from queue import Queue
from typing import Optional

from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.orion.config import OrionCfg
from integration.orion.exceptions import MalformedConfig
from integration.orion.workers import FetchWorker, StandardizeWorker


class OrionConnector(BasePullConnector):
    """Contains functionality to load Orion data"""

    __created_by__ = "Orion Connector"
    __description__ = "Orion Integration"
    __name__ = "Orion Connector"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._config: Optional[OrionCfg()] = None
        self._fetch_worker: Optional[FetchWorker] = None
        self._standardize_worker: Optional[StandardizeWorker] = None

        # TODO: Should be removed  after Coned moving to the new approach
        # and final refactoring
        self._standardized_files: Queue = Queue()

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            self._config = self._config_factory(data, OrionCfg)
            self._configure_workers(
                fetch_cls=FetchWorker, standardize_cls=StandardizeWorker
            )

        self._logger.debug(
            "Loaded configuration.",
            extra={
                "labels": {
                    "elapsed_teime": elapsed(),
                }
            },
        )


def main(event, context):  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="debug",
        level="DEBUG",
        description="ORION DEBUG",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        main_logger.info("Strted data scraping. ")
        try:
            connector = OrionConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
            connector.configure(event)
            connector.run(run_gaps_check=False)
        except MalformedConfig as err:
            main_logger.error(f"Unexpectedly closed due to the error '{err}'")
        finally:
            main_logger.info(
                "Completed.", extra={"labels": {"elapsed_time": elapsed()}}
            )


if __name__ == "__main__":
    CONNECTOR_NAME = "orion"
    METERS_AMOUNT = 1
    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description="ORION DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info(f"Running {CONNECTOR_NAME} integration.")
    import debugpy

    debugpy.listen(CFG.DEBUG_PORT)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()
    with elapsed_timer() as dbg_elapsed:
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            for fl_idx in range(METERS_AMOUNT):
                payload_file = CFG.LOCAL_PATH.joinpath(
                    f"participant_payload_{participant_id}_connector_"
                    f"{CONNECTOR_NAME}_{fl_idx}.json"
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
