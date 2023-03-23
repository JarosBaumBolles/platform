"""Willow Integration"""
import uuid
from json import dumps, load
from pathlib import Path
from queue import Queue
from typing import Optional
from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.willow.config import WillowCfg
from integration.willow.workers import (
    FetchWorker,
    GapsDetectionWorker,
    StandardizeWorker,
)


class WillowConnector(BasePullConnector):
    """Willow Integration"""

    __created_by__ = "Willow Connector"
    __description__ = "Willow Integration"
    __name__ = "Willow Connector"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._config: Optional[WillowCfg] = None
        self._gaps_worker: Optional[GapsDetectionWorker] = None
        self._fetch_worker: Optional[FetchWorker] = None
        self._standardize_worker: Optional[StandardizeWorker] = None

        # TODO: Should be removed  after Coned moving to the new approach
        # and final refactoring
        self._standardized_files: Queue = Queue()

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            self._config = self._config_factory(data, WillowCfg)
            self._configure_workers(
                gaps_cls=GapsDetectionWorker,
                fetch_cls=FetchWorker,
                standardize_cls=StandardizeWorker,
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
        description="WILLOW DEBUG",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        connector = WillowConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":
    import base64

    CONNECTOR_NAME = "willow"
    METERS_AMOUNT = 10

    DEBUG_LOGGER = Logger(
        name="debug",
        level="DEBUG",
        description="WILLOW DEBUG",
        trace_id=uuid.uuid4(),
    )
    DEBUG_LOGGER.info("Running WILLOW integrations")
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
                "Completed integration",
                extra={"labels": {"elapsed_time": dbg_elapsed()}},
            )
