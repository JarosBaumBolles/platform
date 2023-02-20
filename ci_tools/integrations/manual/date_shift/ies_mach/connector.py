"""IES MACH Date Shift integration."""
import base64
import uuid
from json import dumps, load
from queue import Queue
from typing import Optional
from ci_tools.integrations.manual.date_shift.base.workers import (
    FetchWorker,
    GapsDetectionWorker,
    StandardizeWorker,
)
from common import settings as CFG
from common.date_utils import parse
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.ies_mach.config import IesMachCfg


class Connector(BasePullConnector):
    """Ies Mach Shift Connector"""

    __created_by__ = "Ies Mach Shift Meter Date Local Connector"
    __description__ = "Ies Mach Integration"
    __name__ = "Ies Mach Shift Meter Date Local Connector"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._config: Optional[IesMachCfg] = None
        self._gaps_worker: Optional[GapsDetectionWorker] = None
        self._fetch_worker: Optional[FetchWorker] = None
        self._standardize_worker: Optional[StandardizeWorker] = None

        # TODO: Should be removed  after Coned moving to the new approach
        # and final refactoring
        self._standardized_files: Queue = Queue()

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            self._config = self._get_config(data, IesMachCfg)

            self._configure_workers(GapsDetectionWorker, FetchWorker, StandardizeWorker)

        self._logger.debug(
            "Loaded configuration.",
            extra={
                "labels": {
                    "elapsed_teime": elapsed(),
                }
            },
        )

    def run(self):
        self._run_time = parse(tz_info=self.env_tz_info)
        self.fetch()
        self.standardize()


def main(event, context):  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="CONED INTEGRATION",
        level="DEBUG",
        description="CONED INTEGRATION",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        connector = Connector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":
    CONNECTOR_NAME = "ies_mach"
    METERS_AMOUNHT = 13
    DEBUG_LOGGER = Logger(
        name="debug",
        level="DEBUG",
        description=f"{CONNECTOR_NAME} DEBUG",
        trace_id=uuid.uuid4(),
    )
    DEBUG_LOGGER.info(f"Running {CONNECTOR_NAME} integrations")
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
