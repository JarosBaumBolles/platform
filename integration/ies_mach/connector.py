"""IES Mach Integration"""
import base64
import uuid
from json import dumps, load
from queue import Queue
from typing import Optional

from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.base_integration.exceptions import MalformedConfig
from integration.ies_mach.config import IesMachCfg
from integration.ies_mach.workers import (
    FetchWorker,
    GapsDetectionWorker,
    StandardizeWorker,
)


class IESMachConnector(BasePullConnector):
    """IESMach Integration."""

    __created_by__ = "IESMach Connector"
    __description__ = "IESMach Integration"
    __name__ = "IESMach Connector"

    __right_timezones__ = ("UTC", "BuildingLocal")

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
            self._config = self._config_factory(data, IesMachCfg)
            self._configure_workers(GapsDetectionWorker, FetchWorker, StandardizeWorker)

        self._logger.debug(
            "Loaded configuration.",
            extra={
                "labels": {
                    "elapsed_teime": elapsed(),
                }
            },
        )

    def _after_configuration(self, config: IesMachCfg, *args, **kwargs) -> IesMachCfg:
        """Actions before config parser"""
        super()._after_configuration(config, *args, **kwargs)
        if config.time_zone not in self.__right_timezones__:
            self._logger.warning(
                f"Given timezone '{config.time_zone}' is not in alloved "
                f"list '{self.__right_timezones__}'. Set to "
                f"{self.__right_timezones__[0]}"
            )
            config.time_zone = self.__right_timezones__[0]
        return config


def main(event, context):  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="IES Mach run",
        level="DEBUG",
        description="IES MACH run",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        try:
            connector = IESMachConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
            connector.configure(event)
            connector.run()
        except MalformedConfig as err:
            main_logger.error(f"Cannot run connector due to the error '{err}'")
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":
    CONNECTOR_NAME = "ies_mach"
    METERS_AMOUNT = 1
    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description="IES_MACH DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info("Running IES_MACH integrations")

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
                extra={"labels": {"elapsed_time": dbg_elapsed()}},
            )
