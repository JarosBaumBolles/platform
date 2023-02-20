""" EcoStruxture integration module"""

import uuid
from json import JSONDecodeError, dumps, load, loads
from pathlib import Path
from queue import Queue
from typing import Optional
from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePushConnector
from integration.ecostruxture.config import EcoStruxtureCfg
from integration.ecostruxture.exceptions import MalformedConfig
from integration.ecostruxture.workers import (
    FetchWorker,
    GapsDetectionWorker,
    StandardizeWorker,
)


class EcoStruxtureConnector(BasePushConnector):
    """EcoStruxture Integration"""

    __created_by__ = "EcoStruxture Connector"
    __description__ = "EcoStruxture Integration"
    __name__ = "EcoStruxture Connector"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._config: Optional[EcoStruxtureCfg()] = None
        self._gaps_worker: Optional[GapsDetectionWorker] = None
        self._fetch_worker: Optional[FetchWorker] = None
        self._standardize_worker: Optional[StandardizeWorker] = None

        # TODO: Should be removed  after Coned moving to the new approach
        # and final refactoring
        self._standardized_files: Queue = Queue()

        # self._factory = Factory()
        # self._missed_hours = ExpiringDict(max_len=2000, max_age_seconds=3600)
        # self._config: Optional[EcoStruxtureCfg] = None
        # self._gaps_worker: Optional[GapsDetectionWorker] = None

        # # TODO: SHOULD be moved to the BasePullConnector and propagated to
        # # the other pull integrations

        # self._fetched_files_q: Queue = Queue()
        # self._fetch_update_q: Queue = Queue()
        # self._fetch_worker: Optional[FetchWorker] = None

        # self._standardized_files: Queue = Queue()
        # self._standardized_update_files: Queue = Queue()
        # self._standardized_files_count: Counter = Counter()
        # self._standardize_worker: Optional[StandardizeWorker] = None

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            self._config = self._config_factory(data, EcoStruxtureCfg)
            self._configure_workers(GapsDetectionWorker, FetchWorker, StandardizeWorker)

        self._logger.debug(
            "Loaded configuration.",
            extra={
                "labels": {
                    "elapsed_teime": elapsed(),
                }
            },
        )

    def _after_configuration(
        self, config: EcoStruxtureCfg, *args, **kwargs
    ) -> EcoStruxtureCfg:
        """Actions before config parser"""
        super()._after_configuration(config, *args, **kwargs)

        try:
            config.meters_sheet_mapper = loads(
                config.meters_sheet_mapper.replace("'", '"')
            )
        except JSONDecodeError as err:
            raise MalformedConfig(  # pylint:disable=raise-missing-from
                f"Cannot convert given mapper dump '{config.meters_sheet_mapper}'"
                f" to JSON object due to the reason '{err}'. Exit"
            )
        return config


def main(event, context) -> None:  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="ecostruxture integration",
        level="DEBUG",
        description="ECOSTRUXTURE INTEGRATION",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        connector = EcoStruxtureConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info("Completed", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":
    import base64

    CONNECTOR_NAME = "ecostruxture"
    METERS_AMOUNT = 1
    DEBUG_LOGGER = Logger(
        name="debug",
        level="DEBUG",
        description="ECOSTRUXTURE DEBUG",
        trace_id=uuid.uuid4(),
    )
    DEBUG_LOGGER.info("Running ECOSTRUXTURE integrations")
    import debugpy

    debugpy.listen(5678)
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
            "Completed integration", extra={"labels": {"elapsed_time": dbg_elapsed()}}
        )
