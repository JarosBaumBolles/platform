"""Braxos Integration module"""
import base64
import uuid
from io import StringIO
from json import dumps, load
from queue import Queue
from typing import Optional

import paramiko
from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.braxos.config import BraxosCfg
from integration.braxos.workers import (
    FetchWorker,
    GapsDetectionWorker,
    StandardizeWorker,
)


class BraxosConnector(BasePullConnector):
    """Braxos Integration Connector"""

    __created_by__ = "Braxos Connector"
    __description__ = "Braxos Integration"
    __name__ = "Braxos Connector"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._config: Optional[BraxosCfg] = None
        self._gaps_worker: Optional[GapsDetectionWorker] = None
        self._fetch_worker: Optional[FetchWorker] = None
        self._standardize_worker: Optional[StandardizeWorker] = None

        # TODO: Should be removed  after Coned moving to the new approach
        # and final refactoring
        self._standardized_files: Queue = Queue()

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            self._config = self._config_factory(data, BraxosCfg)
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

    def _after_configuration(self, config: BraxosCfg, *args, **kwargs) -> BraxosCfg:
        """Actions before config parser"""
        super()._after_configuration(config, *args, **kwargs)
        raw_key = base64.b64decode(config.key.encode("utf-8")).decode("utf-8")
        config.key = paramiko.RSAKey.from_private_key(StringIO(raw_key[1:-1]))
        return config


def main(event, context):  # pylint:disable=unused-argument
    """Entry Point"""
    main_logger = Logger(
        name="Braxos run",
        level="DEBUG",
        description="BRAXOS RUN",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as ellapsed:
        connector = BraxosConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": ellapsed()}})


if __name__ == "__main__":

    CONNECTOR_NAME = "braxos"
    METERS_AMOUNT = 1
    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description="BRAXOS DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info("Running BRAXOS integrations")

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
