"""Nantum integration"""
import base64
import uuid
from json import dumps, load
from queue import Queue
from typing import Optional

from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.nantum.config import NantumCfg
from integration.nantum.worker import (
    FetchWorker,
    GapsDetectionWorker,
    StandardizeWorker,
)


class NantumConnector(BasePullConnector):
    """Natum Connector"""

    __name__ = "Nantum Connector"
    __today_run_hour__ = 23
    __today_shift_hour__ = 4

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._config: Optional[NantumCfg] = None
        self._gaps_worker: Optional[GapsDetectionWorker] = None
        self._fetch_worker: Optional[FetchWorker] = None
        self._standardize_worker: Optional[StandardizeWorker] = None

        # TODO: Should be removed  after Coned moving to the new approach
        # and final refactoring
        self._standardized_files: Queue = Queue()

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            self._config = self._config_factory(data, NantumCfg)
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

    def get_missed_hours(self) -> None:
        """
        Check connector standardize files for missing files
        representing polling responses.
        """
        # Standardized data stored separately by type meter. It means that it
        # possible to miss different hours for each meter in a list.
        # As the result we need to check each given meter for misssed hour
        # and build index (relation) between missed hour and related meters

        # In accordance with Nantum documentation Nantum Metric Data API -
        # This endpoint returns an array of the sensors that collect data for
        # the metric.
        # In addition to the general information associated with a sensor,
        # the sensor’s JSON object also returns areadingsfield that containsa
        # timeseries, which represents the data gathered by the sensor on a
        # particular date. This timeseries is an array consisting of aseries
        # of readings, each with atimeand avalue.For example, the following
        # field of thesensor object returned by the endpoint represents a
        # timeseries for the sensor

        # Basing on the above to retrive data with minimum Nantum APi calls
        # it is expedient to use the following optimizations:
        # 1. Skip today date if run hour is less than 11.00 p.m.
        # 2. Group all matched missed hours by month-day
        # 3. Find minimum and maximum date

        with elapsed_timer() as elapsed:
            start_date = self._run_time

            skip_day = (
                self._run_time.hour >= self.__today_shift_hour__
                and self._run_time.hour < self.__today_run_hour__
            )

            if skip_day:
                self._logger.warning(
                    f"Skip date {self._run_time} to avoid partial day processing"
                )
                start_date = self._run_time.subtract(days=1)
                start_date = start_date.replace(hour=23)
            self._gaps_worker.run(start_date, self._run_time)
            self._logger.debug(
                "Matched missed hours",
                extra={
                    "statistic": {"missed hours amount": len(self._missed_hours)},
                    "labels": {"elapsed_time": elapsed()},
                },
            )


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
    METERS_AMOUNHT = 8
    DEBUG_LOGGER = Logger(
        name="debug",
        level="DEBUG",
        description="NANTUM DEBUG",
        trace_id=uuid.uuid4(),
    )
    DEBUG_LOGGER.info("Running NANTUM integrations")
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
