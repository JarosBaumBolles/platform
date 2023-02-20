""" WatTime integration module"""
import base64
import uuid
from abc import abstractmethod
from collections import Counter
from json import JSONDecodeError, dumps, load
from queue import Queue
from typing import Optional, Union

from dataclass_factory import Factory
from expiringdict import ExpiringDict

from common import settings as CFG
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.wattime.config import WattimeCfg
from integration.wattime.exceptions import MalformedConfig
from integration.wattime.worker import (
    AverageEmFetchWorker,
    AverageEmGapsDetectionWorker,
    AverageEmStandardizeWorker,
    MarginalEmFetchWorker,
    MarginalEmGapsDetectionWorker,
    MarginalEmStandardizeWorker,
)


class WatTimeBaseConnector(BasePullConnector):
    """Base functionalty of wattime connectors"""

    __created_by__ = "Wattime Connector"
    __description__ = "Wattime Integration"
    __name__ = "Wattime Connector"
    __auth_url__ = "https://api2.watttime.org/v2/login"
    __fetch_url__ = "https://api2.watttime.org/v2/"
    __workers_amount__ = 10

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()

        self._is_fetch_run: bool = False

        self._missed_hours = ExpiringDict(max_len=2000, max_age_seconds=3600)
        self._config: Optional[WattimeCfg] = None
        self._gaps_worker: Optional[
            Union[MarginalEmGapsDetectionWorker, AverageEmGapsDetectionWorker]
        ] = None

        # TODO: SHOULD be moved to the BasePullConnector and propagated to
        # the other pull integrations

        self._fetched_files_q: Queue = Queue()
        self._fetch_update_q: Queue = Queue()
        self._fetch_worker: Optional[
            Union[MarginalEmFetchWorker, AverageEmFetchWorker]
        ] = None

        self._standardized_files: Queue = Queue()
        self._standardized_update_files: Queue = Queue()
        self._standardized_files_count: Counter = Counter()
        self._standardize_worker: Optional[
            Union[MarginalEmStandardizeWorker, AverageEmStandardizeWorker]
        ] = None

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

    def configure(self, data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            try:
                js_config = self._before_configuration(data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")

                self._config = self._factory.load(js_config, WattimeCfg)
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

    @abstractmethod
    def get_missed_hours(self) -> None:
        """Get missed files"""


class WatTimeMarginalEmissionsConnector(WatTimeBaseConnector):
    """Wattime Marginal emissions functionality"""

    __created_by__ = "Wattime Marginal Emissions Connector"
    __description__ = "Wattime Marginal Emissions Integration"
    __name__ = "Wattime Marginal Emissions Connector"
    __fetch_url__ = "https://api2.watttime.org/v2/data"
    __workers_amount__ = 10

    def configure(self, data: bytes) -> None:
        super().configure(data)

        self._gaps_worker = MarginalEmGapsDetectionWorker(
            missed_hours_cache=self._missed_hours, config=self._config
        )

        self._fetch_worker = MarginalEmFetchWorker(
            missed_hours=self._missed_hours,
            fetched_files=self._fetched_files_q,
            fetch_update=self._fetch_update_q,
            config=self._config,
        )

        self._standardize_worker = MarginalEmStandardizeWorker(
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
        # Standardized data stored separately by type meter. It means that it
        # possible to miss different hours for each meter in a list.
        # As the result we need to check each given meter for misssed hour
        # and build index (relation) between missed hour and related meters
        self._logger.info("Matching missed hour.")

        with elapsed_timer() as elapsed:
            self._gaps_worker.run(self._run_time)

        self._logger.debug(
            "Matched missed hour.", extra={"labels": {"elapsed_time": elapsed()}}
        )


class WatTimeAverageEmissionsConnector(WatTimeBaseConnector):
    """Wattime Average emissions functionality

    In accordance with documentation
    https://averageemissions.docs.apiary.io/#reference/0/average-emissions-data

    Obtain historical average emissions for a given region.
    The data is published shortly after the source data becomes available.
    This is typically within an hour in the US but may occur overnight in
    some cases. It is sometimes longer in Europe, for example up to a week
    in Sweden.Use the standard WattTime /login endpoint to obtain an access
    token.We recommend pulling no more than a month of data per query.

    Obtain historical average emissions for a given region. The most
    recent data provided is 12 or more hours old - we do not provide
    realtime average emissions data.
    Based on real behavior we cannot determine actual data range,
    because delay could be various from call to call.

    Summing up to determaine missed hours range we should to run the
    following steps:
        1. Use standard previous missed hour range.
        2. Apply logic to check if minimum delta between end and start data
        is gretaer than 12 hours
        3. Split miised hours in data ranges not more than 1 month
        5. Fetch al miised point
        6. Save all fetched pints in separate raw file
        7. Standardize all fetched raw files using usual approach
    """

    __description__ = "Wattime Average Emissions Integration"

    __latest_point_time__ = "latest_point_time"
    __workers_amount__ = 10

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._standardized_point_times = []

    def configure(self, data: bytes) -> None:
        super().configure(data)

        self._gaps_worker = AverageEmGapsDetectionWorker(
            missed_hours_cache=self._missed_hours, config=self._config
        )

        self._fetch_worker = AverageEmFetchWorker(
            missed_hours=self._missed_hours,
            fetched_files=self._fetched_files_q,
            fetch_update=self._fetch_update_q,
            config=self._config,
        )

        self._standardize_worker = AverageEmStandardizeWorker(
            raw_files=self._fetched_files_q,
            standardized_files=self._standardized_files,
            standardize_update=self._standardized_update_files,
            config=self._config,
        )

    def get_missed_hours(self) -> None:
        # Standardized data stored separately by type meter. It means that it
        # possible to miss different hours for each meter in a list.
        # As the result we need to chec437k each given meter for misssed hour
        # and build index (relation) between missed hour and related meters

        self._logger.info("Matching missed hour.")

        with elapsed_timer() as elapsed:
            self._gaps_worker.run(self._run_time)

        self._logger.debug(
            "Matched missed hour.", extra={"labels": {"elapsed_time": elapsed()}}
        )


def main(
    event, context, integration_type: str = "average"  # pylint:disable=unused-argument
) -> None:
    """Entry point"""
    logger = Logger(
        name="Wattime run",
        level="DEBUG",
        description="Wattime run",
        trace_id=uuid.uuid4(),
    )
    integration_type = integration_type.strip().lower()
    with elapsed_timer() as ellapsed:
        if integration_type == "marginal":
            integration_cls = WatTimeMarginalEmissionsConnector
        elif integration_type == "average":
            integration_cls = WatTimeAverageEmissionsConnector
        else:
            logger.error(
                f"[Wattime] - Given wrong Wattime integration "
                f"type {integration_type}. Allowed vaulues is average or "
                "marginal. Exiting"
            )
            return None

        connector = integration_cls(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        logger.info("Run Completed.", extra={"labels": {"elapsed_time": ellapsed()}})
    return None


if __name__ == "__main__":

    METERS_AMOUNT = 1

    def run_wattime_local(integration_type: str) -> None:
        """Run Wattime integration locally"""
        logger = Logger(
            name="debug",
            level="DEBUG",
            description="Wattime debug",
            trace_id=uuid.uuid4(),
        )
        logger.info("Running Wattime integrations")
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            for fl_idx in range(METERS_AMOUNT):
                payload_file = CFG.LOCAL_PATH.joinpath(
                    f"participant_payload_{participant_id}_connector_"
                    f"wattime_{integration_type}_{fl_idx}.json"
                )

                if not payload_file.exists():
                    logger.warning(
                        f"WATTIME {integration_type}: Payload file "
                        f"'{payload_file}' does not exists. Skipping"
                    )
                    continue

                with open(payload_file, "r", encoding="utf-8") as exmpl_fl:
                    cfg = load(exmpl_fl)

                json_config = dumps(cfg).encode("utf-8")
                event_sample = {"data": base64.b64encode(json_config)}
                main(
                    event=event_sample, context=None, integration_type=integration_type
                )

    print("=" * 40)
    # import debugpy

    # debugpy.listen(CFG.DEBUG_PORT)
    # debugpy.wait_for_client()  # blocks execution until client is attached
    # debugpy.breakpoint()

    run_wattime_local("marginal")
    run_wattime_local("average")
