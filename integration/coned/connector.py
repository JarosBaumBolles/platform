""" ConEd integration module"""

import base64
import datetime
import math
import uuid
import xml.etree.ElementTree as ET
from abc import abstractmethod
from dataclasses import asdict
from json import JSONDecodeError, loads, load, dumps
from pathlib import Path
from typing import Optional

from dataclass_factory import Factory

from common import settings as CFG
from common.bucket_helpers import get_missed_standardized_files
from common.date_utils import format_date, parse, truncate
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.request_helpers import (
    HTTPRequestMethod,
    JBBRequestHelperException,
    PayloadType,
    http_request,
)
from integration.base_integration import BasePullConnector, MalformedConfig
from integration.coned.config import ConedCfg


class ConEdConnector(BasePullConnector):
    """Coned Integration"""

    __created_by__ = "ConEd Connector"
    __description__ = "ConEd Integration"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()
        self._config: Optional[ConedCfg] = None
        self._missed_hours = {}

    def get_missed_hours(self) -> None:
        """Get list of missed hours"""
        # Standardized data stored separately by type meter. It means that it
        # possible to miss different hours for each meter in a list.
        # As the result we need to check each given meter for misssed hour
        # and build index (relation) between missed hour and related meters

        self._logger.info("Matching missed hour.")
        with elapsed_timer() as elapsed:
            for mtr_cfg in self._config.meters:
                mtr_msd_poll_hrs = get_missed_standardized_files(
                    start_date=datetime.datetime.utcnow()
                    - datetime.timedelta(hours=12),
                    bucket_name=mtr_cfg.standardized.bucket,
                    bucket_path=mtr_cfg.standardized.path,
                    range_hours=self._config.gap_regeneration_window,
                )
                if mtr_msd_poll_hrs:
                    self._missed_hours[mtr_cfg.meter_name] = mtr_msd_poll_hrs
            self._logger.debug(
                "Matched missed hour.", extra={"labels": {"elapsed_time": elapsed()}}
            )
        self._logger.info("Matched missed hour.")

    def configure(self, conf_data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            try:
                js_config = self._before_configuration(conf_data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")
                self._config = self._factory.load(js_config, ConedCfg)

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

    def fetch_and_standardize(self) -> None:
        """Fetch and standardize"""
        with elapsed_timer() as elapsed:
            self._logger.info("Start fetching and standardizing data.")

            payload = {
                "grantType": "refresh_token",
                "clientId": self._config.client_id,
                "clientSecret": self._config.client_secret,
                "refreshToken": self._config.refresh_token,
                "subscriptionId": self._config.subscription_id,
            }
            headers = {
                "ocp-apim-subscription-key": self._config.subscription_key,
                "Content-Type": "application/json",
            }

            self._logger.warning("Refreshing token")
            self._logger.debug(f"Payload - {payload}")
            self._logger.debug(f"Headers - {headers}")

            response = http_request(
                "https://api.coned.com/gbc/v1/oauth/v1/Token",
                payload=payload,
                headers=headers,
                method=HTTPRequestMethod.POST,
                request_payload_type=PayloadType.JSON,
                response_payload_type=PayloadType.JSON,
            )

            self._logger.warning(f"Refreshed token - {response}")

            headers = {
                "ocp-apim-subscription-key": self._config.subscription_key,
                "Content-Type": "application/atom+xml",
                "Accept": "application/atom+xml",
            }
            headers["Authorization"] = f'Bearer {response["access_token"]}'

            processed_days = {()}

            meter_response = None

            for mtr_name, missed_hours in self._missed_hours.items():
                mtr_cfg = self._get_meter_config_by_name(mtr_name)
                if not mtr_cfg:
                    self._logger.error(
                        "Cannot find meter config for the given type "
                        f"{mtr_name}. Skipping..."
                    )

                mtr_cfg_json = asdict(mtr_cfg)

                for missed_hour in missed_hours:
                    current_time = truncate(parse(missed_hour), level="hour")
                    filename = format_date(current_time, CFG.PROCESSING_DATE_FORMAT)

                    value = 0

                    current_requested_time = current_time
                    published_max = current_requested_time.strftime("%Y-%m-%d")
                    published_min = (
                        current_requested_time - datetime.timedelta(hours=24)
                    ).strftime("%Y-%m-%d")

                    previous_requested_hour = (
                        current_requested_time - datetime.timedelta(hours=1)
                    )

                    if published_min not in processed_days:
                        try:
                            processed_days.add(published_min)
                            meter_response = http_request(
                                (
                                    "https://api.coned.com/gbc/v1/resource/Subscription/"
                                    f"{self._config.subscription_id}/UsagePoint/"
                                    f"{self._config.usage_point_id}/MeterReading/"
                                    f"{self._config.meter_reading_id}/IntervalBlock/"
                                    f"SP_{self._config.usage_point_id}_KWH%20"
                                    f"{self._config.interval}%20Minute%20Interval%20"
                                    "Read%20Interval"
                                ),
                                parameters={
                                    "publishedMin": published_min,
                                    "publishedMax": published_max,
                                },
                                headers=headers,
                            )
                        except JBBRequestHelperException:
                            self._logger.error(
                                "No data from historical API for "
                                f"{missed_hour}. Skipping..."
                            )
                            continue

                    required_intervals = [
                        math.floor(
                            previous_requested_hour.replace(
                                second=0, minute=0, microsecond=0
                            ).timestamp()
                        )
                    ]
                    interval = int(self._config.interval)
                    for _ in range((60 // interval) - 1):
                        required_intervals.append(
                            required_intervals[-1] + (interval * 60)
                        )

                    self._logger.info(required_intervals)

                    data_was_found = False

                    # TODO: Rewrite as XPath
                    root = ET.fromstring(meter_response)
                    for item in root:
                        if item.tag != "{http://www.w3.org/2005/Atom}entry":
                            continue
                        for item2 in item:
                            if item2.tag != "{http://www.w3.org/2005/Atom}content":
                                continue
                            for item3 in item2:
                                if item3.tag != "{http://naesb.org/espi}intervalBlocks":
                                    continue
                                for item4 in item3:
                                    if (
                                        item4.tag
                                        != "{http://naesb.org/espi}intervalBlock"
                                    ):
                                        continue
                                    for item5 in item4:
                                        if (
                                            item5.tag
                                            == "{http://naesb.org/espi}intervalReading"
                                        ):
                                            current_value = None
                                            applicable = False
                                            for item6 in item5:
                                                if (
                                                    item6.tag
                                                    == "{http://naesb.org/espi}value"
                                                ):
                                                    current_value = float(item6.text)
                                                elif (
                                                    item6.tag
                                                    == "{http://naesb.org/espi}timePeriod"
                                                ):
                                                    for item7 in item6:
                                                        if (
                                                            item7.tag
                                                            == "{http://naesb.org/espi}start"
                                                        ) and (
                                                            int(item7.text)
                                                            in required_intervals
                                                        ):
                                                            applicable = True
                                                            data_was_found = True
                                            if applicable:
                                                value += current_value

                    if data_was_found:
                        self._save_fetched_data(
                            blob_text=meter_response,
                            filename=filename,
                            meter_name=mtr_name,
                        )

                        mtr_cfg_json["start_date"] = current_time
                        mtr_cfg_json["end_date"] = current_time.add(
                            minutes=59, seconds=59
                        )

                        stndrdzd_mtr_data = self._standardize_generic(
                            data=None, getter=lambda x: value, mtr_cfg=mtr_cfg_json
                        )

                        self._save_standardized_data(
                            stndrdzd_mtr_data, filename, mtr_cfg
                        )

            self._logger.debug(
                "Completed fetching data.",
                extra={"labels": {"elapset_time": elapsed()}},
            )

    def run(self):
        super().run()
        self.get_missed_hours()
        self.fetch_and_standardize()
        self.save_update_status()

    @abstractmethod
    def fetch(self) -> None:
        """Fetch Integration data"""

    @abstractmethod
    def standardize(self) -> None:
        """Standardize Fetched data"""


def main(event, context):  # pylint:disable=unused-argument
    """Entry Point"""
    main_logger = Logger(
        name="Coned run",
        level="DEBUG",
        description="CONED RUN",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as ellapsed:
        connector = ConEdConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info(
            f"INFO: CONED INTEGRATION: Completed. Ellapsed time is {ellapsed()}"
        )


if __name__ == "__main__":
    CONNECTOR_NAME = "coned"
    METERS_QUANTITY = 12

    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description="CONED DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info("Running CONED integrations")
    import debugpy

    debugpy.listen(CFG.DEBUG_PORT)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()
    with elapsed_timer() as dbg_elapsed:
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            for call_idx in range(METERS_QUANTITY + 1):
                payload_file = CFG.LOCAL_PATH.joinpath(
                    f"participant_payload_{participant_id}_connector_"
                    f"{CONNECTOR_NAME}_{call_idx}.json"
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
