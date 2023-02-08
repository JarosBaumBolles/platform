"""Orion Integration functionality"""
import base64
import csv
import tempfile
import uuid
from dataclasses import dataclass, field
from json import dumps, load
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import List

from dataclass_factory import Factory

from common import settings as CFG
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePushConnector, StorageInfo


class MalformedConfig(Exception):
    """Exception class specific to this package."""


class RawFileValidationError(Exception):
    """Exception class specific to this package."""


class FileNotExists(Exception):
    """Exception class specific to this package."""


@dataclass
class MeterCfg:
    """Meter config representation"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class FetchStrategy:
    """Fetch starategy"""

    type: str = ""
    description: str = ""


@dataclass
class ExtraInfo:
    """Extra config parameters"""

    raw: StorageInfo = field(default_factory=StorageInfo)
    participant_id: int = -1
    timezone: str = ""
    fetch_strategy: FetchStrategy = field(default_factory=FetchStrategy)


@dataclass
class OrionCfg:
    """Orion Config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)


class OrionConnector(BasePushConnector):
    """Contains functionality to load Orion data"""

    __created_by__ = "Orion Connector"
    __description__ = "Orion Integration"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()

    def configure(self, conf_data: bytes) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Loading configuration.")
            try:
                js_config = self.parse_base_configuration(conf_data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")

                self._config = self._factory.load(js_config, OrionCfg)
            except (ValueError, TypeError, JSONDecodeError) as err:
                raise MalformedConfig from err

            self._logger.debug(
                "Loaded participant configuration.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def fetch(self, working_directory: tempfile.TemporaryDirectory) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Fetching new files.")

            self._fetch_files(
                bucket=self._config.extra.raw.bucket,
                preffix=self._config.extra.raw.path,
                working_directory=working_directory,
            )

            self._logger.debug(
                f"Loaded {len(self._fetched_files)} new files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    @staticmethod
    def _csv_read_validate(file_path: str) -> List[List[str]]:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotExists(f"The local file {file_path} dos not exists.")

        try:
            with open(file_path, mode="r", encoding="utf-8") as csv_fl:
                csv_reader = csv.reader(
                    csv_fl,
                    delimiter=",",
                    quotechar=",",
                    quoting=csv.QUOTE_MINIMAL,
                )

                csv_data = list(csv_reader)
        except (UnicodeDecodeError, FileNotFoundError) as err:
            raise RawFileValidationError(  # pylint:disable=raise-missing-from
                f"Can not read raw {file_path} file due to the error {err}"
            )

        if len(csv_data) < 2:
            raise RawFileValidationError(
                f"CSV file {file_path} does not coresponde to Orion template."
                "File must contains minimum 2 rows with data"
            )

        first_row = csv_data[0]

        if len(first_row) != 2:
            raise RawFileValidationError(
                f"CSV file {file_path} does not coresponde to Orion template."
                "First row must contains only 2 elements."
            )

        if first_row[0].strip() != "date":
            raise RawFileValidationError(
                f"CSV file {file_path} does not coresponde to Orion template."
                f"First row field name {first_row[0]} is not equal to 'date'"
            )

        second_row = csv_data[1]

        if len(second_row) != 5:
            raise RawFileValidationError(
                f"CSV file {file_path} does not coresponde to Orion template."
                "Second row must contains 5 elements."
            )

        if second_row[0].strip() != "total_in_building":
            raise RawFileValidationError(
                f"CSV file {file_path} does not coresponde to Orion template."
                f"Second row first field name {second_row[0]} is not "
                f"equal with 'total_in_building'"
            )

        if second_row[2].strip() != "Correction Factor":
            raise RawFileValidationError(
                f"CSV file {file_path} does not coresponde to Orion template."
                f"Second row second field name {second_row[2]} is not "
                f"equal with 'Correction Factor'"
            )

        return csv_data

    def standardize(self, working_directory: tempfile.TemporaryDirectory) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Standardizing new files.")

            if not self._fetched_files:
                self._logger.error("Absent fetched data. Run fetch before")
            else:
                # TODO: Debug code below needed to write paralel_upload run

                for fl_info in self._fetched_files:
                    try:
                        csv_data = self._csv_read_validate(fl_info["local"])
                    except (FileNotExists, RawFileValidationError) as err:
                        self._logger.error(
                            f"During parsing file {fl_info['local']} "
                            f"recived error {err}"
                        )
                        self._fetched_junk_files.append(
                            {
                                "bucket": fl_info["destination_bucket"],
                                "preffix": fl_info["destination_preffix"],
                                "filename": fl_info["destination_filename"],
                                "destination_bucket": fl_info["destination_bucket"],
                                "destination_preffix": (
                                    f"{fl_info['preffix'].rstrip('/')}"
                                    f"{self.__raw_junk_files_preffix__}"
                                ),
                                "destination_filename": fl_info["destination_filename"],
                                "local": fl_info["local"],
                            }
                        )
                        continue

                    for meter_info in self._config.meters:
                        mtr_type = meter_info.type.strip().lower().replace(" ", "_")
                        stndrdz_mthd_nm = f"_standardize_{mtr_type}"
                        stndrdz_func = getattr(self, stndrdz_mthd_nm, "")
                        if not callable(stndrdz_func):
                            self._logger.error(
                                f"Cannot find standadize processor for the "
                                f"given meter type {meter_type}. Skipping"
                            )
                            continue

                        stndrdzd_mtr_data = stndrdz_func(csv_data, meter_info)
                        meter_type = meter_info.type.strip()

                        self._standardized_files[meter_type].append(
                            {
                                "meter_type": meter_info.type.strip(),
                                "bucket": meter_info.standardized.bucket,
                                "preffix": meter_info.standardized.path,
                                "filename": format_date(
                                    stndrdzd_mtr_data.start_time,
                                    CFG.PROCESSING_DATE_FORMAT,
                                ),
                                "file_body": stndrdzd_mtr_data.as_str(),
                            }
                        )
                        self._standardized_files_count[meter_type] += 1

            self._upload_standardized_to_buckets(
                self._standardized_files,
                worker_replica=self.__default_worker_replica_amount__,
            )

            self._logger.debug(
                f"Processed {sum(self._standardized_files_count.values())} "
                f"standardize files.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _standardize_occupancy(self, data: List[List[str]], mtr_cfg: dict) -> Meter:
        def getter(xdata: dict):
            return xdata["usage"]

        measurement_time, usage = [row[1] for row in data][:2]
        start_date = truncate(parse(measurement_time), level="hour")

        meter_cfg = {
            "start_date": start_date,
            "end_date": start_date.add(minutes=59, seconds=59),
            "meter_uri": mtr_cfg.meter_uri,
        }
        return self._standardize_generic(
            data={
                "usage": int(usage),
            },
            getter=getter,
            mtr_cfg=meter_cfg,
        )

    def run(self) -> None:
        super().run()
        with tempfile.TemporaryDirectory() as working_directory:
            self.fetch(working_directory)
            self.standardize(working_directory)
            self.save_update_status()


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
            connector.run()
        except MalformedConfig as err:
            main_logger.error(f"Unexpectedly closed due to the error '{err}'")
        finally:
            main_logger.info(
                "Completed.", extra={"labels": {"elapsed_time": elapsed()}}
            )


if __name__ == "__main__":
    CONNECTOR_NAME = "orion"

    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description="ORION DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info("Running ORION integrations")
    with elapsed_timer() as dbg_elapsed:
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            payload_file = CFG.LOCAL_PATH.joinpath(
                f"participant_payload_{participant_id}_connector_"
                f"{CONNECTOR_NAME}_0.json"
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
