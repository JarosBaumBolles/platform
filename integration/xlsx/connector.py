"""XLSX Integration"""
import json
import tempfile
import uuid
from dataclasses import dataclass, field
from json import dumps, load
from pathlib import Path
from typing import List

import pandas as pd
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


class FileParsingError(Exception):
    """Exception class specific to this package."""


@dataclass
class MeterCfg:
    """Meter Config"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class FetchStrategy:
    """Fetch Strategy"""

    type: str = ""
    description: str = ""


@dataclass
class ExtraInfo:
    """Extra Info"""

    raw: StorageInfo = field(default_factory=StorageInfo)
    participant_id: int = -1
    timezone: str = ""
    fetch_strategy: FetchStrategy = field(default_factory=FetchStrategy)


@dataclass
class XLSXCfg:
    """Excell Config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)


class XLSXConnector(BasePushConnector):
    """Contains functionality to load XLSX data"""

    __created_by__ = "XLSX Connector"
    __description__ = "XLSX Integration"
    __xlsx_sheet_name__ = "Sheet1"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()
        # self._config: Optional[OrionCfg] = None

    def configure(self, data: bytes) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Loading configuration.")
            try:
                js_config = self._before_configuration(data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")

                self._config = self._factory.load(js_config, XLSXCfg)
            except (ValueError, TypeError, json.decoder.JSONDecodeError) as err:
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

    def _xlsx_read_validate(self, file_path: str) -> pd.DataFrame:

        try:
            dataframe = pd.read_excel(file_path, sheet_name=self.__xlsx_sheet_name__)
        except FileParsingError as err:
            raise FileParsingError(  # pylint:disable=raise-missing-from
                f"Can not read raw {file_path} file due to the error {err}"
            )

        columns = dataframe.columns.values.tolist()
        if "Time" not in columns:
            raise RawFileValidationError(
                f"Time column should be present in file {file_path} due to template."
            )

        if "Value" not in columns:
            raise RawFileValidationError(
                f"Value column should be present in file {file_path} due to template."
            )

        return dataframe

    def standardize(self, working_directory: tempfile.TemporaryDirectory) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Standardizing new files.")

            if not self._fetched_files:
                self._logger.error("Absent fetched data. Run fetch before")
            else:
                for _, fl_info in enumerate(self._fetched_files, 1):
                    self._logger.info(f"FILE INFO {fl_info}")

                    try:
                        dfs = self._xlsx_read_validate(fl_info["local"])
                    except (FileNotExists, RawFileValidationError) as err:
                        self._logger.error(
                            f"During parsing file {fl_info['local']} recived "
                            f"error {err}"
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

                    for _, row in dfs.iterrows():
                        self._logger.info(
                            f"TIME AND VALUE IN ROW {row['Time']}, {row['Value']}"
                        )

                        for _, meter_info in enumerate(self._config.meters, 1):

                            start_date = truncate(parse(row["Time"]), level="hour")
                            value = row["Value"]

                            meter = Meter()
                            meter.created_date = self._run_time
                            meter.start_time = start_date
                            meter.end_time = start_date.add(minutes=59, seconds=59)
                            meter.created_by = self.__created_by__
                            meter.usage = value
                            meter.meter_uri = meter_info.meter_uri

                            meter_type = meter_info.type.strip()
                            self._standardized_files[meter_type].append(
                                {
                                    "meter_type": meter_info.type.strip(),
                                    "bucket": meter_info.standardized.bucket,
                                    "path": meter_info.standardized.path,
                                    "filename": format_date(
                                        start_date, CFG.PROCESSING_DATE_FORMAT
                                    ),
                                    "file_body": meter.as_str(),
                                }
                            )
                            self._standardized_files_count[meter_type] += 1
            self._upload_standardized_to_buckets(
                self._standardized_files,
                worker_replica=20,
            )

            self._logger.debug(
                f"Processed {sum(self._standardized_files_count.values())} "
                "standardize files.",
                extra={"labels": {"elapsed_time": elapsed()}},
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
        description="XLSX DEBUG",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        try:
            connector = XLSXConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
            connector.configure(event)
            connector.run()
        except MalformedConfig as err:
            main_logger.error(f"Unexpectedly closed due to the error '{err}'")
        finally:
            main_logger.info(
                "Completed.", extra={"labels": {"elapsed_time": elapsed()}}
            )


if __name__ == "__main__":
    import base64

    CONNECTOR_NAME = "xlsx"

    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description="XLSX DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info("Running XLSX integrations")
    with elapsed_timer() as dbg_elapsed:
        for participant_id in CFG.DEBUG_PARTICIPANTS:
            payload_file = CFG.LOCAL_PATH.joinpath(
                f"participant_payload_{participant_id}_connector_"
                f"{CONNECTOR_NAME}.json"
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
