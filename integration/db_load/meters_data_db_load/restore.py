"""Integration used to load all standardized data into Data Warehouse

    MVP. At the moment contains simliest sync approach.
    All operations run one by one without any paralelization approach.
    This way used to provide first deliverable MVP asap/ In next iteration code
    below should be redesigned aith asyncio approarch.
"""
import base64
import logging
from dataclasses import dataclass, field
from json import dumps, loads

from google.api_core.exceptions import BadRequest
from google.cloud.exceptions import GoogleCloudError

from common import settings as CFG
from common.bucket_helpers import list_blobs_with_prefix, move_blob
from common.data_representation.config import XmlTypeException, ConfigException
from common.data_representation.standardized.meter import Meter as MeterValue, StandardizedMeterException
from common.date_utils import format_date
from common.db_helper import get_db_connection
from common.elapsed_time import elapsed_timer
from common.request_helpers import retry
from common.sql_templates import generate_insert_sql_meters_data
from integration.base_integration import (
    BasePullConnector,
    GeneralInfo,
    MeterConfig,
    Meters,
    StorageInfo,
)
from integration.base_integration.base import ConnectorConfigException


class DwLoadException(Exception):
    """Exception class specific to this package."""


class DwStopOperationException(Exception):
    """Exception class specific to this package."""


class DwSkipOperationException(Exception):
    """Exception class specific to this package."""


@dataclass
class FileInfo:
    """File info structure"""

    filename: str = ""
    storage: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class UpdatesBucket:
    """Structure to store updates information"""

    count: int = 0
    updates: list = field(default_factory=list)


@dataclass
class Update:
    """Single Update information"""

    meter_uri: str = ""
    meta_info: FileInfo = field(default_factory=FileInfo)
    files: list = field(default_factory=list)


class DwLoadConnector(BasePullConnector):
    """Contains functionality to  load JBB standardized files after latest update into DW"""

    __created_by__ = "DW Load Restore Connector"
    __description__ = "DW Load Restore Integration"
    __bucket_limit__ = CFG.DW_LOAD_FILES_BUCKET_LIMIT
    __processed_prefix__ = "processed"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._cfg_meters = Meters()
        self._cfg_general = GeneralInfo()
        self._updates_bucket = UpdatesBucket()
        self._historic_update_prefix = "historic_update"
        self._files_to_upload = []

    @staticmethod
    def _get_dw_connection():
        return get_db_connection()

    def configure(self, conf_data: bytes) -> None:
        with elapsed_timer() as elapsed:
            logging.debug(f"DEBUG: {self.__description__}: Loading configuration.")
            js_config = self.parse_base_configuration(conf_data)
            self._historic_update_prefix += f'_{js_config["function_id"]}'

            meters_list = js_config.get("meters", [])
            if not meters_list:
                raise DwLoadException("Configuration does not contain meters data")

            self._cfg_general.participant_id = int(js_config["extra"]["participant_id"])

            for mtr in meters_list:
                mtr_cfg = MeterConfig()
                mtr_cfg.storage = StorageInfo()
                mtr_cfg.storage.bucket = mtr["extra"]["standardized"]["bucket"]
                mtr_cfg.storage.path = mtr["extra"]["standardized"]["path"]
                mtr_cfg.meter_type = mtr["type"]
                mtr_cfg.meter_uri = mtr["meter_uri"]

                self._cfg_meters.meters[mtr_cfg.meter_uri.strip().lower()] = mtr_cfg
                logging.debug(
                f"{self._cfg_meters.meters}"
                f"{self._cfg_meters.meters.values()}"
                )
            logging.debug(
                f"DEBUG: {self.__description__}: Loaded configuration. "
                f"Ellapsed time is {elapsed()}"
            )

    def _process_mtr_update_statuses(self, meter_config: MeterConfig) -> None:

        with elapsed_timer() as ellapsed:
            logging.debug(
                f"DEBUG: {self.__description__}: Start processing "
                f"{meter_config.meter_uri} meter. Meter standirdized data is "
                f"located at path {meter_config.storage.bucket.rstrip('/')}/"
                f"{meter_config.storage.path}"
            )

            # for mtr_cfg in self._cfg_meters.meters.values():
            mtr_path = f"{meter_config.storage.path}/{self.__update_prefix__}"

            update_files = list_blobs_with_prefix(
                bucket_name=meter_config.storage.bucket,
                prefix=f"{mtr_path}/{self._historic_update_prefix}_",
                force_dir=False,
            )

            if not update_files:
                raise DwSkipOperationException(
                    f"The meter {meter_config.meter_type} does not contain updates."
                )

            for updt_file in update_files:
                filename = updt_file.name.split("/")[-1]
                try:
                    self._process_mtr_update_file(
                        filename=filename, meter_config=meter_config
                    )
                except DwSkipOperationException as err:
                    logging.warning(f"WARNING: {self.__description__}: {err}. Skipping")
                    file_info = FileInfo()
                    file_info.filename = filename
                    file_info.storage.bucket = meter_config.storage.bucket
                    file_info.storage.path = meter_config.storage.path
                    self._move_to_processed(updt_file=file_info)

            logging.debug(
                f"DEBUG: {self.__description__}: Completed processing "
                f"{meter_config.meter_uri} meter. "
                f"Ellapsed time is {ellapsed()}"
            )

    def _process_mtr_update_file(
        self, filename: str, meter_config: MeterConfig
    ) -> None:
        with elapsed_timer() as ellapsed:
            logging.debug(
                f"DEBUG: {self.__description__}: Processing update status file "
                f"{meter_config.storage.bucket.rstrip('/')}"
                f"{meter_config.storage.path}/{filename}"
            )

            mtr_path = f"{meter_config.storage.path}/{self.__update_prefix__}"

            updt = Update()
            updt.meter_uri = meter_config.meter_uri.strip().lower()
            updt.meta_info.filename = filename
            updt.meta_info.storage.bucket = meter_config.storage.bucket
            updt.meta_info.storage.path = mtr_path

            raw_data = self._load_json_data(
                bucket=updt.meta_info.storage.bucket,
                path=updt.meta_info.storage.path,
                filename=updt.meta_info.filename,
            )

            if not raw_data or not raw_data.get("files", []):
                raise DwSkipOperationException(
                    "The update status file "
                    f"{meter_config.storage.bucket.rstrip('/')}/"
                    f"{meter_config.storage.path}/{filename} is empty or "
                    "malformed."
                )

            stg_files = raw_data.get("files", [])
            bckt_cnt = len(stg_files)

            if self._updates_bucket.count + bckt_cnt > self.__bucket_limit__:
                raise DwStopOperationException(
                    f"Riched out processing bucket limit {self.__bucket_limit__}."
                )
            for std_fl in stg_files:
                stg_fl = FileInfo()
                stg_fl.filename = std_fl["filename"]
                stg_fl.storage.bucket = std_fl["bucket"]
                stg_fl.storage.path = std_fl["path"]
                updt.files.append(stg_fl)

            self._updates_bucket.updates.append(updt)
            self._updates_bucket.count += bckt_cnt

            logging.debug(
                f"DEBUG: {self.__description__}: Completed processing update"
                f" status file {meter_config.storage.bucket.rstrip('/')}"
                f"{meter_config.storage.path}/{filename}. "
                f"Ellapsed time is {ellapsed()}"
            )

    def load_update_info(self) -> None:
        """Retrive and prepare information about latest updates"""
        with elapsed_timer() as ellapsed:
            logging.info(f"INFO: {self.__description__}: Loading update status.")
            self._updates_bucket.count = 0
            self._updates_bucket.files = []

            try:
                for mtr_cfg in self._cfg_meters.meters.values():
                    try:
                        self._process_mtr_update_statuses(meter_config=mtr_cfg)
                    except DwSkipOperationException as err:
                        logging.warning(f"WARNING: {self.__description__}: {err}")
            except DwStopOperationException as err:
                logging.warning(
                    f"WARNING: {self.__description__}: {err}. "
                    "All other updates will be processed in one hour."
                )
            logging.info(
                f"INFO: {self.__description__}: Loaded update status. "
                f"Ellapsed time is {ellapsed()}"
            )

    @retry(BadRequest)
    def _db_load(self, connection, data: MeterValue) -> None:
        cursor = connection.cursor()

        ref_hour_id = format_date(data.start_time, CFG.HOUR_ID_DATE_FORMAT)
        cursor.execute(
            (f"{generate_insert_sql_meters_data(CFG.PROJECT)}"),
            (
                # Insert parameters for meter_data
                int(ref_hour_id),
                self._cfg_general.participant_id,
                int(data.meter_id),
                float(data.usage),
            ),
        )

    def _process_new_standardized_files(self) -> None:
        if not self._updates_bucket.updates:
            logging.warning(
                f"WARNING: {self.__description__}: Latest updates are absent "
                "or not loaded. Exiting"
            )

        dw_connection = self._get_dw_connection()

        for updt_bulk in self._updates_bucket.updates:

            mtr_cfg = self._cfg_meters.meters.get(updt_bulk.meter_uri)

            for blk_fl in updt_bulk.files:
                logging.debug(
                    f"DEBUG: {self.__description__}: Start processing standardized file "
                    f"{blk_fl.storage.bucket}/{blk_fl.storage.path}/"
                    f"{blk_fl.filename}"
                )

                meter_data = MeterValue()
                try:
                    meter_data.read_from_bucket(
                        bucket=blk_fl.storage.bucket,
                        subdirectory=blk_fl.storage.path,
                        filename=blk_fl.filename,
                        binary_mode=False,
                    )
                except (
                    StandardizedMeterException,
                    XmlTypeException,
                    ConfigException,
                ) as err:
                    logging.error(
                        f"ERROR: {self.__description__}: Can not parse file"
                        f"'{blk_fl.storage.bucket}/{blk_fl.storage.path}/"
                        f"{blk_fl.filename}' due to the error '{err}'. Skiping"
                    )
                    continue

                logging.debug("meter_data.meter_uri", meter_data.meter_uri)
                logging.debug("mtr_cfg.meter_uri", mtr_cfg.meter_uri)

                if meter_data.meter_uri != mtr_cfg.meter_uri:
                    logging.warning(
                        f"WARNING: {self.__description__}: "
                        f"Meter uri {meter_data.meter_uri} found in the file "
                        f"{blk_fl.storage.bucket}/{blk_fl.storage.path}"
                        f"does not equal to uri in correponded meter configuration."
                        f"{mtr_cfg.meter_uri}. Replacing"
                    )
                    meter_data.meter_uri = mtr_cfg.meter_uri

                logging.debug(
                    f"DEBUG: {self.__description__}: Completed processing standardized file "
                    f"{blk_fl.storage.bucket}/{blk_fl.storage.path}/"
                    f"{blk_fl.filename}"
                )

                logging.debug(
                    f"DEBUG: {self.__description__}: Start db loading of procesed data from "
                    f"{blk_fl.storage.bucket}/{blk_fl.storage.path}/"
                    f"{blk_fl.filename}"
                )
                self._db_load(
                    connection=dw_connection,
                    data=meter_data,
                )
                logging.debug(
                    f"DEBUG: {self.__description__}: Completed db loading of procesed data from "
                    f"{blk_fl.storage.bucket}/{blk_fl.storage.path}/"
                    f"{blk_fl.filename}"
                )
            self._move_to_processed(updt_file=updt_bulk.meta_info)
        dw_connection.close()

    def process_new_standardized_files(self) -> None:
        """Load data from latest standardized files into DW"""
        with elapsed_timer() as ellapsed:
            logging.info(f"INFO: {self.__description__}: Loading new standardized files.")
            self._process_new_standardized_files()
            logging.info(
                f"INFO: {self.__description__}: Completed Loading new "
                f"standardized files. Ellapsed time is {ellapsed()}"
            )

    def _move_to_processed(self, updt_file: FileInfo) -> None:

        with elapsed_timer() as ellapsed:
            logging.info(
                f"INFO: {self.__description__}: Moving processed update file."
                f"{updt_file.storage.bucket}/{updt_file.storage.path}/"
                f"{updt_file.filename}."
            )
            if not updt_file.filename.startswith(self._historic_update_prefix):
                logging.error(
                    f"ERROR: {self.__description__}: The given file"
                    f"{updt_file.storage.bucket}/{updt_file.storage.path}/"
                    f"{updt_file.filename} is not update status file or "
                    "already processed. Skipping"
                )
            else:

                exist_path = f"{updt_file.storage.path}/{updt_file.filename}"
                exist_path = exist_path.lstrip("/")

                dest_filename = updt_file.filename.replace(
                    self._historic_update_prefix, self.__processed_prefix__
                )
                dest_path = f"{updt_file.storage.path}/{dest_filename}"
                dest_path = dest_path.lstrip("/")
                try:
                    move_blob(
                        bucket_name=updt_file.storage.bucket,
                        blob_name=exist_path,
                        destination_bucket=updt_file.storage.bucket,
                        new_blob_name=dest_path,
                    )
                except GoogleCloudError as err:
                    logging.error(
                        f"ERROR: {self.__description__}: Cannot move"
                        f" file {exist_path} file to {dest_path} due to the error"
                        f"{err}"
                    )
            logging.info(
                f"INFO: {self.__description__}: Moved processed update file."
                f"{updt_file.storage.bucket}/{updt_file.storage.path}/"
                f"{updt_file.filename}. Ellapsed time is {ellapsed()}"
            )

    def run(self) -> None:
        with elapsed_timer() as ellapsed:
            logging.info(f"INFO: {self.__description__}: Start db load processing.")
            self.load_update_info()
            self.process_new_standardized_files()
            logging.info(
                f"INFO: {self.__description__}: Completed db load processing. "
                f"Ellapsed time is {ellapsed()}"
            )

    def fetch(self):
        pass

    def standardize(self):
        pass


def main(event, context):
    """Entry point"""
    with elapsed_timer() as ellapsed:
        connector = DwLoadConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        logging.info(f"INFO: DW LOAD INTEGRATION: Completed. Ellapsed time is {ellapsed()}")
