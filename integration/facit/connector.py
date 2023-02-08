""" Standardization phase - validation and normalization happens here. """
import base64
import uuid
from dataclasses import dataclass, field
from json import dumps, load, JSONDecodeError
from pathlib import Path
from typing import Any, Dict, Optional

from dataclass_factory import Factory
from google.cloud.exceptions import GoogleCloudError

import common.settings as CFG
from common.bucket_helpers import file_exists, move_blob, upload_file_to_bucket
from common.data_representation.config.participant import (
    ConfigException,
    ParticipantConfig,
    XmlTypeException,
)
from common.data_representation.standardized.meter import Meter as MeterValue
# from common.data_representation.standardized.meter import StandardizedMeterException
from common.date_utils import format_date, parse, truncate
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector, StorageInfo
from integration.facit.config import FacitCfg
from common.data_representation.config.meter import MeterData
from queue import Queue
from integration.facit.workers import FetchWorker, StandrdizeWorker
from collections import Counter

class Connector(BasePullConnector):
    "Facit Integration"

    __created_by__ = "Facit Connector"
    __description__ = "Facit Integration"    
    __name__ = "Facit Connector"

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        # self._willow_meter_cfg: Optional[MeterCfg] = None
        self._factory = Factory()
        self._config: Optional[FacitCfg] = None   
        self._meter_config: Optional[MeterData] = None   
        
        self._fetched_files_q: Queue = Queue()
        self._fetch_update_q: Queue = Queue()
        self._fetch_worker: Optional[FetchWorker] = None

        self._standardized_files: Queue = Queue()
        self._standardized_update_files: Queue = Queue()
        self._standardized_files_count: Counter = Counter()
        self._standardize_worker: Optional[StandrdizeWorker] = None   


    # TODO: Review ability to move this method in base class 
    def configure(self, conf_data: bytes) -> None:
        self._logger.debug("Loading configuration.")
        with elapsed_timer() as elapsed:
            try:
                js_config = self.parse_base_configuration(conf_data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")
                self._config = self._factory.load(js_config, FacitCfg)
            except (ValueError, TypeError, JSONDecodeError) as err:
                raise MalformedConfig from err

            self._meter_config = self._load_related_meter_config()
            
            self._fetch_worker = FetchWorker(
                missed_hours = Queue(), 
                fetched_files=self._fetched_files_q,
                fetch_update=self._fetch_update_q,
                config=self._config,
            )                

            self._standardize_worker = StandrdizeWorker(
                raw_files=self._fetched_files_q,
                standardized_files=self._standardized_files,
                standardize_update=self._standardized_update_files,
                config=self._config,
            )  

    def _load_related_meter_config(self) -> Optional[MeterData]:
        try:
            cfg = ParticipantConfig()
            cfg.read_from_bucket(
                bucket=self._config.willow_bucket,
                subdirectory=CFG.CONFIG_BASE_PATH,
                filename=CFG.PARTICIPANT_CONFIG_NAME,
                binary_mode=False,
            )

            self._logger.info("zsrg")
            cfg = cfg.as_json()
            for connector in cfg.get("connectors", []):
                for meter_uri, mtr_cfg in connector.get("meters", {}).items():
                    if meter_uri == self._config.willow_meter_uri:
                        return self._factory.load(mtr_cfg, MeterData)

            self._logger.error(
                f"Cannot find expected meter '{self._config.willow_meter_uri}' "
                f"in the given config 'gs://{self._config.willow_bucket}/"
                f"{CFG.CONFIG_BASE_PATH}/{CFG.PARTICIPANT_CONFIG_NAME}'"
            )
        except ConfigException as err:
            self._logger.error(
                "Can not parse the given participant configuration file "
                f"'gs://{self._config.willow_bucket}/{CONFIG_BASE_PATH}/'"
                f"{CFG.PARTICIPANT_CONFIG_NAME}' due to the error '{err}'"
            )        

    def validate_config(self) -> Optional[bool]:
        if self._config is None:
            self._logger.error(
                "The main configuration does not loaded yet. " 
                "Please run configuration before"
            )
            return False
        
        if self._meter_config is None:
            self._logger.error(
                "The related meter configuration does not loaded yet. " 
                "Please run configuration before"
            )
            return False

        if self._config.willow_bucket != self._meter_config.meteredDataLocation.bucket:
            self._logger.warning(
                f"The Facit bucket '{elf._config.willow_bucket}' is not equal "
                "with related meter configuration "
                f"{self._meter_config.meteredDataLocation.bucket}"
            )

        for mtr in self._config.meters:
            if mtr.meter_uri != self._meter_config.meter_uri:
                self._logger.error(
                    f"Related meter URI '{self._meter_config.meter_uri}' "
                    f"dos not match facit configuration {mtr.meter_uri}"
                )
                return False 

            if mtr.type != self._meter_config.type:
                self._logger.error(
                    f"Related meter type '{self._meter_config.type}' "
                    f"dos not match facit configuration {mtr.type}"
                )
                return False                     
            
        return True

    # TODO: @todo Redesign. Move to base class 
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
            self._logger.error(
                "The 'configure' method must be run before. Complete."
            )
            return None

        self._fetch_worker.run(self._run_time)  

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
        if not self.validate_config():
            self._logger.error(
                f"Cannot run '{self.__name__}' due to the configuration gaps."
                " Se logs for more information"
            )
            return None


        # self.get_missed_hours()
        self.fetch()
        self.standardize()

         

# class FacitConnector(BasePullConnector):
#     """Facit integration."""

#     folder_name = Path("FACIT")

#     __created_by__ = "Facit Connector"
#     __description__ = "Facit Integration"

#     __junk_folder__ = "junk"
#     __processed_folder__ = "processed"
#     __failed_folder__ = "failed"
#     __meter_uri__ = f"{CFG.PROJECT}_participant_3/config/meter_willow_1_occupancy.xml"
#     __destination_bucket__ = f"{CFG.PROJECT}_participant_3"
#     __willow_meter_uri__ = Path(f"{CFG.PROJECT}_participant_3").joinpath(
#         "config", "meter_willow_1_occupancy.xml"
#     )

#     def __init__(self, env_tz_info):
#         super().__init__(env_tz_info=env_tz_info)
#         self._willow_meter_cfg: Optional[MeterCfg] = None
#         self._factory = Factory()
#         self._config: Optional[FacitConfig] = None

#     def _standardize(self, meter_data, cfg: MeterCfg) -> Optional[str]:
#         """Data validation and normalization"""

#         meter_data.meter_uri = cfg.meter_uri
#         filename = format_date(
#             truncate(parse(meter_data.end_time), level="hour"),
#             CFG.STANDARDIZED_METER_DATE_FORMAT,
#         )

#         try:
#             upload_file_to_bucket(
#                 bucket_name=cfg.meteredDataLocation.bucket,
#                 blob_text=meter_data.as_str(),
#                 blob_path=cfg.meteredDataLocation.path,
#                 file_name=filename,
#             )
#             return filename
#         except GoogleCloudError as err:
#             self._logger.error(
#                 f"Cannot save standardized meter data hour '{filename}' "
#                 f"by path 'gs://{cfg.meteredDataLocation.bucket}/"
#                 f"{cfg.meteredDataLocation.path}/{filename} due to the error "
#                 f"'{err}'"
#             )

#         return filename

#     def _move_to_folder(self, folder: str) -> None:
#         with elapsed_timer() as elapsed:
#             file_path = Path(self._config.name)
#             dest_path = Path(self.folder_name).joinpath(folder, file_path.name)

#             self._logger.debug(
#                 f"Moving file gs://{self._config.bucket}/{self._config.name}"
#                 f" to gs://{self._config.bucket}/{dest_path}"
#             )

#             move_blob(
#                 bucket_name=self._config.bucket,
#                 blob_name=self._config.name,
#                 destination_bucket=self._config.bucket,
#                 new_blob_name=str(dest_path),
#                 quiet=True,
#             )
#             self._logger.debug(
#                 "Moved file to the given folder.",
#                 extra={"labels": {"elapsed time": elapsed()}},
#             )

#     def _move_to_junk(self):
#         self._move_to_folder(self.__junk_folder__)

#     def _move_to_failed(self):
#         self._move_to_folder(self.__failed_folder__)

#     def _move_to_processed(self):
#         self._move_to_folder(self.__processed_folder__)

#     def _save_standardize_update_status(  # pylint: disable=arguments-differ
#         self, file_name: str, cfg: MeterCfg
#     ) -> None:
#         with elapsed_timer() as elapsed:
#             self._logger.debug("Saving standardize update status.")
#             update_body = {
#                 "files": [
#                     {
#                         "bucket": cfg.meteredDataLocation.bucket,
#                         "path": cfg.meteredDataLocation.path,
#                         "filename": file_name,
#                     }
#                 ]
#             }
#             base_file_name = format_date(
#                 self._run_time, CFG.FACIT_PROCESSING_DATE_FORMAT
#             )

#             filename = CFG.UPDATE_FILENAME_TMPL.format(
#                 update_prefix=self.__update_prefix__, cnt=0, run_date=base_file_name
#             )
#             self._upload_update_status_file(
#                 raw_data=update_body,
#                 str_bucket=cfg.meteredDataLocation.bucket,
#                 str_path=str(
#                     Path(cfg.meteredDataLocation.path).joinpath(self.__update_prefix__)
#                 ),
#                 filename=filename,
#             )

#             self._logger.debug(
#                 "Standardized update status has been saved.",
#                 extra={"labels": {"elapsed time": elapsed()}},
#             )

#     def _upload_update_status_file(  # pylint: disable=arguments-differ
#         self, raw_data: dict, str_bucket: str, str_path: str, filename: str
#     ) -> None:
#         with elapsed_timer() as ellapsed:
#             self._logger.debug(
#                 f"Saving update status to file {str_bucket}/{str_path}/{filename}"
#             )
#             try:
#                 upload_file_to_bucket(
#                     str_bucket,
#                     dumps(raw_data, sort_keys=True, indent=4),
#                     blob_path=str_path,
#                     file_name=filename,
#                 )
#             except GoogleCloudError as err:
#                 self._logger.error(
#                     f"Cannot save update status for '{filename}' hour in "
#                     f"{str_bucket}/{str_path}/{filename} due to the error '{err}'"
#                 )
#             self._logger.debug(
#                 f"Saved update status to file {str_bucket}/{str_path}/{filename}.",
#                 extra={"labels": {"elapsed_time": ellapsed()}},
#             )

#     def run(self) -> None:
#         super().run()
#         file_path = Path(self._config.name)
#         meter_data = MeterValue()

#         try:
#             if not self._config.name.startswith(f"{self.folder_name}/occupancy_"):
#                 raise FacitRunTimeError(
#                     f"The uploaded file name 'gs://{self._config.bucket}/"
#                     f"{self._config.name}' does not match expected template "
#                     f"'gs://{self._config.bucket}/{self.folder_name}/occupancy_'."
#                     f" Moving to 'gs://{self._config.bucket}/{self.folder_name}/"
#                     f"{self.__junk_folder__}/'"
#                 )

#             is_cfg_exist = file_exists(
#                 bucket=self._config.bucket,
#                 subdirectory=str(file_path.parent),
#                 file_name=file_path.name,
#             )
#             if not is_cfg_exist:
#                 self._logger.error(
#                     f"The given file gs://{self._config.bucket}/"
#                     f"{self._config.name} does not exist. Exit"
#                 )
#                 return None

#             meter_data.read_from_bucket(
#                 bucket=self._config.bucket,
#                 subdirectory=str(file_path.parent),
#                 filename=file_path.name,
#                 binary_mode=False,
#             )
#         except (
#             StandardizedMeterException,
#             XmlTypeException,
#             ConfigException,
#         ) as err:
#             self._logger.error(
#                 f"Can not parse file 'gs://{self._config.bucket}/"
#                 f"{file_path}' due to the error '{err}'. Skipping"
#             )
#             self._move_to_failed()
#         except FacitRunTimeError as err:
#             self._logger.warning(f"Moving to junk due to the reason '{err}'")
#             self._move_to_junk()
#         else:
#             file_name = self._standardize(meter_data, self._willow_meter_cfg)
#             self._move_to_processed()
#             self._save_standardize_update_status(file_name, self._willow_meter_cfg)
#         return None

#     def standardize(self) -> None:
#         """Standardize Fetched Data"""

#     def fetch(self) -> Any:
#         """Fetch data from data source"""


def main(event, context) -> None:  # pylint: disable=unused-argument
    """Entry point"""
    connector_name = "facit"
    main_logger = Logger(
        name=f"{connector_name} run",
        level="DEBUG",
        description=f"{connector_name}",
        trace_id=uuid.uuid4(),
    )
    main_logger.info(f"Running {connector_name} connector")
    connector = Connector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
    connector.configure(event)
    connector.run()
    # bucket = event.get("bucket", "").strip()
    # expected_path = Path(FacitConnector.folder_name)
    # file_path = Path(event.get("name", "").strip())

    # try:
    #     file_path.parent.relative_to(expected_path)
    #     is_processing = bool(expected_path == file_path.parent)

    # except ValueError:
    #     is_processing = False

    # if bucket != CFG.LANDING_ZONE_BUCKET or not is_processing:
    #     main_logger.warning(
    #         f"Detected false positive event fot file 'gs://{bucket}/{file_path}."
    #         "Exit."
    #     )
    # else:
    #     with elapsed_timer() as elapsed:
    #         try:
    #             connector = FacitConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
    #             connector.configure(event)
    #             connector.run()
    #         except FacitRunTimeError as err:
    #             main_logger.error(
    #                 f"Cannot to run {connector_name} dur to the recieved error"
    #                 f" '{err}'"
    #             )
    #     main_logger.info(
    #         f"{connector_name} Completed.",
    #         extra={"labels": {"elapsed time": elapsed()}},
    #     )


if __name__ == "__main__":
    CONNECTOR_NAME = "facit"
    METERS_AMOUNT = 1    


    debug_logger = Logger(
        name="debug",
        level="DEBUG",
        description=f"{CONNECTOR_NAME} DEBUG",
        trace_id=uuid.uuid4(),
    )
    debug_logger.info(f"Running {CONNECTOR_NAME} integrations")
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
            "Completed integration", extra={"labels": {"elapsed_time": dbg_elapsed()}}
        )