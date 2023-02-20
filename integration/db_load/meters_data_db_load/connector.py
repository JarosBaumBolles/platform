"""Integration used to load all standardized data into Data Warehouse"""
import base64
import uuid
from json import JSONDecodeError, dumps
from queue import Queue
from time import time
from typing import Any, Dict, List, Optional

from dataclass_factory import Factory
from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.cloud.bigquery import (
    Client,
    LoadJobConfig,
    PolicyTagList,
    SchemaField,
    SourceFormat,
    Table,
)
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from pandas import DataFrame

from common import settings as CFG
from common.db_helper import get_db_connection
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from integration.base_integration import BasePullConnector
from integration.db_load.meters_data_db_load.config import DbLoadCfg
from integration.db_load.meters_data_db_load.exceptions import (
    LoadJsonError,
    MalformedConfig,
)
from integration.db_load.meters_data_db_load.workers import (
    FinalizeUpdatesWorker,
    LoadUpdatesdataWorker,
    LoadUpdatesFilesWorker,
)

LOCAL_RUN = False
SECRET_PATH = str(CFG.LOCAL_PATH.joinpath("bq_secret.json"))
SCOPE = ["https://www.googleapis.com/auth/bigquery"]


class DwLoadConnector(BasePullConnector):
    """Contains functionality to load standardized files after latest update into DW"""

    __created_by__ = "DW Load Connector"
    __description__ = "DW Load Integration"
    __bucket_limit__ = CFG.DW_LOAD_FILES_BUCKET_LIMIT
    __update_filename_tmpl__ = CFG.UPDATE_FILENAME_TMPL
    __destination_tbl_name__ = "meters_data"
    __max_retry_count__ = 3
    __retry_delay__ = 0.5
    __merge_query_template__ = """
        MERGE `{destinanation_tbl}` as dt_dest
        USING `{source_tbl}` as dt_src
        ON (
            dt_dest.ref_hour_id = dt_src.ref_hour_id 
            AND dt_dest.ref_participant_id = dt_src.ref_participant_id
            AND dt_dest.ref_meter_id = dt_src.ref_meter_id
        )
        WHEN NOT MATCHED THEN
        INSERT ROW
        WHEN MATCHED THEN
        UPDATE SET data = dt_src.data
    """
    __data_primary_key__ = ["ref_hour_id", "ref_meter_id", "ref_participant_id"]

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)

        self._config: DbLoadCfg = None
        self._factory: Factory = Factory()

        self._update_files_q: Queue = Queue()
        self._load_updates_list_worker: Optional[LoadUpdatesFilesWorker] = None

        self._update_data_q: Queue = Queue()
        self._loaded_update_files_q: Queue = Queue()
        self._load_updates_data_worker: Optional[LoadUpdatesdataWorker] = None

        self._tmp_data_tbl_name = f"{self.__destination_tbl_name__}_{uuid.uuid4()}"

        self._temp_tbl_id = f"{CFG.PROJECT}.{CFG.DATASET}.{self._tmp_data_tbl_name}"

        self._finalize_updates_worker: Optional[FinalizeUpdatesWorker] = None

        _policy_tags = PolicyTagList(
            [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        )

        # TODO: Should be moved to datawarehouse schema
        self._meter_data_tbl_schema = [
            SchemaField(
                "ref_hour_id", "INTEGER", mode="REQUIRED", policy_tags=_policy_tags
            ),
            SchemaField(
                "ref_participant_id",
                "INTEGER",
                mode="REQUIRED",
                policy_tags=_policy_tags,
            ),
            SchemaField(
                "ref_meter_id", "INTEGER", mode="REQUIRED", policy_tags=_policy_tags
            ),
            SchemaField("data", "FLOAT", mode="REQUIRED", policy_tags=_policy_tags),
        ]

    @staticmethod
    def _get_dw_connection():
        return get_db_connection()

    @staticmethod
    def _get_connection() -> Client:
        if not LOCAL_RUN:
            return Client()
        credentials = service_account.Credentials.from_service_account_file(SECRET_PATH)
        return Client(credentials=credentials)

    def configure(self, data: bytes) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Loading configuration.")
            try:
                js_config = self._before_configuration(data)
                if not js_config:
                    raise MalformedConfig("Recieved Malformed configuration JSON")

                self._config = self._factory.load(js_config, DbLoadCfg)
            except (ValueError, TypeError, JSONDecodeError) as err:
                raise MalformedConfig from err

            self._load_updates_list_worker = LoadUpdatesFilesWorker(
                updates_files=self._update_files_q, config=self._config
            )

            self._load_updates_data_worker = LoadUpdatesdataWorker(
                updates_files=self._update_files_q,
                update_data=self._update_data_q,
                loaded_update_files=self._loaded_update_files_q,
                config=self._config,
            )

            self._finalize_updates_worker = FinalizeUpdatesWorker(
                updates_files=self._loaded_update_files_q, config=self._config
            )

        self._logger.debug(
            "Loaded configuration.",
            extra={
                "labels": {
                    "elapsed_teime": elapsed(),
                }
            },
        )

    def _load_updates_files_list(self) -> None:
        """Load data list logic"""
        self._logger.info("Loading updates files")

        if self._load_updates_list_worker is None:
            self._logger.error(
                "The 'configure' method must be run before"
                " _load_updates_list_worker call. Skip."
            )
            return None
        self._load_updates_list_worker.run()
        return None

    def _finalize_update_files(self) -> None:
        """Move update files to processed state"""
        self._logger.info("Finalizing updates files")

        if self._load_updates_list_worker is None:
            self._logger.error(
                "The 'configure' method must be run before"
                " _finalize_update_files call. Skip."
            )
            return None
        self._finalize_updates_worker.run()
        return None

    def _load_updates_data(self) -> None:
        """Load update data from updates files logic"""
        self._logger.info("Loading updates files")

        if self._load_updates_data_worker is None:
            self._logger.error(
                "The 'configure' method must be run before"
                " _load_updates_data_worker call. Skip."
            )
            return None
        self._load_updates_data_worker.run()
        return None

    def _create_temporary_table(self, client: Client) -> None:
        table = Table(self._temp_tbl_id, schema=self._meter_data_tbl_schema)
        client.create_table(table, exists_ok=True)

    def _delete_temporary_table(self, client: Client) -> None:
        table = Table(self._temp_tbl_id, schema=self._meter_data_tbl_schema)
        client.delete_table(table, not_found_ok=True)

    def _get_data_rows(self) -> List[Dict[str, Any]]:
        if self._update_data_q is None or self._update_data_q.empty():
            self._logger.warning("Update data does not exists or empty. Skip.")
            return []

        rec_df = DataFrame.from_records(list(self._update_data_q.queue))
        rec_raw_cnt = len(rec_df)
        self._logger.debug(f"Found a '{rec_raw_cnt}' records for loading.")

        rec_df.drop_duplicates(subset=self.__data_primary_key__, inplace=True)

        rec_cnt = len(rec_df)

        self._logger.debug(f"Removed '{rec_raw_cnt - rec_cnt}' dublicate records.")

        return rec_df.to_dict("records")

    def _insert_updates_in_dw(self, client: Client) -> int:
        self._logger.info("Loading data in the DataWarehouse.")
        rows = self._get_data_rows()

        if not rows:
            self._logger.warning("Update data does not exists or empty. Skip.")
            return 0

        self._create_temporary_table(client=client)

        job_config = LoadJobConfig()
        job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = self._meter_data_tbl_schema

        retry_count, delay = 0, self.__retry_delay__

        while retry_count < self.__max_retry_count__:

            try:
                job = client.load_table_from_json(
                    rows, self._temp_tbl_id, job_config=job_config
                )
                res = job.result()
                return res.output_rows
            except (BadRequest, Forbidden, NotFound, HttpError) as err:
                retry_count += 1
                delay *= retry_count
                time.sleep(delay)
                self._logger.error(
                    f"Failed json data insertion  due to the error '{err}'"
                    f"Retrying in {delay} seconds..."
                )
            else:
                # TODO: Try to use clear_queue methid here
                self._update_data_q = Queue()

        self._logger.error("Rich out retry attempts. Failed row insertions")
        return 0

    def _merge_data_to_main_tbl(self, client: Client) -> None:
        query = self.__merge_query_template__.format(
            destinanation_tbl=(
                f"{CFG.PROJECT}.{CFG.DATASET}.{self.__destination_tbl_name__}"
            ),
            source_tbl=self._temp_tbl_id,
        )

        job = client.query(query)
        job.result()

    def run(self) -> None:
        with elapsed_timer() as ellapsed:
            self._load_updates_files_list()
            self._load_updates_data()

            with self._get_connection() as client:
                try:
                    output_rows = self._insert_updates_in_dw(client)
                    if output_rows:
                        self._logger.info(
                            f"Inserted '{output_rows}' into temporary table "
                            f"'{self._temp_tbl_id}'. Merging to main table"
                        )
                        self._merge_data_to_main_tbl(client)
                    else:
                        self._logger.warning("Data has not been uploaded.")
                except (
                    LoadJsonError,
                    BadRequest,
                    Forbidden,
                    NotFound,
                    HttpError,
                ) as err:
                    self._logger.error(
                        f"Recieved error during inserting data into DataWarehouse"
                        f" '{err}'"
                    )
                else:
                    self._finalize_update_files()
                finally:
                    self._delete_temporary_table(client)

            self._logger.info(
                "Completed db load processing.",
                extra={"labels": {"elapsed_time": ellapsed()}},
            )

    def fetch(self):
        pass

    def standardize(self):
        pass


def main(event, context):  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="METERS DATA LOAD",
        level="DEBUG",
        description="METERS DATA LOAD",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        connector = DwLoadConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.configure(event)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": elapsed()}})


if __name__ == "__main__":

    import json

    DEBUG_LOGGER = Logger(
        name="METERS DATA LOAD DEBUG",
        level="DEBUG",
        description="METERS DATA LOAD DEBUG",
        trace_id=uuid.uuid4(),
    )

    DEBUG_LOGGER.error("=" * 40)

    import debugpy

    debugpy.listen(5678)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()

    for participant_id in CFG.DEBUG_PARTICIPANTS:
        DEBUG_LOGGER.debug("=" * 40)
        file_path = CFG.LOCAL_PATH.joinpath(f"dbload_{participant_id}_payload.json")
        if not file_path.exists():
            DEBUG_LOGGER.error(f"Payload file {file_path} doesn't exists. Skipping")
            continue
        with open(file_path, "r", encoding="utf-8") as exmpl_fl:
            cfg = json.load(exmpl_fl)

        json_config = dumps(cfg).encode("utf-8")
        event_sample = {"data": base64.b64encode(json_config)}

        main(event=event_sample, context=None)

    DEBUG_LOGGER.error("=" * 40)
