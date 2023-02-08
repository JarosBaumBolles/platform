"""
Entry point for the DB Load dispatcher based on scheduler events.
"""

import json
import queue
import uuid
from concurrent import futures
from typing import Any, Callable, Dict, List

from google.cloud import pubsub_v1

from common import settings as CFG
from common.data_representation.config import ConfigException
from common.data_representation.config.participant import ParticipantConfig
from common.dispatcher import is_cloud_function_exists
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.thread_pool_executor import run_thread_pool_executor
from dispatcher.base_dispatcher import BaseDispatcher

DEFAULT_WORKER_REPLICA_AMOUNT = 20

class DbLoadDispatcher(BaseDispatcher):
    """Read prticipant config and dispatch dbload tasks"""

    __description__ = "DB LOAD DISPATCHER"
    __local_cfg_name_tmpl__ = "dbload_{participant_id}_payload.json"

    def __init__(self, env_tz_info=CFG.ENVIRONMENT_TIME_ZONE) -> None:
        super().__init__(env_tz_info=env_tz_info)

        self._topic_path = self._publisher.topic_path(  # pylint: disable=no-member
            CFG.PROJECT, CFG.DB_LOAD_FUNCTION_NAME
        )
        self._payloads = queue.Queue()

    def _get_payload(self, raw_cfg: dict, participant_id: int) -> dict:
        """Generate connector payload

        Args:
            raw_mtr_cfg (dict): Meter full configuration

        Returns:
            dict: Connector payload
        """
        with elapsed_timer() as elapsed:
            payload = {
                "meters": [],
                "extra": {"participant_id": participant_id},
            }

            for cnctr_cfg in raw_cfg.get("connectors", []):
                for meter_name, meter_info in cnctr_cfg.get("meters", {}).items():
                    payload["meters"].append(
                        {
                            "meter_name": meter_name,
                            "meter_id": meter_info["meter_id"],
                            "meter_uri": meter_info["meter_uri"],
                            "type": meter_info["type"],
                            "extra": {
                                "timezone": meter_info.get("timezone", ""),
                                "standardized": {
                                    "bucket": meter_info.get(
                                        "meteredDataLocation", {}
                                    ).get("bucket", ""),
                                    "path": meter_info.get(
                                        "meteredDataLocation", {}
                                    ).get("path", ""),
                                },
                            },
                        }
                    )
            self._logger.debug(
                "The payload has been created.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
        return payload

    def _parse_participant_config(self, bucket) -> Dict:
        with elapsed_timer() as elapsed:
            cfg = ParticipantConfig()
            cfg.read_from_bucket(
                bucket=bucket.name,
                subdirectory=CFG.CONFIG_BASE_PATH,
                filename=CFG.PARTICIPANT_CONFIG_NAME,
                binary_mode=False,
            )
            json_cfg = cfg.as_json()
            self._logger.debug(
                f"Completed `{bucket.name}` config parsing.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
            return json_cfg

    def _parse_participant_configs(self, buckets: List[Any]) -> None:
        self._logger.info("Processing participants configuration.")
        with elapsed_timer() as elapsed:
            for bucket in buckets:
                participant_id = int(bucket.name.lstrip(CFG.BUCKET_PREFFIX))
                self._logger.debug(
                    f"Processing participant {participant_id} configuration"
                )

                try:
                    json_cfg = self._parse_participant_config(bucket)
                except ConfigException as err:
                    self._logger.error(
                        f"Cannot parse {bucket.name}/{CFG.CONFIG_BASE_PATH}/"
                        f"{CFG.PARTICIPANT_CONFIG_NAME} due to the error "
                        f"'{err}'. Skipping."
                    )
                    continue

                if not json_cfg.get("connectors", []):
                    self._logger.warning(
                        f"The participant {participant_id} configuration does "
                        "not contain connectors. Skipping"
                    )
                    continue
                # TODO: OPENWEATHER ISSUE. DEBUG CODE. REMOVE AFTER COMPLETION
                payload = self._get_payload(json_cfg, participant_id)
                message_json = json.dumps(
                    {
                        "data": payload,
                    }
                )

                payload_data = message_json.encode("utf-8")
                self._logger.debug("=" * 40)
                self._logger.debug(f"payload_data - '{payload_data}'")
                self._logger.debug("=" * 40)
                # ============================================================
                self._payloads.put(payload)
            self._logger.debug(
                "Processed participants configuration.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    @staticmethod
    def _pub_sub_done_callback(
        publish_future: pubsub_v1.publisher.futures.Future,  # pylint:disable=unused-argument
        payload: str,
        description: str,
        timeout: int = 120,
    ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
            call_logger = Logger(
                name=description,
                level="DEBUG",
                description=description,
                trace_id=uuid.uuid4(),
            )
            try:
                # Wait timeout seconds for the publish call to succeed.
                call_logger.info(
                    f"Result of call dbload for participant "
                    f"{payload['extra']['participant_id']} is"
                    f"{publish_future.result(timeout=timeout)}"
                )
            except futures.TimeoutError:
                call_logger.error(
                    f"Publishing {payload['extra']['participant_id']} timed out."
                )

        return callback

    @staticmethod
    def _call_dbload_worker(  # pylint:disable=too-many-arguments
        publisher: pubsub_v1.PublisherClient,
        topic_path: str,
        payload_queue: queue.Queue,
        futures_queue: queue.Queue,
        done_callback: Callable,
        description: str,
        worker_idx: int,  # pylint:disable=unused-argument
    ) -> None:
        empty_run_count = 0
        while True:
            if payload_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                payload = payload_queue.get()
                message_json = json.dumps(
                    {
                        "data": payload,
                    }
                )

                payload_data = message_json.encode("utf-8")

                publish_future = publisher.publish(topic_path, data=payload_data)

                publish_future.add_done_callback(
                    done_callback(publish_future, payload, description=description)
                )
                futures_queue.put(publish_future)
                payload_queue.task_done()

    def _save_payload_locally(self):
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving payload locally.")

            if self._payloads.empty():
                self._logger.warning(
                    "There is no payload required for the call "
                    "`{CFG.DB_LOAD_FUNCTION_NAME}` function. Exit."
                )
                return None

            while not self._payloads.empty():
                payload = self._payloads.get()
                json_cfg = {
                    "data": payload,
                }

                filename = self.__local_cfg_name_tmpl__.format(
                    participant_id=json_cfg["data"]["extra"]["participant_id"]
                )

                file_path = self.__local_path__.joinpath(filename)
                with open(file_path, "w", encoding="UTF-8") as cfg_fl:
                    json.dump(json_cfg, cfg_fl, indent=4)
            self._logger.debug(
                "Saved payload locally.", extra={"labels": {"elapsed_time": elapsed()}}
            )

    def _run_dbload(self, workers_replica: int = DEFAULT_WORKER_REPLICA_AMOUNT) -> None:

        with elapsed_timer() as elapsed:
            self._logger.debug("Running db load.")

            if self._payloads.empty():
                self._logger.warning(
                    f"The data for call `{CFG.DB_LOAD_FUNCTION_NAME}` function "
                    "absent. Exit."
                )
                return None

            is_connector_exists = is_cloud_function_exists(CFG.DB_LOAD_FUNCTION_NAME)

            if not is_connector_exists:
                self._logger.error(
                    f"The `{CFG.DB_LOAD_FUNCTION_NAME}` connector is not "
                    f"deployed in the '{CFG.PROJECT}' environment. Exit"
                )
                return None
            result_queue = queue.Queue()
            pub_futures = run_thread_pool_executor(
                workers=[
                    (
                        self._call_dbload_worker,
                        [
                            self._publisher,
                            self._topic_path,
                            self._payloads,
                            result_queue,
                            self._pub_sub_done_callback,
                            self.__description__,
                        ],
                    )
                ],
                worker_replica=workers_replica,
            )

            for res in pub_futures:
                try:
                    res.result()
                except Exception as err:  # pylint: disable=broad-except
                    self._logger.error(
                        f"Received error '{err}' during call "
                        f"{CFG.DB_LOAD_FUNCTION_NAME}"
                    )

            pub_sub_futures = []
            while not result_queue.empty():
                pub_sub_futures.append(result_queue.get())
                result_queue.task_done()

            # Wait for all the publish futures to resolve before exiting.
            futures.wait(pub_sub_futures, return_when=futures.ALL_COMPLETED)
            self._logger.debug(
                "Completed db load run.", extra={"labels": {"elapsed_time": elapsed()}}
            )

    def run(self) -> None:
        """Run dispatcher loop."""
        with elapsed_timer() as elapsed:
            self._logger.debug("Run db load from scratch")

            buckets = self.get_buckets()
            self._parse_participant_configs(buckets)
            if not CFG.DEBUG:
                self._run_dbload()
            else:
                self._save_payload_locally()
            self._logger.debug(
                "Completed load run.", extra={"labels": {"elapsed_time": elapsed()}}
            )


def main():
    """Entry point"""
    dblod_dispatcher = DbLoadDispatcher()
    dblod_dispatcher.run()


# TODO: Commented code below should be removed after restore logic completion
# def main1():
#     """Entry point logic."""

#     publisher = pubsub_v1.PublisherClient()

#     restore_function_names_to_call = [
#         func_name
#         for func_name in list_cloud_functions()
#         if func_name.startswith(CFG.DB_LOAD_RESTORE_FUNCTION_NAME_TEMPLATE)
#     ]
#     print(
#         f"DEBUG: DB LOAD DISPATCHER: RESTORE FUNCTIONS TO CALL {restore_function_names_to_call}."
#     )

#     with elapsed_timer() as global_ellapsed:
#         with elapsed_timer() as bckts_elpsd:
#             buckets = get_buckets_list(
#                 project=CFG.PROJECT, preffix=CFG.BUCKET_PREFFIX
#             )
#             print(
#                 f"DEBUG: DB LOAD DISPATCHER: Loaded buckets list. Ellapsed time {bckts_elpsd()}."
#             )


#         for bucket in buckets:
#             print(f"DEBUG: DB LOAD DISPATCHER: Processing {bucket.name} configuration")

#             if DEBUG and bucket.name not in ALLOWED_BUCKETS:
#                 print(
#                     f"WARNING: DB LOAD DISPATCHER: Enabled  DEBUG mode. "
#                     f"Disabled {bucket.name} processing. Skipping"
#                 )
#                 continue

#             full_config_name = (
#                 f"{bucket.name}/{CFG.CONFIG_BASE_PATH}/{CFG.PARTICIPANT_CONFIG_NAME}"
#             )
#             # Entry Point for bucket operation multiprocessing
#             participant_id = int(bucket.name.lstrip(CFG.BUCKET_PREFFIX))
#             with elapsed_timer() as cnfg_prs_elpsd:
#                 try:
#                     cfg = ParticipantConfig()
#                     cfg.read_from_bucket(
#                         bucket=bucket.name,
#                         subdirectory=CFG.CONFIG_BASE_PATH,
#                         filename=CFG.PARTICIPANT_CONFIG_NAME,
#                         binary_mode=False,
#                     )
#                     json_cfg = cfg.as_json()
#                     print(
#                         f"DEBUG: DB LOAD DISPATCHER: Completed "
#                         f"{full_config_name} config parsing. "
#                         f"Ellapsed time {cnfg_prs_elpsd()}."
#                     )
#                 except ConfigException as err:
#                     print(
#                         f"ERROR: DB LOAD DISPATCHER: Cannot parse "
#                         f"{full_config_name} due "
#                         f"to the error {err}. Skipping"
#                     )
#                     continue

#             if not json_cfg.get("connectors", []):
#                 print(
#                     f"WARNING: DB LOAD DISPATCHER: The participant "
#                     f'configuration "{full_config_name}" does not contain '
#                     "defined connectors"
#                 )

#             with elapsed_timer() as payload_ellapsed:
#                 payload = _get_payload(json_cfg, participant_id)
#                 print(
#                     "DEBUG: DB LOAD DISPATCHER: The function "
#                     f"{CFG.DB_LOAD_FUNCTION_NAME} payload formed. "
#                     f"Ellapsed time {payload_ellapsed()}."
#                 )

#             print(
#                 f"INFO: DB LOAD DISPATCHER: Calling {CFG.DB_LOAD_FUNCTION_NAME} to load data."
#             )

#             with elapsed_timer() as gcp_func_ex_elpsd:
#                 is_connector_exists = is_cloud_function_exists(
#                     CFG.DB_LOAD_FUNCTION_NAME
#                 )
#                 print(
#                     f"DEBUG: DB LOAD DISPATCHER: Completed check of cloud function "
#                     f"{CFG.DB_LOAD_FUNCTION_NAME} deployment. "
#                     f"Ellapsed time {gcp_func_ex_elpsd()}."
#                 )
#             if not is_connector_exists:
#                 print(
#                     "ERROR: DB LOAD DISPATCHER: The given db load "
#                     f'connector "{CFG.DB_LOAD_FUNCTION_NAME}" '
#                     f'is not deployed to the "{CFG.PROJECT}" environment. '
#                     f"Skipping"
#                 )
#                 continue

#             with elapsed_timer() as gcp_func_call_ellapsed:
#                 topic_path = publisher.topic_path(  # pylint: disable=no-member
#                     CFG.PROJECT, CFG.DB_LOAD_FUNCTION_NAME
#                 )
#                 message_json = json.dumps(
#                     {
#                         "data": payload,
#                     }
#                 )
#                 data = message_json.encode("utf-8")
#                 print(f"DEBUG: TOPIC PATH {topic_path}")
#                 future = publisher.publish(topic_path, data=data)
#                 result = future.result()
#                 print(
#                     f"DEBUG: DISPATCHER: Data pushed to gcp topic "
#                     f"{CFG.DB_LOAD_FUNCTION_NAME}. "
#                     f"{gcp_func_call_ellapsed()}, with result {result}"
#                 )

#             if restore_function_names_to_call:
#                 print(
#                     "DEBUG: DISPATCHER: Start calling DW restore cloud "
#                     f"functions {restore_function_names_to_call}. "
#                 )
#                 for func in restore_function_names_to_call:

#                     topic_path = publisher.topic_path(  # pylint: disable=no-member
#                         CFG.PROJECT, func
#                     )
#                     function_id = func.split(
#                         CFG.DB_LOAD_RESTORE_FUNCTION_NAME_TEMPLATE
#                     )[-1]
#                     payload["function_id"] = function_id

#                     if not DEBUG:
#                         message_json = json.dumps(
#                             {
#                                 "data": payload,
#                             }
#                         )
#                         data = message_json.encode("utf-8")
#                         print(f"DEBUG: TOPIC PATH {topic_path}")
#                         future = publisher.publish(topic_path, data=data)
#                         result = future.result()
#                         print(
#                             f"DEBUG: DISPATCHER: Data pushed to gcp topic {func}. "
#                             f"with result {result}"
#                         )
#                     else:
#                         json_cfg = {
#                             "data": payload,
#                         }
#                         with open(
#                             f"/tmp/participant_db_load_payload_{bucket[-1]}.json",
#                             "w",
#                             encoding="UTF-8",
#                         ) as prtcpnt_fl:
#                             json.dump(json_cfg, prtcpnt_fl, indent=4)

#                 print(
#                     "DEBUG: DISPATCHER: Finish calling DW restore cloud "
#                     f"functions {restore_function_names_to_call}. "
#                 )

#     print(
#         "DEBUG: DB LOAD DISPATCHER: Completed loop. "
#         f"Execution time {global_ellapsed()}."
#     )
#     return "Prototype scheduler executed successfully"


# def _get_payload(raw_cfg: dict, participant_id: int) -> dict:
#     """Generate connector payload

#     Args:
#         raw_mtr_cfg (dict): Meter full configuration

#     Returns:
#         dict: Connector payload
#     """
#     payload = {
#         "meters": [],
#         "extra": {"participant_id": participant_id},
#     }

#     for cnctr_cfg in raw_cfg.get("connectors", []):
#         for meter_name, meter_info in cnctr_cfg.get("meters", {}).items():

#             payload["meters"].append(
#                 {
#                     "meter_name": meter_name,
#                     "meter_id": meter_info["meter_id"],
#                     "meter_uri": meter_info["meter_uri"],
#                     "type": meter_info["type"],
#                     "extra": {
#                         "timezone": meter_info.get("timezone", ""),
#                         "standardized": {
#                             "bucket": meter_info.get("meteredDataLocation", {}).get(
#                                 "bucket", ""
#                             ),
#                             "path": meter_info.get("meteredDataLocation", {}).get(
#                                 "path", ""
#                             ),
#                         },
#                     },
#                 }
#             )
#     return payload


if __name__ == "__main__":
    # In general to be able run this code locally a few steps must be comleted before
    # 1. Setup GCP authentication on local environment
    #   1.1 MAke shure that shell command 'gcloud auth application-default login' completed before
    # 2. Setup Project Environment Variables - PROJECT, BUCKET_PREFFIX
    #    NOTE: If variables aren't defined in your ~/.bashrc file or directly by export command
    #          the following values wil be use
    #               PROJECT = 'develop-epbp'
    #               BUCKET_PREFFIX = 'prototype_develop-epbp_participant_'

    # import debugpy
    # debugpy.listen(CFG.DEBUG_PORT)
    # debugpy.wait_for_client()  # blocks execution until client is attached
    # debugpy.breakpoint()
    main()
