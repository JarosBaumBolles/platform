"""DataWarehouse update warehouse"""
import json
import queue
import uuid
from concurrent import futures
from enum import Enum
from queue import Queue
from typing import Dict, List

from google.cloud import pubsub_v1
from google.cloud.storage.bucket import Bucket

from common import settings as CFG
from common.dispatcher import is_cloud_function_exists
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.thread_pool_executor import run_thread_pool_executor
from dispatcher.base_dispatcher import BaseDispatcher, UnexpectedAction


class DwScope(Enum):
    """Data WarehouseScope Update"""

    participant = "participant"  # pylint: disable=invalid-name
    calendar = "calendar"  # pylint: disable=invalid-name


class _DwUpdateWorkerCallback:
    __description__ = "DW Update callback"
    __timeout__ = 20

    def __init__(self, payload: Dict) -> None:
        self._trace_id = str(uuid.uuid4())
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)
        self._payload = payload

    def callback(self, publish_future: pubsub_v1.publisher.futures.Future) -> None:
        """Pub Sub callback"""
        try:
            # Wait timeout seconds for the publish call to succeed.
            if self._payload["extra"]:
                self._logger.info(
                    f"Result of call data warehouse update for participant "
                    f"{self._payload['extra']['participant_id']} is "
                    f"{publish_future.result(timeout=self.__timeout__)}"
                )
            else:
                self._logger.info(
                    f"Result of call data warehouse calendar update is "
                    f"{publish_future.result(timeout=self.__timeout__)}"
                )
        except futures.TimeoutError:
            if self._payload["extra"]:
                self._logger.error(
                    f"Publishing {self._payload['extra']['participant_id']} "
                    "timed out."
                )
            else:
                self._logger.error("Publishing calendar update timed out.")


class DwUpdateWorker(BaseDispatcher):
    """DataWarehouse Dispatcher"""

    def __init__(
        self, payload_queue: Queue, env_tz_info=CFG.ENVIRONMENT_TIME_ZONE
    ) -> None:
        super().__init__(env_tz_info=env_tz_info)
        self._payload_queue: Queue = payload_queue
        self._topic_path = self._publisher.topic_path(  # pylint: disable=no-member
            CFG.PROJECT, CFG.DW_UPDATE_FUNCTION_NAME
        )

    def run_dw_update(
        self,
        logs: Queue,
        futures_queue: Queue,
        worker_idx: int,  # pylint:disable=unused-argument
    ) -> None:
        """Call datawarehouse update connector"""
        if self._payload_queue.empty():
            logs.put(
                "WARNING",
                self._trace_id,
                "Payload queue is empty. exit.",
            )
            return None

        is_connector_exists = is_cloud_function_exists(CFG.DW_UPDATE_FUNCTION_NAME)

        if not is_connector_exists:
            logs.put(
                "ERROR",
                f"The `{CFG.DW_UPDATE_FUNCTION_NAME}` connector is not "
                f"deployed in the '{CFG.PROJECT}' environment. Exit",
            )
            return None

        empty_run_count = 0
        while True:
            if self._payload_queue.empty():
                if empty_run_count == 5:
                    break
                empty_run_count += 1
            else:

                payload = self._payload_queue.get()
                message_json = json.dumps(
                    {
                        "data": payload,
                    }
                )
                payload_data = message_json.encode("utf-8")
                publish_future = self._publisher.publish(
                    self._topic_path, data=payload_data
                )
                done_callback = _DwUpdateWorkerCallback(payload)
                publish_future.add_done_callback(done_callback.callback)
                futures_queue.put(publish_future)
                self._payload_queue.task_done()


class DwUpdateScheduler(BaseDispatcher):
    """
    Contains functionality to load participant information after latest
    update into DataWarehousez
    """

    __created_by__ = "DATAWAREHOUSE PARTICIPANTS UPDATE SCHEDULER"
    __description__ = "DATAWAREHOUSE PARTICIPANTS UPDATE SCHEDULER"
    __local_cfg_name_tmpl__ = "dw_update_{participant_id}_payload.json"

    __workers_replica__ = 10

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._payloads = queue.Queue()
        self._worker = DwUpdateWorker(self._payloads)

    def _get_participant_payload(self, bucket: Bucket) -> dict:
        """Generate connector payload

        Args:
            bucket (Bucket): Meter full configuration

        Returns:
            dict: Connector payload
        """
        self._logger.info(f"Parcing bucket '{bucket.name}'.")
        return {
            "scope": DwScope.participant.value,
            "extra": {
                "participant_id": int(bucket.name.lstrip(CFG.BUCKET_PREFFIX)),
                "bucket": bucket.name,
                "path": CFG.CONFIG_BASE_PATH,
                "filename": CFG.PARTICIPANT_CONFIG_NAME,
            },
        }

    def get_payload(self) -> None:
        """Process available participants and form paylod"""
        with elapsed_timer() as elapsed:
            self._logger.debug("Payload generation.")
            buckets: List[Bucket] = self.get_buckets()
            for bucket in buckets:
                self._payloads.put(self._get_participant_payload(bucket))
            self._payloads.put(
                {
                    "scope": DwScope.calendar.value,
                    "extra": {},
                }
            )
            self._logger.debug(
                "Loaded configuration.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )

    def _run_dw_update(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Call data warehouse update.")

            result_queue = queue.Queue()
            logs_queue = queue.Queue()

            pub_futures = run_thread_pool_executor(
                workers=[
                    (
                        self._worker.run_dw_update,
                        [
                            logs_queue,
                            result_queue,
                        ],
                    )
                ],
                worker_replica=self.__workers_replica__,
            )

            self.process_worker_run_results(pub_futures, result_queue, logs_queue)

            self._logger.debug(
                "Completed datawarehouse update run.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def process_worker_run_results(
        self, run_futures, pub_sub_futures_queue, logs
    ) -> None:
        """Process worker statuses and collected logs"""
        for res in run_futures:
            try:
                res.result()
            except Exception as err:  # pylint: disable=broad-except
                self._logger.error(
                    f"ERROR: Received error '{err}' during datawarehouse updating."
                )

        pub_sub_futures = []
        while not pub_sub_futures_queue.empty():
            pub_sub_futures.append(pub_sub_futures_queue.get())
            pub_sub_futures_queue.task_done()

        # Wait for all the publish futures to resolve before exiting.
        futures.wait(pub_sub_futures, return_when=futures.ALL_COMPLETED)

        while not logs.empty():
            level, _, message = logs.get()
            self._logger.log(level, message)
            logs.task_done()

    def _save_payload_locally(self):
        with elapsed_timer() as elapsed:
            self._logger.debug("Saving payload locally.")

            if self._payloads.empty():
                self._logger.warning(
                    "There is no payload required for the call "
                    f"`{CFG.DW_UPDATE_FUNCTION_NAME}` connector. Exit."
                )
                return None

            while not self._payloads.empty():
                payload = self._payloads.get()
                json_cfg = {
                    "data": payload,
                }

                extra = json_cfg.get("data", {}).get("extra", {})
                filename = self.__local_cfg_name_tmpl__.format(
                    participant_id=extra.get("participant_id", "calendar")
                )

                file_path = self.__local_path__.joinpath(filename)
                with open(file_path, "w", encoding="UTF-8") as cfg_fl:
                    json.dump(json_cfg, cfg_fl, indent=4)
            self._logger.debug(
                "Saved payload locally.", extra={"labels": {"elapsed_time": elapsed()}}
            )

    def run(self) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Processing participants")
            self.get_payload()
            if not CFG.DEBUG:
                self._run_dw_update()
            else:
                self._save_payload_locally()
            self._logger.debug(
                "Completed partcipants processing",
                extra={"labels": {"elapsed_time": elapsed()}},
            )


def main():
    """Entry point"""
    main_logger = Logger(
        name="DATAWAREHOUSE PARTICIPANTS UPDATE SCHEDULER",
        level="DEBUG",
        description="DATAWAREHOUSE PARTICIPANTS UPDATE SCHEDULER",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as ellapsed:
        try:
            scheduler = DwUpdateScheduler(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
            scheduler.run()
        except UnexpectedAction as err:
            main_logger.error(f"Unexpectedly closed due to the error '{err}'")
        finally:
            main_logger.info(
                "Completed.", extra={"labels": {"elapsed_time": ellapsed()}}
            )


if __name__ == "__main__":
    DEBUG_LOGGER = Logger(
        name="EXPORT PUBLIC DATA",
        level="DEBUG",
        description="EXPORT PUBLIC DATA",
        trace_id=uuid.uuid4(),
    )
    DEBUG_LOGGER.info("=" * 40)

    # import debugpy

    # debugpy.listen(CFG.DEBUG_PORT)
    # debugpy.wait_for_client()  # blocks execution until client is attached
    # debugpy.breakpoint()

    main()
    DEBUG_LOGGER.info("=" * 40)
