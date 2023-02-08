#!/usr/bin/env python
"""
Permission assignment utility for HourlyBuildingDataPlatform.
Use cases:
- Update permissions when new participant appears
- Reset permissions
"""

import argparse
import queue
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import List, Optional, Tuple

from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import Client

from common import settings as CFG
from common.bucket_helpers import get_buckets_list
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.thread_pool_executor import run_thread_pool_executor


def _cfg_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage project permissions")
    parser.add_argument(
        "-P",
        "--participants",
        type=int,
        dest="participants",
        nargs="+",
        default=[],
        help="Specify participants to apply permissions for",
    )

    parser.add_argument(
        "-DP",
        "--drop_policies",
        dest="drop_policy",
        action="store_true",
        help="Flag. If True delete policies before applying",
    )

    return parser


@dataclass
class Config:
    """Config representation"""

    drop_policy: bool = False


class PolicyManager:
    """Big Query Row Policy creation"""

    __description__ = "Big Query Row Policies Generator"

    __dw_tables__ = {
        "meters": "ref_participant_id",
        "meters_association": "ref_participant_id",
        "meters_data": "ref_participant_id",
        "participants": "participant_id",
        "properties": "ref_participant_id",
    }

    __drop_all_table_policies_query__ = """
        DROP ALL ROW ACCESS POLICIES ON `{project}.standardized_new.{table}`;    
    """

    __create_participant_zero_policy_query__ = """
        CREATE OR REPLACE ROW ACCESS POLICY 
        participant_{participant_id}_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO (
            'group:participant{participant_id}_admin@hourlybuildingdata.com',
            'group:participant{participant_id}_operator@hourlybuildingdata.com'   
        )
        FILTER USING ({column}={participant_id});
    """

    __create_participant_policy_query__ = """
        CREATE OR REPLACE ROW ACCESS POLICY 
        participant_{participant_id}_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO (
            'group:participant{participant_id}_admin@hourlybuildingdata.com',
            'group:participant{participant_id}_operator@hourlybuildingdata.com'   
        )
        FILTER USING ({column} in ({participant_id}, 0));
    """

    __create_developer_policy__ = """
        CREATE OR REPLACE ROW ACCESS POLICY 
        developers_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO ('group:developers@hourlybuildingdata.com')
        FILTER USING ({column} in ({ids}));
    """

    __create_admins_policy__ = """
        CREATE OR REPLACE ROW ACCESS POLICY
        admins_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO ('group:admins@hourlybuildingdata.com')
        FILTER USING (TRUE);
    """

    __create_dbload_policy__ = """
        CREATE OR REPLACE ROW ACCESS POLICY 
        dbload_{participant_id}_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO ('group:dbload{participant_id}_services@hourlybuildingdata.com')
        FILTER USING (TRUE);
    """

    __create_dw_update_services_policy__ = """
        CREATE OR REPLACE ROW ACCESS POLICY
        dw_update_services_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO ('group:dw_update_services@hourlybuildingdata.com')
        FILTER USING (TRUE);
    """

    __create_export_public_services_policy__ = """
        CREATE OR REPLACE ROW ACCESS POLICY
        export_public_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO ('group:export-public-data@hourlybuildingdata.com')
        FILTER USING (TRUE);
    """

    __create_bq_services_policy__ = """
        CREATE OR REPLACE ROW ACCESS POLICY
        bq_services_{project_type}_{table}_policy 
        ON `{project}.standardized_new.{table}`
        GRANT TO ('serviceAccount:bq-858895469875@bigquery-encryption.iam.gserviceaccount.com')
        FILTER USING (TRUE);
    """

    def __init__(self) -> None:
        self.participants: Optional[List] = None
        self._settings: Config = Config()
        self._logger = Logger(
            name="ROW POLICY",
            level="DEBUG",
            description="ROW POLICY",
            trace_id=uuid.uuid4(),
        )

    def configure(self, args) -> None:
        """Config parser"""
        if not args.participants:
            self.participants = self._get_participant_list()
        else:
            self.participants = args.participants

        self._settings.drop_policy = args.drop_policy

    @staticmethod
    def _get_dw_connection():
        return Client()

    @staticmethod
    def __format_query_string(query: str) -> str:
        parts = map(str.strip, query.split("\n"))
        return " ".join(parts).strip()

    @staticmethod
    def _db_run_query(connection: Client, query: str) -> None:
        return connection.query(query).result()

    def _get_participant_list(self):
        with elapsed_timer() as elapsed:
            self._logger.info("Loading particpants list.")
            buckets = get_buckets_list(project=CFG.PROJECT, preffix=CFG.BUCKET_PREFFIX)
            participants = list(map(lambda x: int(x.name.split("_")[-1]), buckets))
            self._logger.info(
                "Loaded participants list.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
            return sorted(participants)

    @staticmethod
    def _pool_queries_worker(
        client: Client,
        wrkr_queue: queue,
        error_queue: queue,
        result_queue: queue,
        worker_idx: int,  # pylint:disable=unused-argument
    ) -> None:
        empty_run_count = 0
        while True:
            if wrkr_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                query = wrkr_queue.get()
                try:
                    result = client.query(query).result()
                    result_queue.put({query: result})
                except BadRequest as err:
                    error_queue.put({query: str(err)})

                wrkr_queue.task_done()
                time.sleep(0.5)

    def _create_dbload_policies(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Adding db load row policies.")
            prj_type = CFG.PROJECT.split("-")[0].strip()
            count: Counter = Counter()
            queue_queries = queue.Queue()

            for participant_id in self.participants:
                for tbl in self.__dw_tables__:
                    if tbl == "meters_data":
                        query = self.__create_dbload_policy__.format(
                            participant_id=participant_id,
                            project_type=prj_type,
                            table=tbl,
                            project=CFG.PROJECT,
                        )
                        queue_queries.put(self.__format_query_string(query=query))
                    count[tbl] += 1

            _, errors = self._execute_queries_in_pull(
                connection,
                queue_queries,
            )
            self._process_error_queries_queue(errors)
            self._logger.info(
                f"Added developers row policies. Completed {sum(count.values())}"
                " policies creation.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _drop_policies(self, connection: Client) -> None:
        if self._settings.drop_policy:
            with elapsed_timer() as elapsed:
                self._logger.info("Removing existing row policies.")

                count: Counter = Counter()
                queue_queries = queue.Queue()
                for tbl in self.__dw_tables__:
                    query = self.__drop_all_table_policies_query__.format(
                        table=tbl, project=CFG.PROJECT
                    )
                    queue_queries.put(self.__format_query_string(query=query))
                    count[tbl] += 1

                _, errors = self._execute_queries_in_pull(
                    connection,
                    queue_queries,
                )

                self._process_error_queries_queue(errors)

                self._logger.info(
                    f"Completed row policies removing. Run {sum(count.values())}"
                    " deletion.",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )

    def _create_developers_policies(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Adding developers row policies.")
            prj_type = CFG.PROJECT.split("-")[0].strip()
            count: Counter = Counter()
            queue_queries = queue.Queue()

            ids = ", ".join(map(str, self.participants))
            for tbl, column in self.__dw_tables__.items():
                query = self.__create_developer_policy__.format(
                    project_type=prj_type,
                    table=tbl,
                    project=CFG.PROJECT,
                    column=column,
                    ids=ids,
                )
                queue_queries.put(self.__format_query_string(query=query))
                count[tbl] += 1

            _, errors = self._execute_queries_in_pull(
                connection,
                queue_queries,
            )
            self._process_error_queries_queue(errors)
            self._logger.info(
                f"Added developers row policies. Added {sum(count.values())} "
                "policies.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _create_admin_policies(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Adding admins row policies.")
            prj_type = CFG.PROJECT.split("-")[0].strip()
            count: Counter = Counter()
            queue_queries = queue.Queue()
            for tbl in self.__dw_tables__:
                query = self.__create_admins_policy__.format(
                    project_type=prj_type,
                    table=tbl,
                    project=CFG.PROJECT,
                )
                queue_queries.put(self.__format_query_string(query=query))
                count[tbl] += 1

            _, errors = self._execute_queries_in_pull(
                connection,
                queue_queries,
            )
            self._process_error_queries_queue(errors)
            self._logger.info(
                f"Added {sum(count.values())} admins row policies.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _create_dw_update_policies(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Adding dw_update row policies.")
            prj_type = CFG.PROJECT.split("-")[0].strip()
            count: Counter = Counter()
            queue_queries = queue.Queue()
            for tbl in self.__dw_tables__:
                query = self.__create_dw_update_services_policy__.format(
                    project_type=prj_type,
                    table=tbl,
                    project=CFG.PROJECT,
                )
                queue_queries.put(self.__format_query_string(query=query))
                count[tbl] += 1

            _, errors = self._execute_queries_in_pull(
                connection,
                queue_queries,
            )
            self._process_error_queries_queue(errors)
            self._logger.info(
                f"Added dw_update {sum(count.values())} row policies.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _create_export_public_data_policies(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Adding export public row policies.")
            prj_type = CFG.PROJECT.split("-")[0].strip()
            count: Counter = Counter()
            queue_queries = queue.Queue()
            for tbl in self.__dw_tables__:
                query = self.__create_export_public_services_policy__.format(
                    project_type=prj_type,
                    table=tbl,
                    project=CFG.PROJECT,
                )
                queue_queries.put(self.__format_query_string(query=query))
                count[tbl] += 1

            _, errors = self._execute_queries_in_pull(
                connection,
                queue_queries,
            )
            self._process_error_queries_queue(errors)
            self._logger.info(
                f"Added dw_update {sum(count.values())} row policies.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _create_bq_policies(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Adding bq row policies.")
            prj_type = CFG.PROJECT.split("-")[0].strip()
            count: Counter = Counter()
            queue_queries = queue.Queue()
            for tbl in self.__dw_tables__:
                query = self.__create_bq_services_policy__.format(
                    project_type=prj_type,
                    table=tbl,
                    project=CFG.PROJECT,
                )
                queue_queries.put(self.__format_query_string(query=query))
                count[tbl] += 1

            _, errors = self._execute_queries_in_pull(
                connection,
                queue_queries,
            )
            self._process_error_queries_queue(errors)
            self._logger.info(
                f"Added bq {sum(count.values())} row policies.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _create_participant_policies(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.info("Adding participants row policies.")
            prj_type = CFG.PROJECT.split("-")[0].strip()
            count: Counter = Counter()
            queue_queries = queue.Queue()
            for participant_id in self.participants:
                for tbl, column in self.__dw_tables__.items():
                    if participant_id == 0:  # Contains data shared for all participants
                        query = self.__create_participant_zero_policy_query__.format(
                            participant_id=participant_id,
                            project_type=prj_type,
                            table=tbl,
                            project=CFG.PROJECT,
                            column=column,
                        )
                    else:
                        query = self.__create_participant_policy_query__.format(
                            participant_id=participant_id,
                            project_type=prj_type,
                            table=tbl,
                            project=CFG.PROJECT,
                            column=column,
                        )
                    queue_queries.put(self.__format_query_string(query=query))
                    count[participant_id] += 1

            _, errors = self._execute_queries_in_pull(
                connection,
                queue_queries,
            )
            self._process_error_queries_queue(errors)
            self._logger.info(
                f"Added {sum(count.values())} participants row policies.",
                extra={"labels": {"elspased_time": elapsed()}},
            )

    def _process_error_queries_queue(self, error_queue: queue.Queue) -> None:
        if error_queue.empty():
            self._logger.info("Errors queue is absent")
        else:
            while not error_queue.empty():
                error = error_queue.get()
                for query, err in error.items():
                    self._logger.error(
                        f"During DW query '{query}' run recieved error '{err}'"
                    )
                error_queue.task_done()

    def _execute_queries_in_pull(
        self,
        connection: Client,
        queue_queries: queue.Queue(),
    ) -> Tuple[queue.Queue, queue.Queue]:
        with elapsed_timer() as elapsed:
            error_queue = queue.Queue()
            result_queue = queue.Queue()

            if not queue_queries.qsize():
                self._logger.warning("Query queue is empty.")
                return result_queue, error_queue

            self._logger.info("Running DW queries.")
            futures = run_thread_pool_executor(
                workers=[
                    (
                        self._pool_queries_worker,
                        [
                            connection,  # BigQuery client
                            queue_queries,  # queries to run
                            error_queue,
                            result_queue,
                        ],
                    )
                ],
                worker_replica=50,
            )

            for res in futures:
                try:
                    res.result()
                except Exception as err:  # pylint: disable=broad-except
                    self._logger.error(f"Received error '{err}' during run queries")
            self._logger.info(
                "Completed DW queries.", extra={"labels": {"elapsed_time": elapsed()}}
            )
        return result_queue, error_queue

    def run(self) -> None:
        """Run loop"""

        with elapsed_timer() as elapsed:
            self._logger.info("Start row policies creation.")
            dw_connection = self._get_dw_connection()
            self._drop_policies(dw_connection)
            self._create_participant_policies(dw_connection)
            self._create_developers_policies(dw_connection)
            self._create_admin_policies(dw_connection)
            self._create_dbload_policies(dw_connection)
            self._create_dw_update_policies(dw_connection)
            self._create_export_public_data_policies(dw_connection)
            self._create_bq_policies(dw_connection)
            self._logger.info(
                "Completed row policies creation.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )
            dw_connection.close()


def main() -> None:
    """Entry points"""
    debug_logger = Logger(
        name="ROW POLICIES CREATION",
        level="DEBUG",
        description="ROW POLICIES CREATION",
        trace_id=uuid.uuid4(),
    )

    debug_logger.info('=' * 40)

    import debugpy
    debugpy.listen(5678)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()

    parser = _cfg_arg_parser()
    args = parser.parse_args()

    manager = PolicyManager()
    manager.configure(args)
    manager.run()
    debug_logger.info('=' * 40)

if __name__ == "__main__":
    main()
