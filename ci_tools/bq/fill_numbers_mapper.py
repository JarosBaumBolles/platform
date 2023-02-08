"""Used hardcode mapper given number rangeto the letters representatio"""
import uuid

from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import Client
from google.oauth2 import service_account

from common import settings as CFG
from common.big_query_utils import insert_json_data
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.request_helpers import retry
from datawarehouse import (
    PRODUCTION_NUMBER_LETTERS_MAPPER_SCHEMA as NUMBER_LETTERS_MAPPER_SCHEMA,
)

# During to the biguery design and total asyncronous approuch in the bencmark
# application we have no choise to use some hardcode for building names masking
# and Anonymization


class NumberLettersHardcode:
    """Simple utils to fil mapper"""

    __description__ = "FILL NUMBER MAPPERS"
    __max_number__ = 1000

    __table_name__ = "_number_letters_mapper"

    __delete_numbers_query__ = """
        DELETE FROM `{project}.standardized_new.{table}` WHERE TRUE;
    """

    def __init__(self):
        self._trace_id = uuid.uuid4()
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)

    @staticmethod
    def like_excel_column_name(numb: int) -> str:
        """Convert the given number to the letter like it realized in excel."""
        string = ""
        while numb > 0:
            numb, remainder = divmod(numb - 1, 26)
            string = chr(65 + remainder) + string
        return string

    @retry(BadRequest)
    def _db_run_query(self, connection: Client, query: str) -> None:
        query_str = self.__format_query_string(query)
        query = connection.query(query_str)
        res = query.result()
        return res

    @staticmethod
    def _get_dw_connection():
        if not CFG.LOCAL_RUN or CFG.DEBUG:
            return Client()

        return Client(
            credentials=service_account.Credentials.from_service_account_file(
                CFG.LOCAL_AUTH_SECRET_PATH
            )
        )

    @staticmethod
    def __format_query_string(query: str) -> str:
        parts = map(str.strip, query.split("\n"))
        return " ".join(parts).strip()

    def _delete_numbers(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Starting numbers rows deletion.")

            query = self.__delete_numbers_query__.format(
                project=CFG.PROJECT, table=self.__table_name__
            )

            self._db_run_query(connection=connection, query=query)
            self._logger.debug(
                f"The '{self.__table_name__}' has been truncated",
                extra={"labels": {"elapsed_timer": elapsed()}},
            )

    def _insert_mappers(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Start numbers updating.")
            rows = []
            for numb in range(1, self.__max_number__ + 1):
                rows.append({"number": numb, "name": self.like_excel_column_name(numb)})

            if rows:
                insert_json_data(
                    connection=connection,
                    json_rows=rows,
                    full_table_id=(
                        f"{CFG.PROJECT}.standardized_new.{self.__table_name__}"
                    ),
                    schema=NUMBER_LETTERS_MAPPER_SCHEMA,
                    max_worker_replica=5,
                )

            self._logger.debug(
                "Updated numbers.",
                extra={
                    "labels": {"elapsed_time": elapsed(), "inserted rows": len(rows)},
                },
            )

    def run(self):
        """Run loop"""
        dw_connection = self._get_dw_connection()
        self._delete_numbers(dw_connection)
        self._insert_mappers(dw_connection)
        dw_connection.close()


def main():
    """Entry point"""
    main_logger = Logger(
        name="FILL NUMBER MAPPERS",
        level="DEBUG",
        description="FILL NUMBER MAPPERS",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as elapsed:
        main_logger.info("Updating numbers mapping.")
        connector = NumberLettersHardcode()
        connector.run()
        main_logger.info(
            "Completed.",
            extra={
                "labels": {
                    "elapsed_time": elapsed(),
                }
            },
        )


if __name__ == "__main__":

    DEBUG_LOGGER = Logger(
        name="FILL NUMBER MAPPERS",
        level="DEBUG",
        description="FILL NUMBER MAPPERS",
        trace_id=uuid.uuid4(),
    )

    DEBUG_LOGGER.error("=" * 40)
    import debugpy

    debugpy.listen(5678)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()

    main()
