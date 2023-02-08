"""
Export public data to csv file end upload to public.
"""

import uuid
from pathlib import Path
from typing import Optional

from google.api_core.exceptions import Forbidden as GcpForbidden
from google.api_core.exceptions import NotFound as GcpNotFound
from google.api_core.exceptions import Unauthorized as GcpUnauthorized
from google.cloud.bigquery import Client, CreateDisposition, WriteDisposition
from google.cloud.bigquery.job import QueryJobConfig
from google.oauth2 import service_account
from pendulum import DateTime

from common import settings as CFG
from common.bucket_helpers import move_blob
from common.date_utils import format_date, parse, parse_timezone, truncate
from common.elapsed_time import elapsed_timer
from common.logging import Logger

# TODO: Should be changed to general way
LOCAL_RUN = False
SECRET_PATH = str(CFG.LOCAL_PATH.joinpath("bq_secret.json"))
SCOPE = ["https://www.googleapis.com/auth/bigquery"]


class PublicCsvExporter:
    """Read prticipant config and dispatch dbload tasks"""

    __description__ = "EXPORT PUBLIC DATA"
    __remote_tmp_path__ = "temp"

    __bucket_name__ = "web-portal-static-content"
    __public_view__ = "public_v1"
    __public_csv_file_name__ = "public.csv"
    __public_csv_path = Path("data")
    # __monthly_avg_view__ = "monthly_averages"
    # __month_avg_csv_file_name__ = "month_average_public.csv"

    __query_tmpl__ = """
        SELECT
            Time,
            Year,
            Month,
            Date,
            Hour,
            Property,
            Footage,
            Electricity,
            Emissions,
            Occupancy,
            Temperature,
            RealFeelTemperature,
            Humidity,
            WindDirection,
            WindSpeed,
            CloudCover,
            DewPoint,
            GridEmissions AS AverageGridEmissions,
            avgMonthlyElectricity,
            avgMonthlyEmissions,
            avgMonthlyOccupancy,
            HourlyAverageElectricity,
            HourlyAverageEmissions,
            HourlyAverageOccupancy,
            HourlyAverageTemperature,
            HourlyAverageRealFeelTemperature,
            HourlyAverageHumidity,
            HourlyAverageWindDirection,
            HourlyAverageWindSpeed,
            HourlyAverageCloudCover,
            HourlyAverageDewPoint,
            HourlyAverageGridEmissions,
            MonthlyElectricity,
            MonthlyEmissions,
            MonthlyOccupancy,
            MonthlyGridEmissions,
            MonthlyAverageGridEmissions,
            MonthlyAverageTemperature,
            MonthlyAverageRealFeelTemperature,
            MonthlyAverageHumidity,
            MonthlyAverageWindDirection,
            MonthlyAverageWindSpeed,
            MonthlyAverageCloudCover,
            MonthlyAverageDewPoint,
            MonthlyDewPoint
        FROM `{project}.{dataset}.{table}`
        WHERE Time BETWEEN "{start_date}" AND "{end_date}"
        ORDER BY Time desc, Property asc
    """

    # __avrg_monthly_query_tmpl__ = """
    #     SELECT
    #         hourly.Time as Time,
    #         hourly.Year as Year,
    #         hourly.Month as Month,
    #         hourly.Date as Date,
    #         hourly.Hour as Hour,
    #         hourly.Electricity as Electricity,
    #         hourly.Emissions as Emissions,
    #         hourly.Occupancy as Occupancy,
    #         hourly.Temperature as Temperature,
    #         hourly.RealFeelTemperature as RealFeelTemperature,
    #         hourly.Humidity as Humidity,
    #         hourly.WindDirection as WindDirection,
    #         hourly.WindSpeed as WindSpeed,
    #         hourly.CloudCover as CloudCover,
    #         hourly.DewPoint as DewPoint,
    #         hourly.AverageGridEmissions as AverageGridEmissions,
    #         hourly.Footage as Footage,
    #         hourly.Property as Property,
    #         avgMonthlyElectricity,
    #         avgMonthlyEmissions,
    #         avgMonthlyOccupancy
    #     FROM `{project}.{dataset}.{public_view}` AS hourly,
    #         `{project}.{dataset}.{avg_view}` AS monthly
    #     WHERE hourly.Year = monthly.Year
    #     AND hourly.Month = monthly.Month
    #     AND hourly.RealProperty = monthly.RealProperty
    #     AND Time BETWEEN "{start_date}" AND "{end_date}"
    #     ORDER BY Time, hourly.Property
    # """

    __destination_query_table__ = "export_public_csv"
    # __destination_month_avg_query_table__ = "export_public_with_monthly_averages_csv"

    __hours_amount__ = 2976

    def __init__(self, env_tz_info: str) -> None:
        self.env_tz_info = parse_timezone(env_tz_info)
        self._run_time: Optional[DateTime] = None

        self._trace_id = str(uuid.uuid4())
        self._logger = Logger(description=self.__description__, trace_id=self._trace_id)

    @staticmethod
    def _get_connection() -> Client:
        if not LOCAL_RUN:
            return Client()
        credentials = service_account.Credentials.from_service_account_file(SECRET_PATH)
        return Client(credentials=credentials)

    def _get_query_job_configuration(
        self,
        client: Client,
        destination_table: str,
        create_disposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
        write_disposition: WriteDisposition = WriteDisposition.WRITE_TRUNCATE,
    ) -> QueryJobConfig:

        dataset = client.dataset(CFG.DATASET)
        table = dataset.table(destination_table)

        job_cfg = QueryJobConfig()
        job_cfg.create_disposition = create_disposition
        job_cfg.write_disposition = write_disposition
        job_cfg.destination = table

        return job_cfg

    def export_from_public_view_to_table(self, client: Client) -> None:
        """Export data from view to the table"""
        with elapsed_timer() as elapsed:
            self._logger.info(
                "Exporting data from public view into temporary table "
                f"'{CFG.PROJECT}/{CFG.DATASET}.{self.__destination_query_table__}'"
            )
            end_date = truncate(self._run_time.in_timezone("UTC"), level="hour")
            start_date = end_date.subtract(hours=self.__hours_amount__)

            query = self.__query_tmpl__.format(
                project=CFG.PROJECT,
                dataset=CFG.DATASET,
                table=self.__public_view__,
                start_date=format_date(start_date, CFG.PROCESSING_DATE_FORMAT),
                end_date=format_date(end_date, CFG.PROCESSING_DATE_FORMAT),
            )
            try:
                job_cfg = self._get_query_job_configuration(
                    client, self.__destination_query_table__
                )
                job = client.query(query, job_config=job_cfg)
                job.result()
            except GcpNotFound as err:
                self._logger.error(
                    f"Cannot export data from '{CFG.PROJECT}/{CFG.DATASET}."
                    f"{self.__public_view__}' to '{CFG.PROJECT}/{CFG.DATASET}."
                    f"{self.__destination_query_table__}' due to the error "
                    f"'{err}'",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )

                self._logger.warning(
                    f"Delete'{CFG.PROJECT}/{CFG.DATASET}."
                    f"{self.__destination_query_table__}' to prevent"
                    " previous data exporting."
                )

                client.delete_table(job_cfg.destination, not_found_ok=True)
            except (GcpForbidden, GcpUnauthorized) as err:
                self._logger.error(
                    f"Cannot export data from '{CFG.PROJECT}/{CFG.DATASET}."
                    f"{self.__public_view__}' to '{CFG.PROJECT}/{CFG.DATASET}."
                    f"{self.__destination_query_table__}' due to the error "
                    f"'{err}'",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )
            else:
                self._logger.info(
                    "Exported data from public view into temporary table "
                    f"'{CFG.PROJECT}/{CFG.DATASET}.{self.__destination_query_table__}'",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )

    def export_table_to_csv(self, client: Client, table: str, file_name: str) -> None:
        """Export data from table to csv file"""
        with elapsed_timer() as elapsed:

            destination_uri = (
                f"gs://{CFG.WEB_PORTAL_BUCKET}/"
                f"{self.__remote_tmp_path__}/"
                f"{file_name}"
            )
            self._logger.info(
                "Exporting data from temporary table "
                f"'{CFG.PROJECT}/{CFG.DATASET}.{table}'"
                f" into '{destination_uri}'"
            )
            try:
                dataset = client.dataset(CFG.DATASET)
                table = dataset.table(table)
                extract_job = client.extract_table(
                    table,
                    destination_uri,
                    location=CFG.GCP_BIGQUERY_REGION,
                )
                extract_job.result()
            except (GcpNotFound, GcpForbidden, GcpUnauthorized) as err:
                self._logger.error(
                    f"Cannot export data from '{CFG.PROJECT}/{CFG.DATASET}."
                    f"{self.__public_view__}' to '{destination_uri}"
                    f"due to the error '{err}'",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )
            else:
                self._logger.info(
                    f"Exported data to '{destination_uri}'",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )

    def fetch(self, client: Client) -> None:
        """Export data from public view into CSV file"""
        self.export_from_public_view_to_table(client)
        self.export_table_to_csv(
            client=client,
            table=self.__destination_query_table__,
            file_name=self.__public_csv_file_name__,
        )

    # def fetch_avg_data(self, client: Client) -> None:
    #     """Export data from public view into CSV file"""
    #     self.export_from_public_with_m_avg_view_to_table(client)
    #     self.export_table_to_csv(
    #         client=client,
    #         table=self.__destination_month_avg_query_table__,
    #         file_name=self.__month_avg_csv_file_name__,
    #     )

    # def export_from_public_with_m_avg_view_to_table(self, client: Client) -> None:
    #     """Export data from public_with_monthly_averages view to the table"""
    #     with elapsed_timer() as elapsed:
    #         self._logger.info(
    #             "Exporting data from public_with_monthly_averages view into temporary table "
    #             f"'{CFG.PROJECT}/{CFG.DATASET}."
    #             f"{self.__destination_month_avg_query_table__}'"
    #         )
    #         end_date = truncate(self._run_time.in_timezone("UTC"), level="hour")
    #         start_date = end_date.subtract(hours=self.__hours_amount__)

    #         query = self.__avrg_monthly_query_tmpl__.format(
    #             project=CFG.PROJECT,
    #             dataset=CFG.DATASET,
    #             public_view=self.__public_view__,
    #             avg_view=self.__monthly_avg_view__,
    #             start_date=format_date(start_date, CFG.PROCESSING_DATE_FORMAT),
    #             end_date=format_date(end_date, CFG.PROCESSING_DATE_FORMAT),
    #         )

    #         try:
    #             job_cfg = self._get_query_job_configuration(
    #                 client, self.__destination_month_avg_query_table__
    #             )
    #             job = client.query(query, job_config=job_cfg)
    #             job.result()
    #         except GcpNotFound as err:
    #             self._logger.error(
    #                 f"Cannot export data from '{CFG.PROJECT}/{CFG.DATASET}."
    #                 f"{self.__public_view__}' and '{CFG.PROJECT}/{CFG.DATASET}."
    #                 f"{self.__monthly_avg_view__}' to '{CFG.PROJECT}/{CFG.DATASET}."
    #                 f"{self.__destination_month_avg_query_table__}' due to the error "
    #                 f"'{err}'",
    #                 extra={"labels": {"elapsed_time": elapsed()}},
    #             )

    #             self._logger.warning(
    #                 f"Delete'{CFG.PROJECT}/{CFG.DATASET}."
    #                 f"{self.__destination_month_avg_query_table__}' to prevent"
    #                 " previous data exporting."
    #             )

    #             client.delete_table(job_cfg.destination, not_found_ok=True)
    #         except (GcpForbidden, GcpUnauthorized) as err:
    #             self._logger.error(
    #                 f"Cannot export data from '{CFG.PROJECT}/{CFG.DATASET}."
    #                 f"{self.__public_view__}' to '{CFG.PROJECT}/{CFG.DATASET}."
    #                 f"{self.__destination_month_avg_query_table__}' due to the error "
    #                 f"'{err}'",
    #                 extra={"labels": {"elapsed_time": elapsed()}},
    #             )
    #         else:
    #             self._logger.info(
    #                 "Exported data from month public average view into temporary table "
    #                 f"'{CFG.PROJECT}/{CFG.DATASET}."
    #                 f"{self.__destination_month_avg_query_table__}'",
    #                 extra={"labels": {"elapsed_time": elapsed()}},
    #             )

    def save_public_csv(self) -> None:
        """Copy exported file to the destination"""
        self.save(
            tmp_path=self.__remote_tmp_path__,
            destination_path=self.__public_csv_path,
            temp_file_name=self.__public_csv_file_name__,
            file_name=self.__public_csv_file_name__,
        )

    # def save_month_avg_csv(self) -> None:
    #     """Copy exported file to the destination"""
    #     self.save(
    #         tmp_path=self.__remote_tmp_path__,
    #         destination_path=self.__public_csv_path,
    #         temp_file_name=self.__month_avg_csv_file_name__,
    #         file_name=self.__public_csv_file_name__,
    #     )

    def save(
        self, tmp_path: str, destination_path: str, temp_file_name: str, file_name: str
    ) -> None:
        """Move exported CSV to the destination path"""
        with elapsed_timer() as elapsed:
            self._logger.info(
                f"Moving '"
                f"gs://{CFG.WEB_PORTAL_BUCKET}/{tmp_path}/{file_name}"
                "' to the '"
                f"gs://{CFG.WEB_PORTAL_BUCKET}/{destination_path}/{file_name}"
            )

            file_path = Path(tmp_path).joinpath(temp_file_name)
            new_file_path = Path(destination_path).joinpath(file_name)

            try:
                move_blob(
                    bucket_name=CFG.WEB_PORTAL_BUCKET,
                    blob_name=str(file_path),
                    destination_bucket=CFG.WEB_PORTAL_BUCKET,
                    new_blob_name=str(new_file_path),
                )
            except (GcpNotFound, GcpForbidden, GcpUnauthorized) as err:
                self._logger.error(
                    f"Cannot move file '{CFG.WEB_PORTAL_BUCKET}/{file_path}'"
                    f" to '{CFG.WEB_PORTAL_BUCKET}/{new_file_path}' "
                    f"due to the error '{err}'",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )
            else:
                self._logger.info(
                    f"Saved file '{CFG.WEB_PORTAL_BUCKET}/{file_path}'"
                    f" to '{CFG.WEB_PORTAL_BUCKET}/{new_file_path}' ",
                    extra={"labels": {"elapsed_time": elapsed()}},
                )

    def run(self) -> None:
        """Run loop entry"""
        self._logger.info("Fetching public data")
        self._run_time = parse(tz_info=self.env_tz_info)

        connection = self._get_connection()
        self.fetch(connection)
        self.save_public_csv()

        # self.fetch_avg_data(connection)
        # self.save_month_avg_csv()
        connection.close()


def main():
    """Entry point"""
    main_logger = Logger(
        name="EXPORT PUBLIC DATA",
        level="DEBUG",
        description="EXPORT PUBLIC DATA",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as ellapsed:
        connector = PublicCsvExporter(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
        connector.run()
        main_logger.info("Completed.", extra={"labels": {"elapsed_time": ellapsed()}})


if __name__ == "__main__":

    DEBUG_LOGGER = Logger(
        name="EXPORT PUBLIC DATA",
        level="DEBUG",
        description="EXPORT PUBLIC DATA",
        trace_id=uuid.uuid4(),
    )

    DEBUG_LOGGER.error("=" * 40)
    # import debugpy

    # debugpy.listen(CFG.DEBUG_PORT)
    # debugpy.wait_for_client()  # blocks execution until client is attached
    # debugpy.breakpoint()
    main()

    DEBUG_LOGGER.error("=" * 40)
