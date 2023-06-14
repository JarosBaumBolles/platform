"""Integration used to load all Participant meta info like meter, properties
    and so on in DW.

    MVP. At the moment contains simliest sync approach.
    All operations run one by one without any paralelization approach.
    This way used to provide first deliverable MVP asap. In next iteration code
    below should be redesigned aith asyncio approarch.
"""
# pylint: disable=logging-fstring-interpolation
# pylint: disable=too-many-lines
import datetime as dt
import hashlib
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from json import dumps
from operator import itemgetter
from queue import Queue
from typing import Any, Dict, List, Optional

import holidays
from dataclass_factory import Factory
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import Client
from google.oauth2 import service_account
from pendulum.datetime import DateTime

from common import settings as CFG
from common.big_query_utils import insert_json_data
from common.data_representation.config.participant import (
    ConfigException,
    ParticipantConfig,
)
from common.data_representation.config.property.data_structure import (
    AdditionalInfo,
    MeterPropertyAssociationList,
    PropertyGeneralInfo,
    PropertyUse,
)
from common.data_representation.config.property.data_structure.general_info import (
    PropertyAddress,
)
from common.date_utils import date_range, format_date, parse, truncate
from common.elapsed_time import elapsed_timer
from common.logging import Logger
from common.request_helpers import retry
from datawarehouse import PRODUCTION_CALENDAR_SCHEMA as CALENDAR_SCHEMA
from datawarehouse import (
    PRODUCTION_METERS_ASSOCIATION_SCHEMA as METERS_ASSOCIATION_SCHEMA,
)
from dispatcher.participants_info_update_dispatcher import DwScope
from integration.base_integration import BasePullConnector

# TODO: Fix to import schem depend on project name

LOCAL_RUN = False
SECRET_PATH = CFG.LOCAL_PATH.joinpath("bq_secret.json")


class UnexpectedActionScope(Exception):
    """Exception class specific to this package."""


@dataclass
class DataFile:
    """Data file structure"""

    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""


@dataclass
class DataLocation:
    """Data Location representation"""

    bucket: str = ""
    path: str = ""


@dataclass
class ContactData:
    """Contact Information representation"""

    name: str = ""
    firstName: str = ""  # pylint: disable=invalid-name
    lastName: str = ""  # pylint: disable=invalid-name
    email: str = ""
    address: PropertyAddress = field(default_factory=PropertyAddress)
    jobTitle: str = ""  # pylint: disable=invalid-name
    phone: str = ""


@dataclass
class Audit:
    """Audit data"""

    createdBy: str = ""  # pylint: disable=invalid-name
    createdDate: str = ""  # pylint: disable=invalid-name


@dataclass
class Meter:  # pylint: disable=too-many-instance-attributes
    """Meters data representation"""

    type: str = ""
    meter_uri: str = ""
    meter_id: int = 0
    unitOfMeasure: str = ""  # pylint: disable=invalid-name
    updateFrequency: str = ""  # pylint: disable=invalid-name
    meteredDataLocation: DataLocation = field(  # pylint: disable=invalid-name
        default_factory=DataLocation
    )  # pylint: disable=invalid-name
    tags: dict = field(default_factory=dict)
    audit: Audit = field(default_factory=Audit)


@dataclass
class FetchStarategy:
    """Fetch Strategy representation"""

    type: str = ""
    descriptions: str = ""


@dataclass
class Connector:
    """Connector data representation"""

    meters: Optional[Dict[str, Meter]] = None
    function: str = ""
    timezone: str = ""
    parameters: dict = field(default_factory=dict)
    fetchStrategy: FetchStarategy = field(  # pylint: disable=invalid-name
        default_factory=FetchStarategy
    )
    rawDataLocation: DataLocation = field(  # pylint: disable=invalid-name
        default_factory=DataLocation
    )


@dataclass
class Property:
    """Property Data Representation"""

    general_info: PropertyGeneralInfo
    property_uses: Optional[PropertyUse] = None
    meter_property_association_list: Optional[MeterPropertyAssociationList] = None
    additional_info: Optional[AdditionalInfo] = None


@dataclass
class Participant:
    """Participant Representaion data"""

    contact: Optional[ContactData] = None
    connectors: Optional[List[Connector]] = None
    properties: Optional[Dict[str, Property]] = None
    audit: Optional[Audit] = None
    tags: Optional[Dict] = None


class ScopeUpdate(Enum):
    """Acope Update"""

    full = "full"  # pylint: disable=invalid-name
    update = "update"  # pylint: disable=invalid-name
    calendar = "calendar"  # pylint: disable=invalid-name


@dataclass
class ExtraInfo:
    """Extra section of config"""

    participant_id: int = 0
    scope: ScopeUpdate = ScopeUpdate("full")


@dataclass
class CalendarRow:
    """Calendar row reprezentation"""

    hour_id: int = 19700010100  # Hour identifier in the form of YYMMDDHH,
    # such as 21113000 for the first hour of 30
    # Nov 21, or 21113023 for the last hour of
    # 30 Nov 21

    ref_region_id: int = 0  # Reference to the region, regions here mostly
    # differ in their working days schedule
    # TODO: During MVP period we hardcoded only one
    # region added in regions table - New York (region_id=0)
    # After MVP we shoul take into acount multi region availaviliy

    year: int = 1970  # Year, such as 2021
    day: int = 1  # Day of year, 1 - 365
    date: int = 1  # Date in month, 1 - 31
    month: int = 1  # Month number, 1 - 12
    hour: int = 0  # Hour in a day, 0-23
    dow: int = 0  # Day of week, Mon = 0, Sun = 6
    working_day: bool = True  # Mon-Fri except public holidays
    month_name: str = "January"  # January-December
    month_name_abbr: str = "Jan"  # Jan-Dec
    dow_name: str = ""  # Sunday-Saturday
    dow_name_abbr: str = ""  # Sun-Sat


@dataclass
class Calendar:
    """Calendar representation"""

    rows: Optional[List[CalendarRow]] = None


@dataclass
class MetersAssociation:
    """Meters Association reprezentattion"""

    ref_hour_id: int = -1  # Reference to hour
    ref_participant_id: int = -1  # Reference to participant
    ref_meter_id: int = -1  # Reference to meter

    ref_property_id: int = -1  # Reference to property that is
    # associated with this meter

    weight: float = 0  # Meter weight this hour


@dataclass
class Region:
    """Region representation"""

    region_id: int = -1
    country: str = ""
    state: str = ""
    timezone: str = ""
    description: str = ""


@dataclass
class Regions:
    """Regions representation"""

    regions: Optional[List[Region]] = None


class DwUpdateConnector(BasePullConnector):
    """Contains functionality to load participant connections after latest update into DW"""

    __created_by__ = "DW Info Load Connector"
    __description__ = "DW Info Load Integration"
    __cfg_previous_version__ = "previous_state"

    __callendar_deep_year__ = 3
    __meters_association_deep_week__ = 96
    __allowed_scope__ = (
        "full",
        "update",
        "calendar",
    )

    __meters_insert_rows_sql__ = """
        INSERT INTO `{project}.standardized_new.meters`
        (
           meter_id,
           meter_uri,
           ref_participant_id,
           type,
           unitOfMeasure,
           updateFrequency, 
           haystack
        ) VALUES (
            {meter_id}, 
            '{meter_uri}', 
            {ref_participant_id},
            '{type}', 
            '{unitOfMeasure}', 
            '{updateFrequency}', 
            '{haystack}'
        );
    """

    __meters_delete_rows_sql__ = """
        DELETE
        FROM `{project}.standardized_new.meters`
        WHERE ref_participant_id={participant_id};
    """

    __participant_insert_rows_sql__ = """
        INSERT INTO `{project}.standardized_new.participants`
        (
           participant_id,
           name
        ) VALUES (
           {participant_id},
           '{name}'
        );
    """

    __participant_delete_rows_sql__ = """
        DELETE 
        FROM `{project}.standardized_new.participants`
        WHERE participant_id={participant_id};
    """

    __properties_insert_rows_sql__ = """
        INSERT INTO `{project}.standardized_new.properties`
        (
           ref_region_id,
           property_id,
           property_uri,
           ref_participant_id,
           name,
           address1,
           address2,
           city,
           country,
           state,
           postal_code,
           footage,
           footage_10_categories,
           footage_3_categories
        ) VALUES (
           {ref_region_id},
           {property_id},
           '{property_uri}',
           {ref_participant_id},
           '{name}',
           '{address1}',
           '{address2}',
           '{city}',
           '{country}',
           '{state}',
           '{postal_code}',
           {footage},
           '{footage_10_categories}',
           '{footage_3_categories}'
        );
    """

    __properties_delete_rows_sql__ = """
        DELETE 
        FROM `{project}.standardized_new.properties`
        WHERE ref_participant_id={participant_id}
    """

    __calendar_get_latest_date__ = """
        SELECT cl.hour_id AS hour_id,
            cl.ref_region_id AS ref_region_id,
            cl.year AS year,
            cl.day AS day,
            cl.date AS date,
            cl.month AS month,
            cl.hour AS hour,
            cl.dow AS dow,
            cl.working_day AS working_day,
            cl.month_name AS month_name,
            cl.month_name_abbr AS month_name_abbr,
            cl.dow_name AS dow_name,
            cl.dow_name_abbr AS dow_name_abbr
        FROM `{project}.standardized_new.calendar` AS cl
        RIGHT JOIN
        (
            SELECT MAX(hour_id) AS hour_id,
                    ref_region_id
            FROM `{project}.standardized_new.calendar`
            GROUP BY ref_region_id
        ) AS tb ON cl.ref_region_id = tb.ref_region_id
        AND cl.hour_id = tb.hour_id;
    """

    __regions_get__ = """
        SELECT * FROM `{project}.standardized_new.regions`;
    """

    __meters_associations_latest_dates__ = """
        SELECT mal.ref_hour_id as ref_hour_id,
            mal.ref_participant_id as ref_participant_id,
            mal.ref_meter_id as ref_meter_id,
            mal.ref_property_id as ref_property_id,
            mal.weight as weight
        FROM `{project}.standardized_new.meters_association` AS mal
        RIGHT JOIN
        (
            SELECT max(ref_hour_id) as ref_hour_id,
                ref_meter_id,
                ref_property_id
            FROM `{project}.standardized_new.meters_association`
            GROUP BY ref_meter_id,
                    ref_property_id
        ) AS tb ON mal.ref_hour_id = tb.ref_hour_id
            AND mal.ref_meter_id = tb.ref_meter_id
            AND mal.ref_property_id = tb.ref_property_id
        WHERE mal.ref_participant_id = {participant_id}
    """

    __meters_association_orphan_meter_ids__ = """
        SELECT COUNT(*) as row_count
        FROM `{project}.standardized_new.meters_association`
        WHERE ref_participant_id = {participant_id}
        AND ref_meter_id not in
            (SELECT DISTINCT(ma.ref_meter_id) AS ref_meter_id
            FROM `{project}.standardized_new.meters_association` AS ma
            RIGHT JOIN
            (SELECT ref_property_id,
                    ref_meter_id,
                    ref_participant_id,
                    MAX(ref_hour_id) AS hour_id
                FROM {project}.standardized_new.meters_association
                GROUP BY ref_property_id,
                        ref_meter_id,
                        ref_participant_id,
                        ref_meter_id,
                        ref_participant_id) AS max_mt ON ma.ref_property_id = max_mt.ref_property_id
            AND ma.ref_meter_id = max_mt.ref_meter_id
            AND ma.ref_participant_id = max_mt.ref_participant_id
            AND ma.ref_hour_id = max_mt.hour_id
            LEFT JOIN {project}.standardized_new.meters mt ON ma.ref_meter_id = mt.meter_id
            AND (ma.ref_participant_id = mt.ref_participant_id OR mt.ref_participant_id=0)
            WHERE mt.meter_id = ma.ref_meter_id
        );
    """

    __meters_association_delete_orphan_meter_ids__ = """
        DELETE
        FROM `{project}.standardized_new.meters_association`
        WHERE ref_participant_id = {participant_id}
        AND ref_meter_id not in
            (SELECT DISTINCT(ma.ref_meter_id) AS ref_meter_id
            FROM `{project}.standardized_new.meters_association` AS ma
            RIGHT JOIN
            (SELECT ref_property_id,
                    ref_meter_id,
                    ref_participant_id,
                    MAX(ref_hour_id) AS hour_id
                FROM {project}.standardized_new.meters_association
                GROUP BY ref_property_id,
                        ref_meter_id,
                        ref_participant_id,
                        ref_meter_id,
                        ref_participant_id) AS max_mt ON ma.ref_property_id = max_mt.ref_property_id
            AND ma.ref_meter_id = max_mt.ref_meter_id
            AND ma.ref_participant_id = max_mt.ref_participant_id
            AND ma.ref_hour_id = max_mt.hour_id
            LEFT JOIN {project}.standardized_new.meters mt ON ma.ref_meter_id = mt.meter_id
            AND (ma.ref_participant_id = mt.ref_participant_id OR mt.ref_participant_id=0)
            WHERE mt.meter_id = ma.ref_meter_id
        );
    """

    __meters_association_orphan_properties_ids__ = """
        SELECT count(*) AS row_count
        FROM `{project}.standardized_new.meters_association`
        WHERE ref_participant_id = {participant_id}
        AND ref_property_id not in
            (SELECT DISTINCT(ma.ref_property_id) AS ref_meter_id
            FROM `{project}.standardized_new.meters_association` AS ma
            RIGHT JOIN
            (SELECT ref_property_id,
                    ref_meter_id,
                    ref_participant_id,
                    MAX(ref_hour_id) AS hour_id
                FROM `{project}.standardized_new.meters_association`
                GROUP BY ref_property_id,
                        ref_meter_id,
                        ref_participant_id,
                        ref_meter_id,
                        ref_participant_id) AS max_mt ON ma.ref_property_id = max_mt.ref_property_id
            AND ma.ref_meter_id = max_mt.ref_meter_id
            AND ma.ref_participant_id = max_mt.ref_participant_id
            AND ma.ref_hour_id = max_mt.hour_id
            LEFT JOIN `{project}.standardized_new.properties` AS pt ON pt.property_id=ma.ref_property_id
            AND ma.ref_participant_id=pt.ref_participant_id);
    """

    __meters_association_delete_orphan_properties_ids__ = """
        DELETE
        FROM `{project}.standardized_new.meters_association`
        WHERE ref_participant_id = {participant_id}
        AND ref_property_id not in
            (SELECT DISTINCT(ma.ref_property_id) AS ref_meter_id
            FROM `{project}.standardized_new.meters_association` AS ma
            RIGHT JOIN
            (SELECT ref_property_id,
                    ref_meter_id,
                    ref_participant_id,
                    MAX(ref_hour_id) AS hour_id
                FROM `{project}.standardized_new.meters_association`
                GROUP BY ref_property_id,
                        ref_meter_id,
                        ref_participant_id,
                        ref_meter_id,
                        ref_participant_id) AS max_mt ON ma.ref_property_id = max_mt.ref_property_id
            AND ma.ref_meter_id = max_mt.ref_meter_id
            AND ma.ref_participant_id = max_mt.ref_participant_id
            AND ma.ref_hour_id = max_mt.hour_id
            LEFT JOIN `{project}.standardized_new.properties` AS pt ON pt.property_id=ma.ref_property_id
            AND ma.ref_participant_id=pt.ref_participant_id);
    """

    __properties_mapper_get_row_query__ = """
        SELECT * 
        FROM `{project}.standardized_new.properties_mapper`
        WHERE ref_participant_id={ref_participant_id}
            AND ref_property_id={ref_property_id}
            AND property_hash="{property_hash}"
    """

    __properties_mapper_get_letter_query__ = """
        SELECT * 
        FROM `{project}.standardized_new._number_letters_mapper` 
        WHERE name not in (
            SELECT property_letter FROM `{project}.standardized_new.properties_mapper`
        ) 
        ORDER BY number ASC LIMIT 1;    
    """

    __properties_mapper_insert_rows_query__ = """
        INSERT INTO `{project}.standardized_new.properties_mapper`
        (
            ref_participant_id,
            ref_property_id,
            property_hash,
            property_letter
        ) VALUES (
            {ref_participant_id},
            {ref_property_id},
            '{property_hash}',
            '{property_letter}'
        );
    """

    __participant_meters_keys__ = (
        "type",
        "meter_id",
        "meter_uri",
        "unitOfMeasure",
        "updateFrequency",
        "tags",
    )

    def __init__(self, env_tz_info):
        super().__init__(env_tz_info=env_tz_info)
        self._factory = Factory()
        self._participant_info: Optional[Participant] = None
        self._extra_info: Optional[ExtraInfo] = None
        self._status_update: Optional[Queue] = Queue()

    @staticmethod
    def _get_data_fields(data_obj):
        return (
            []
            if not is_dataclass(data_obj)
            else list(map(lambda x: x.name, fields(data_obj)))
        )

    def __check_participant_state(
        self, latest_version: Dict[str, Any], participant_cfg: Dict[str, Any]
    ) -> None:
        """Check if participant config changed from previous run"""
        checksum = dumps(
            participant_cfg["participant"]["contact"], sort_keys=True, indent=4
        )

        checksum = hashlib.md5(checksum.encode("utf-8")).hexdigest()  # nosec
        if checksum != latest_version["participant"]:
            self._logger.warning(
                f"Detected {participant_cfg['extra']['participant_id']}-th "
                "participant configuration change."
            )
            participant_cfg["extra"]["scope"] = ScopeUpdate.full.value
            latest_version["participant"] = checksum

    def __check_properties_state(
        self, previous_state: Dict[str, Any], participant_cfg: Dict[str, Any]
    ) -> None:
        for key, values in participant_cfg["participant"]["properties"].items():
            # info[key] = info[key]["general_info"]
            prprt_checksum = dumps(values["general_info"], sort_keys=True, indent=4)

            prprt_checksum = hashlib.md5(  # nosec
                prprt_checksum.encode("utf-8")
            ).hexdigest()

            perv_state = (
                previous_state["properties"].get(key, {}).get("general_info", -1)
            )

            if prprt_checksum != perv_state:
                self._logger.warning(
                    f"Detected {participant_cfg['extra']['participant_id']}-th "
                    f"property '{key}' configuration change."
                )
                previous_state["properties"][key]["general_info"] = prprt_checksum
                participant_cfg["extra"]["scope"] = ScopeUpdate.full.value

    def __get_previos_state(self, participant_config: Dict[str, Any]) -> Dict[str, Any]:
        previous_state = self._load_json_data(
            bucket=participant_config["extra"]["bucket"],
            path=participant_config["extra"]["path"],
            filename=self.__cfg_previous_version__,
        )

        if not previous_state:
            previous_state = {
                "participant": "",
                "properties": defaultdict(lambda: defaultdict(str)),
                "meters": {},
            }

        return previous_state

    def __check_meters_state(
        self, previous_state: Dict[str, Any], participant_cfg: Dict[str, Any]
    ) -> None:
        data_getter = itemgetter(*self.__participant_meters_keys__)

        for connector in participant_cfg["participant"]["connectors"]:
            for key, meter in connector["meters"].items():
                mtr_data = dict(
                    zip(self.__participant_meters_keys__, data_getter(meter))
                )
                mtr_checksum = dumps(mtr_data, sort_keys=True, indent=4)
                mtr_checksum = hashlib.md5(  # nosec
                    mtr_checksum.encode("utf-8")
                ).hexdigest()

                perv_state = previous_state["meters"].get(key, -1)

                if mtr_checksum != perv_state:
                    self._logger.warning(
                        f"Detected {participant_cfg['extra']['participant_id']}-th "
                        f"meter '{key}' configuration change."
                    )
                    previous_state["meters"][key] = mtr_checksum
                    participant_cfg["extra"]["scope"] = ScopeUpdate.full.value

    def _get_prticipant_update_scope(self, participant_cfg: Dict[str, Any]) -> None:
        # TODO: Should be refactored later to be more clear

        self._logger.info(
            f"Match '{participant_cfg['extra']['participant_id']}' participant"
            "update type."
        )

        previous_state = self.__get_previos_state(participant_cfg)
        self.__check_participant_state(previous_state, participant_cfg)
        self.__check_properties_state(previous_state, participant_cfg)
        self.__check_meters_state(previous_state, participant_cfg)

        if participant_cfg["extra"]["scope"] is ScopeUpdate.full.value:
            self._status_update.put(
                DataFile(
                    file_name=self.__cfg_previous_version__,
                    bucket=participant_cfg["extra"]["bucket"],
                    path=participant_cfg["extra"]["path"],
                    body=dumps(previous_state, sort_keys=True, indent=4),
                )
            )
        elif participant_cfg["extra"]["scope"] is None:
            participant_cfg["extra"]["scope"] = ScopeUpdate.update.value

    @staticmethod
    def _load_participant_config(config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            load_cfg = ParticipantConfig()
            load_cfg.read_from_bucket(
                bucket=config["bucket"],
                subdirectory=config["path"],
                filename=config["filename"],
                binary_mode=False,
            )
            return load_cfg.as_json()
        except ConfigException as err:
            raise UnexpectedActionScope(  # pylint:disable=raise-missing-from
                "Can not parse teh given participant configuration file "
                f"'gs://{config['bucket']/config['path']}/'"
                f"{config['filename']}' due to the error '{err}'"
            )

    def configure(self, data: bytes) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Loading configuration.")

            js_config = self._before_configuration(data)
            self._logger.debug("Loading participant configuration.")

            if js_config["scope"] == DwScope.participant.value:
                prtcpnt_cfg = self._load_participant_config(js_config["extra"])
                new_cfg: Dict[str, Any] = {
                    "participant": prtcpnt_cfg,
                    "extra": {
                        "participant_id": js_config["extra"]["participant_id"],
                        "bucket": js_config["extra"]["bucket"],
                        "path": js_config["extra"]["path"],
                        "scope": None,
                    },
                }

                self._get_prticipant_update_scope(new_cfg)
            elif js_config["scope"] == DwScope.calendar.value:
                new_cfg: Dict[str, Any] = {
                    "participant": {},
                    "extra": {
                        "participant_id": 0,
                        "scope": ScopeUpdate.calendar.value,
                    },
                }

            self._participant_info = self._factory.load(
                new_cfg["participant"], Participant
            )

            # TODO: Fix of the issue described below
            # Due to unknown reason dataclass factory can not parse correctly
            # properties in Participant data class

            if hasattr(self._participant_info.properties, "items"):
                for pro_key, prop_value in self._participant_info.properties.items():
                    self._participant_info.properties[pro_key] = self._factory.load(
                        prop_value, Property
                    )
            else:
                self._logger.warning("Absent Participant configuration.")

            self._logger.debug("Loading general configuration.")

            self._extra_info = self._factory.load(new_cfg["extra"], ExtraInfo)

            self._logger.debug(
                "Loaded configuration.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )

    @staticmethod
    def _get_dw_connection():
        if not LOCAL_RUN:
            return Client()
        credentials = service_account.Credentials.from_service_account_file(SECRET_PATH)
        return Client(credentials=credentials)

    @retry(BadRequest)
    def _db_run_query(self, connection: Client, query: str) -> None:
        query_str = self.__format_query_string(query)
        query = connection.query(query_str)
        res = query.result()
        return res

    @staticmethod
    def __format_query_string(query: str) -> str:
        parts = map(str.strip, query.split("\n"))
        return " ".join(parts).strip()

    def _delete_participant_rows(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Starting properties rows deletion.")

            query = self.__participant_delete_rows_sql__.format(
                project=CFG.PROJECT, participant_id=self._extra_info.participant_id
            )
            self._db_run_query(connection=connection, query=query)
            if self._participant_info.contact is None:
                self._logger.error(
                    "Can not delete participant rows due to absent contact in "
                    "configuration.",
                )

            self._logger.debug(
                "Deleted participant rows.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )

    def _insert_participant_data(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Starting participant processing.")

            if self._participant_info.contact is not None:
                query = self.__participant_insert_rows_sql__.format(
                    project=CFG.PROJECT,
                    participant_id=self._extra_info.participant_id,
                    name=self._participant_info.contact.name,
                )

                self._db_run_query(connection=connection, query=query)
            else:
                self._logger.debug("Absent Contact configuration.")

            self._logger.debug(
                "Completed participant processing.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )

    def _get_regions(self, connection: Client) -> Dict[int, Region]:
        with elapsed_timer() as elapsed:
            self._logger.debug("Start fetching region ids.")

            result = list(
                self._db_run_query(
                    connection, self.__regions_get__.format(project=CFG.PROJECT)
                )
            )

            regions = self._factory.load({"regions": list(map(dict, result))}, Regions)

            regions_idx = defaultdict(list)

            for reg in regions.regions:
                regions_idx[reg.region_id].append(reg)

            self._logger.debug(
                "Completed fetching region ids.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )

            return regions_idx

    def _get_latest_calendar(self, connection: Client) -> dict[int, CalendarRow]:
        with elapsed_timer() as elapsed:
            self._logger.debug("Start fetching calendar data.")

            rows = list(
                self._db_run_query(
                    connection,
                    query=self.__calendar_get_latest_date__.format(project=CFG.PROJECT),
                )
            )

            calendar = self._factory.load({"rows": list(map(dict, rows))}, Calendar)

            result = defaultdict(list)
            if calendar.rows:
                for row in calendar.rows:
                    result[row.ref_region_id].append(row)

            self._logger.debug(
                "Fetched calendar data.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )

            return result

    def __generate_calendar_rows(
        self, start_date: DateTime, end_date: DateTime, rg_inf: Region
    ) -> List:
        with elapsed_timer() as elapsed:
            self._logger.debug("Generate absent rows.", extra={"type": "Calendar"})
            dt_range = date_range(
                start_date=start_date, end_date=end_date, range_unit="hours"
            )

            result = []
            cnt_holidays = holidays.country_holidays(rg_inf.country)

            for date in dt_range:
                dt_utc = date.in_tz("UTC")

                dt_holiday = dt.date(
                    date.year,
                    date.month,
                    date.day,
                )

                is_holiday = bool(cnt_holidays.get(dt_holiday))

                result.append(
                    CalendarRow(
                        hour_id=int(format_date(dt_utc, "YYYYMMDDHH")),
                        year=dt_utc.year,
                        day=dt_utc.day_of_year,
                        date=dt_utc.day,
                        month=dt_utc.month,
                        hour=dt_utc.hour,
                        dow=dt_utc.day_of_week - 1,
                        working_day=not is_holiday,
                        month_name=format_date(dt_utc, "MMMM"),
                        month_name_abbr=format_date(dt_utc, "MMM"),
                        dow_name=format_date(dt_utc, "dddd"),
                        dow_name_abbr=format_date(dt_utc, "ddd"),
                        ref_region_id=rg_inf.region_id,
                    )
                )

            self._logger.debug(
                "Generate absent rows.",
                extra={
                    "type": "Calendar",
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )
            return result

    def _update_calendar(self, connection: Client) -> None:
        regions = self._get_regions(connection)
        calendar = self._get_latest_calendar(connection)

        with elapsed_timer() as elapsed:
            self._logger.debug("Start calendar updating.")

            rows = []

            st_rg_ids = set(regions.keys())
            exist_ids = set(calendar.keys())
            new_regions = st_rg_ids.difference(exist_ids)
            active_regions = st_rg_ids.intersection(exist_ids)

            if new_regions:
                for region_id in regions:
                    region = regions.get(region_id, None)
                    if not region:
                        self._logger.error(
                            "Cannot find information. for the given region "
                            f"{region_id}. Skipping",
                        )
                        continue

                    region = region[0]
                    end_date = truncate(
                        self._run_time.in_tz(region.timezone), level="hour"
                    )

                    start_date = end_date.subtract(years=self.__callendar_deep_year__)

                    rows += self.__generate_calendar_rows(
                        start_date=start_date,
                        end_date=end_date,
                        rg_inf=region,
                    )

            if active_regions:
                for region_id in active_regions:
                    region = regions.get(region_id, None)
                    if not region:
                        self._logger.error(
                            "Cannot find information. for the given region "
                            f"{region_id}. Skipping",
                        )
                        continue

                    region = region[0]

                    end_date = truncate(
                        self._run_time.in_tz(region.timezone), level="hour"
                    )
                    for row in calendar.get(region_id, []):
                        start_date = parse(
                            str(row.hour_id), "YYYYMMDDHH", tz_info="UTC"
                        ).in_tz(region.timezone)
                        if start_date >= end_date:
                            self._logger.debug(
                                f"The Region {region_id} is up to date. Skipping",
                            )
                            continue

                        rows += self.__generate_calendar_rows(
                            start_date=start_date,
                            end_date=end_date,
                            rg_inf=region,
                        )

            if rows:
                insert_json_data(
                    connection=connection,
                    json_rows=self._factory.dump(rows),
                    full_table_id=f"{CFG.PROJECT}.standardized_new.calendar",
                    schema=CALENDAR_SCHEMA,
                    max_worker_replica=5,
                )

                # TODO: ADD error logic check

            self._logger.debug(
                "Updated calendar.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )

    def _get_meters_association_rows(
        self, connection: Client, query: str
    ) -> dict[int, MetersAssociation]:
        with elapsed_timer() as elapsed:
            self._logger.debug("Start fetching meters association data.")

            rows = self._db_run_query(connection, query=query)

            result = defaultdict(list)
            for row in rows:
                row_dict = dict(row)

                key = (
                    row_dict["ref_meter_id"],
                    row_dict["ref_property_id"],
                )

                result[key].append(self._factory.load(row_dict, MetersAssociation))

            self._logger.debug(
                "Fetched meters association data.",
                extra={
                    "labels": {
                        "elapsed_time": elapsed(),
                    },
                },
            )
            return result

    def __generate_meters_association_rows(
        self,
        start_date,
        end_date,
        meter_id: int,
        ref_participant_id: int,
        property_id: int,
        weight: float,
    ) -> List:
        with elapsed_timer() as elapsed:
            self._logger.debug(
                "Generate absent rows.",
                extra={
                    "type": "Meters Association",
                },
            )

            dt_range = date_range(
                start_date=start_date, end_date=end_date, range_unit="hours"
            )

            result = list(
                map(
                    lambda dt: MetersAssociation(
                        ref_hour_id=int(format_date(dt, "YYYYMMDDHH")),
                        ref_participant_id=ref_participant_id,
                        ref_meter_id=meter_id,
                        ref_property_id=property_id,
                        weight=weight,
                    ),
                    dt_range,
                )
            )

            self._logger.debug(
                "Generate absent rows.",
                extra={
                    "type": "Meters Association",
                    "labels": {"elapsed_time": elapsed()},
                },
            )

            return result

    # pylint:disable=too-many-locals
    def _upsert_meters_association_data(self, connection: Client) -> None:
        curr_date = truncate(self._run_time.in_tz("UTC"), level="hour")
        with elapsed_timer() as elapsed:
            self._logger.debug("Starting meters association processing.")
            rows = []
            meters_latest_data = self._get_meters_association_rows(
                connection,
                self.__meters_associations_latest_dates__.format(
                    project=CFG.PROJECT, participant_id=self._extra_info.participant_id
                ),
            )

            processed_keys = set()

            if self._participant_info.properties is None:
                self._participant_info.properties = {}
                self._logger.error("Absent Properties configuration.")

            for prprt_nm, prprt_val in self._participant_info.properties.items():
                self._logger.debug(f"Processing property {prprt_nm}.")

                meter_prpet_name = self._get_data_fields(
                    prprt_val.meter_property_association_list
                )
                for mpa_name in meter_prpet_name:
                    self._logger.debug(
                        f"Processing meter association '{mpa_name}' item."
                    )

                    mpa_parts = getattr(
                        prprt_val.meter_property_association_list, mpa_name, None
                    )

                    if not mpa_parts:
                        self._logger.debug(
                            f"The meter association '{mpa_name}' is empty. Skiping.",
                        )
                        continue

                    for mpa_el in mpa_parts:
                        meter_id = self.get_unique_id(mpa_el.meterURI.strip())
                        property_id = prprt_val.general_info.property_id
                        end_date = curr_date
                        key = (
                            meter_id,
                            property_id,
                        )

                        ltst_meter_dt = meters_latest_data.get(key, [])

                        if not ltst_meter_dt:
                            start_date = end_date.subtract(
                                weeks=self.__meters_association_deep_week__
                            )
                        else:
                            start_date = parse(
                                str(ltst_meter_dt[0].ref_hour_id),
                                dt_format="YYYYMMDDHH",
                                tz_info="UTC",
                            )

                        if start_date >= end_date:
                            self._logger.debug(
                                f"The meter association '{meter_id}' of property "
                                f"'{property_id}' is up to date. Skiping.",
                            )
                            processed_keys.add(key)
                            continue

                        rows += self.__generate_meters_association_rows(
                            start_date=start_date,
                            end_date=end_date,
                            meter_id=meter_id,
                            ref_participant_id=self._extra_info.participant_id,
                            property_id=property_id,
                            weight=float(mpa_el.weight),
                        )
                        processed_keys.add(key)

            # process outdated meters
            # TODO: Code must be verivied
            if processed_keys:
                for key, meters in meters_latest_data.items():
                    if key not in processed_keys:
                        for meter in meters:
                            end_date = curr_date
                            start_date = parse(str(meter.ref_hour_id), "YYYYMMDDHH")
                            if start_date < end_date:
                                self._logger.debug(
                                    f"Found outdated meter association "
                                    f"'{meter.ref_meter_id}'. Disabling",
                                )

                                rows += self.__generate_meters_association_rows(
                                    start_date=start_date,
                                    end_date=end_date,
                                    meter_id=meter.ref_meter_id,
                                    ref_participant_id=meter.ref_meter_id,
                                    property_id=meter.ref_property_id,
                                    weight=0,
                                )
            if rows:
                insert_json_data(
                    connection=connection,
                    json_rows=self._factory.dump(rows),
                    full_table_id=f"{CFG.PROJECT}.standardized_new.meters_association",
                    schema=METERS_ASSOCIATION_SCHEMA,
                    max_worker_replica=5,
                )

            # TODO: ADD error logic check
            self._logger.debug(
                f"Completed meters association processing. Upserted {len(rows)}",
                extra={"lebels": {"elapsed_time": elapsed()}},
            )

    def _delete_properties_rows(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Deleting properties rows")
            query = self.__properties_delete_rows_sql__.format(
                project=CFG.PROJECT, participant_id=self._extra_info.participant_id
            )
            self._db_run_query(connection=connection, query=query)
            if self._participant_info.properties is None:
                self._logger.debug(
                    "Can not delete properties rows due to absent properties "
                    "configuration.",
                )

            self._logger.debug(
                "Deleted properties rows.",
                extra={"lebels": {"elapsed_time": elapsed()}},
            )

    def _insert_properties_data(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Processing properties")

            deduplication_check = set()
            count = 0

            if self._participant_info.properties is not None:
                properties = self._participant_info.properties.items()
                for property_name, m_property in properties:
                    self._logger.debug(f"Processing '{property_name}' property.")

                    query = self.__properties_insert_rows_sql__.format(
                        project=CFG.PROJECT,
                        ref_region_id=0,  # TODO: Should be fixed after three
                        # months probation period.
                        property_id=m_property.general_info.property_id,
                        property_uri=m_property.general_info.propertyURI,
                        ref_participant_id=self._extra_info.participant_id,
                        name=m_property.general_info.name,
                        address1=m_property.general_info.address.address1,
                        address2=m_property.general_info.address.address2,
                        city=m_property.general_info.address.city,
                        country=m_property.general_info.address.country,
                        state=m_property.general_info.address.state,
                        postal_code=m_property.general_info.address.postalCode,
                        footage=m_property.general_info.grossFloorArea.value,
                        footage_10_categories=0,
                        footage_3_categories=0,
                    )

                    if query in deduplication_check:
                        self._logger.warning(
                            "Found duplication property info "
                            f"{self._extra_info.participant_id}. participant. "
                            "Skiping",
                        )
                        continue
                    deduplication_check.add(query)

                    self._db_run_query(connection=connection, query=query)
                    count += 1
            else:
                self._logger.error("Absent Properties configuration.")

            self._logger.debug(
                f"Completed properties processing. Processed {count} properties.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def _upsert_properties_mapper_data(self, connection: Client) -> None:
        # TODO: Refactor to use parallel operations
        # TODO: Add duplicates check
        with elapsed_timer() as elapsed:
            ref_participant_id = self._extra_info.participant_id
            self._logger.debug(
                f"Processing '{ref_participant_id}' participant's properties mappers"
            )
            if self._participant_info.properties is not None:
                properties = self._participant_info.properties.items()

                for property_name, m_property in properties:
                    self._logger.debug(f"Processing '{property_name}' property.")

                    ref_property_id = m_property.general_info.property_id
                    property_name = m_property.general_info.name
                    property_hash = str(self.get_unique_id(property_name))

                    # check if letter already present in mapper
                    # key is ref_participant_id, ref_property_id property_hash
                    query = self.__properties_mapper_get_row_query__.format(
                        project=CFG.PROJECT,
                        ref_participant_id=ref_participant_id,
                        ref_property_id=ref_property_id,
                        property_hash=property_hash,
                    )

                    rows = list(self._db_run_query(connection, query=query))
                    rows = list(map(dict, rows))
                    if not rows:
                        query = self.__properties_mapper_get_letter_query__.format(
                            project=CFG.PROJECT
                        )
                        rows = list(self._db_run_query(connection, query=query))
                        rows = list(map(dict, rows))
                        property_letter = rows[0]["name"]

                        query = self.__properties_mapper_insert_rows_query__.format(
                            project=CFG.PROJECT,
                            ref_participant_id=ref_participant_id,
                            ref_property_id=ref_property_id,
                            property_hash=property_hash,
                            property_letter=property_letter,
                        )
                        self._db_run_query(connection, query=query)
                    else:
                        self._logger.warning(
                            f"The property '{property_name}' already contain "
                            "assigned letter"
                        )
                    self._logger.debug(
                        f"Completed property '{property_name}' processing."
                    )
            self._logger.debug(
                f"Completed '{ref_participant_id}' participant's "
                "properties mappers processing",
                extra={
                    "labels": {
                        "elapsed time": elapsed(),
                    }
                },
            )

    def _delete_meters_rows(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Starting meters rows deletion.")

            query = self.__meters_delete_rows_sql__.format(
                project=CFG.PROJECT, participant_id=self._extra_info.participant_id
            )
            self._db_run_query(connection=connection, query=query)
            self._logger.debug(
                "Deleted meter rows fot the participant "
                f"#{self._extra_info.participant_id}.",
                extra={"labels": {"elapsed_timer": elapsed()}},
            )

    def _insert_meters(self, connection: Client) -> None:
        with elapsed_timer() as elapsed:
            self._logger.debug("Starting meters processing.")
            deduplication_check = set()
            count = 0
            if self._participant_info.connectors is None:
                self._logger.error("Absent Connectors configuration. Exit")

            for connector in self._participant_info.connectors:
                self._logger.debug(f"Starting {connector.function} processing.")
                for meter_name, meter in connector.meters.items():
                    self._logger.debug(f"Starting {meter_name} processing.")

                    query = self.__meters_insert_rows_sql__.format(
                        project=CFG.PROJECT,
                        meter_id=meter.meter_id,
                        meter_uri=meter.meter_uri,
                        ref_participant_id=self._extra_info.participant_id,
                        type=meter.type,
                        unitOfMeasure=meter.unitOfMeasure,
                        updateFrequency=meter.updateFrequency,
                        haystack=", ".join(set(meter.tags["ids"])),
                    )

                    if query in deduplication_check:
                        self._logger.warning(
                            "Found duplication meter info in meter "
                            f"{meter_name} of connector {connector.function}. "
                            "Skiping ",
                        )
                        continue
                    deduplication_check.add(query)

                    self._db_run_query(connection=connection, query=query)
                    count += 1
                self._logger.debug(f"Completed {connector.function} processing.")

            self._logger.debug(
                f"Completed meters processing. Processed {count} meters.",
                extra={"labels": {"elapsed_time": elapsed()}},
            )

    def __delete_orphan_meters_association(
        self, check_query: str, delete_query: str, orphan_type: str, connection: Client
    ) -> None:
        with elapsed_timer() as elapsed:
            result = list(
                self._db_run_query(
                    connection=connection,
                    query=check_query.format(
                        project=CFG.PROJECT,
                        participant_id=self._extra_info.participant_id,
                    ),
                )
            )

            orphan_cnt = result[0]["row_count"]
            if orphan_cnt:
                self._logger.debug(
                    f"Found '{orphan_cnt}' orphan {orphan_type} in "
                    "'meters_association' table for participant "
                    f"'{self._extra_info.participant_id}'. Removing",
                )
                self._db_run_query(
                    connection=connection,
                    query=delete_query.format(
                        project=CFG.PROJECT,
                        participant_id=self._extra_info.participant_id,
                    ),
                )
                self._logger.debug(
                    f"Removed '{orphan_cnt}' orphan {orphan_type} in "
                    "'meters_association' table for participant "
                    f"'{self._extra_info.participant_id}'.",
                    extra={
                        "labels": {
                            "elapsed_time": elapsed(),
                        }
                    },
                )

    def _delete_orphan_meters_association_meters_ids(self, connection: Client) -> None:
        self.__delete_orphan_meters_association(
            check_query=self.__meters_association_orphan_meter_ids__,
            delete_query=self.__meters_association_delete_orphan_meter_ids__,
            orphan_type="meters",
            connection=connection,
        )

    def _delete_orphan_meters_association_property_ids(
        self, connection: Client
    ) -> None:
        self.__delete_orphan_meters_association(
            check_query=self.__meters_association_orphan_properties_ids__,
            delete_query=self.__meters_association_delete_orphan_properties_ids__,
            orphan_type="properties",
            connection=connection,
        )

    def run(self, **kwargs) -> None:
        self._run_time = parse(tz_info=self.env_tz_info)
        with elapsed_timer() as ellapsed:
            self._logger.info("Start db load processing.")
            dw_connection = self._get_dw_connection()
            action_scope = self._extra_info.scope

            if action_scope == ScopeUpdate.full:
                self._delete_meters_rows(dw_connection)
                self._insert_meters(dw_connection)

                self._delete_properties_rows(dw_connection)
                self._insert_properties_data(dw_connection)
                self._upsert_properties_mapper_data(dw_connection)

                self._delete_participant_rows(dw_connection)
                self._insert_participant_data(dw_connection)

                self._upsert_meters_association_data(dw_connection)
            elif action_scope == ScopeUpdate.update:
                self._upsert_meters_association_data(dw_connection)
            elif action_scope == ScopeUpdate.calendar:
                self._update_calendar(dw_connection)
            else:
                raise UnexpectedActionScope(
                    f"{self.__description__}: "
                    f"Recieved unexpected action scope '{action_scope}'. "
                    f"Allowed scope values are '{self.__allowed_scope__}'. "
                )

            if action_scope != ScopeUpdate.calendar:
                self._delete_orphan_meters_association_meters_ids(dw_connection)
                self._delete_orphan_meters_association_property_ids(dw_connection)

            self._logger.info(
                "Completed db load processing.",
                extra={"labels": {"elapsed_time": ellapsed()}},
            )

    def fetch(self) -> None:
        """Fetch Integration data"""

    def standardize(self) -> None:
        """Standardize Fetched data"""


def main(event, context):  # pylint:disable=unused-argument
    """Entry point"""
    main_logger = Logger(
        name="DW UPDATE",
        level="DEBUG",
        description="DW UPDATE",
        trace_id=uuid.uuid4(),
    )
    with elapsed_timer() as ellapsed:
        main_logger.info("Updating Participant configuration in DataWarehouse.")
        try:
            connector = DwUpdateConnector(env_tz_info=CFG.ENVIRONMENT_TIME_ZONE)
            connector.configure(event)
            connector.run()
        except UnexpectedActionScope as err:
            main_logger.error(f"Unexpectedly closed due to the error '{err}'")
        finally:
            main_logger.info(
                "Completed.", extra={"labels": {"elapsed_time": ellapsed()}}
            )


if __name__ == "__main__":
    import base64
    import json

    DEBUG_LOGGER = Logger(
        name="EXPORT PUBLIC DATA",
        level="DEBUG",
        description="EXPORT PUBLIC DATA",
        trace_id=uuid.uuid4(),
    )

    DEBUG_LOGGER.error("=" * 40)
    import debugpy

    debugpy.listen(5678)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()

    updates_targets = CFG.DEBUG_PARTICIPANTS + ["calendar"]
    for participant_id in updates_targets:
        file_path = CFG.LOCAL_PATH.joinpath(f"dw_update_{participant_id}_payload.json")
        if not file_path.exists():
            DEBUG_LOGGER.error(f"Payload file {file_path} doesn't exists. Skipping")
            continue
        with open(file_path, "r", encoding="utf-8") as exmpl_fl:
            cfg = json.load(exmpl_fl)

        json_config = dumps(cfg).encode("utf-8")
        event_sample = {"data": base64.b64encode(json_config)}

        main(event=event_sample, context=None)
