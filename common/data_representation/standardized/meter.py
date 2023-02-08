"""Code to process standardized values of meters"""
import uuid
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal
from typing import Optional

import pendulum as pdl
from lxml import builder, etree
from pendulum.datetime import DateTime

from common import settings as CFG
from common.data_representation.config import AuditData, BaseConfig
from common.date_utils import DateParseException, format_date, parse, parse_timezone
from common.logging import Logger


class StandardizedMeterException(Exception):
    """Exception class specific to this package."""


@dataclass
class StandardizedMeter:
    """An standardazed meter reprezentation dataclass"""

    # pylint: disable=invalid-name
    meterURI: str = field(
        default="",
        metadata={
            "title": "Path that identifies meter in unique way",
            "description": "Meter URI",
        },
    )

    # pylint: disable=invalid-name
    startTime: DateTime = field(
        default_factory=pdl.now,
        metadata={"title": "Start time of meter value", "description": "start time"},
    )

    # pylint: disable=invalid-name
    endTime: DateTime = field(
        default_factory=pdl.now,
        metadata={"title": "End time of meter value", "description": "end time"},
    )

    usage: Decimal = field(
        default_factory=Decimal,
        metadata={
            "title": "Meter value",
        },
    )

    audit: AuditData = field(default_factory=AuditData)


class Meter(BaseConfig):  # pylint: disable=too-many-instance-attributes
    """An standardazed meter reprezentation object"""

    espm = "http://portfoliomanager.energystar.gov/ns"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    hbd = "http://hourlybuildingdata.com/ns"
    schema_location = (
        "http://hourlybuildingdata.com/ns http://hourlybuildingdata.com/ns/main.xsd"
    )

    ST_METER_DATA_TG = "hbd:meterData"
    ST_METERED_DATA_TG = "hbd:meteredData"
    ST_METERED_METER_URI_TG = "hbd:meterURI"
    ST_METERED_START_TIME_TG = "hbd:startTime"
    ST_METERED_END_TIME_TG = "hbd:endTime"
    ST_METERED_USAGE_TG = "hbd:usage"

    def __init__(
        self,
        path_info: Optional[str] = None,
        meter_timezone: str = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
        env_timezone: Optional[str] = CFG.ENVIRONMENT_TIME_ZONE,
        dt_format: str = CFG.PROCESSING_DATE_FORMAT,
        xml_dt_format: str = CFG.STANDARDIZED_METER_DATE_FORMAT,
    ) -> None:
        super().__init__(path_info=path_info)
        self.meter_tz = parse_timezone(meter_timezone)
        self.env_timezone = parse_timezone(env_timezone)
        self.meter_data_format = dt_format
        self.meter_xml_data_format = xml_dt_format
        self._namespaces = {"hbd": self.hbd, "xsi": self.xsi, "espm": self.espm}
        self._root_element_maker = (
            builder.ElementMaker(  # pylint: disable=c-extension-no-member
                namespace=self._namespaces["hbd"],
                nsmap=self._namespaces,
            )
        )

        self._hbd_element_maker = (
            builder.ElementMaker(  # pylint: disable=c-extension-no-member
                namespace=self._namespaces["hbd"]
            )
        )

        self._espm_element_maker = (
            builder.ElementMaker(  # pylint: disable=c-extension-no-member
                namespace=self._namespaces["espm"],
            )
        )

        self._data = StandardizedMeter()

    def _parse_date(
        self,
        value: str,
        dt_format: str,
        timezone: Optional[str] = CFG.DEFAULT_LOCAL_TIMEZONE_NAME,
        msg_prfx: str = "",
    ) -> DateTime:
        try:
            return parse(value, dt_format=dt_format, tz_info=timezone)
        except DateParseException as err:
            self._logger.error(
                f'[{msg_prfx}] - Cannot convert given value "{value}" '
                f'to date due to the error "{err}"'
            )
            raise StandardizedMeterException from err

    @property
    def start_time(self):  # pylint: disable=missing-function-docstring
        return self._data.startTime

    @start_time.setter
    def start_time(self, value):
        self._data.startTime = self._parse_date(
            value, dt_format=self.meter_data_format, msg_prfx="Start Time"
        )

    @property
    def end_time(self):  # pylint: disable=missing-function-docstring
        return self._data.endTime

    @end_time.setter
    def end_time(self, value):
        self._data.endTime = self._parse_date(
            value, dt_format=self.meter_data_format, msg_prfx="End Time"
        )

    @property
    def created_date(self):  # pylint: disable=missing-function-docstring
        return self._data.audit.createdDate

    @created_date.setter
    def created_date(self, value):

        date = self._parse_date(
            value, dt_format=self.meter_data_format, msg_prfx="Created Time"
        )

        str_value = format_date(date, self.meter_xml_data_format)

        self._data.audit.createdDate = str_value

    @property
    def created_by(self):  # pylint: disable=missing-function-docstring
        return self._data.audit.createdBy

    @created_by.setter
    def created_by(self, value):
        self._data.audit.createdBy = str(value).strip()

    @property
    def usage(self):  # pylint: disable=missing-function-docstring
        return self._data.usage

    @usage.setter
    def usage(self, value):
        value = str(value)
        if not value.lstrip("-").replace(".", "", 1).isdigit():
            raise StandardizedMeterException(
                f'ERROR: Usage: Looks like given value "{value}" is not a number'
            )
        self._data.usage = Decimal(value)

    @property
    def meter_uri(self):  # pylint: disable=missing-function-docstring
        return self._data.meterURI

    @meter_uri.setter
    def meter_uri(self, value):
        self._data.meterURI = str(value).strip()

    @property
    def meter_id(self):  # pylint: disable=missing-function-docstring
        return self.get_unique_id(self._data.meterURI)

    def as_xml(self):
        """Generate XML representation of the meter data"""

        root_tag = self._root_element_maker.meterData(
            self._hbd_element_maker.meteredData(
                self._hbd_element_maker.meterURI(self.meter_uri),
                self._hbd_element_maker.startTime(
                    format_date(self.start_time, dt_format=self.meter_xml_data_format)
                ),
                self._hbd_element_maker.endTime(
                    format_date(self.end_time, dt_format=self.meter_xml_data_format)
                ),
                self._hbd_element_maker.usage(str(self.usage)),
                self._hbd_element_maker.audit(
                    self._espm_element_maker.createdBy(self.created_by),
                    self._espm_element_maker.createdDate(self.created_date),
                ),
            )
        )
        root_tag.attrib[f"{{{self.xsi}}}schemaLocation"] = self.schema_location
        return root_tag

    def as_json(self):
        """Convert Data representation to json"""
        json_data = asdict(self._data)

        for key in ("startTime", "endTime"):
            json_data[key] = format_date(json_data[key], self.meter_xml_data_format)
        return json_data

    def as_str(self):
        """Convert XML representation to string"""
        return etree.tostring(  # pylint: disable=c-extension-no-member
            self.as_xml(), pretty_print=True, xml_declaration=True, encoding="UTF-8"
        )

    def as_dataclass(self) -> dataclass:
        return replace(self._data)

    def parse_string_xml(self, data: str, config: dict) -> None:
        parser = etree.XMLParser(  # pylint: disable=c-extension-no-member
            ns_clean=True, recover=False, encoding="utf-8", remove_comments=True
        )
        try:
            root = etree.fromstring(  # pylint: disable=c-extension-no-member
                bytes(data, encoding="utf-8"), parser=parser
            )
        except etree.XMLSyntaxError as err:  # pylint: disable=c-extension-no-member
            raise StandardizedMeterException from err

        mapped_root_tag = self._map_element_tag(root)

        if mapped_root_tag != self.ST_METER_DATA_TG:
            raise StandardizedMeterException(
                f"ERROR: STANDARDIZED DATA: Unxpected root tag {mapped_root_tag}. "
                f"Expected root tag is {self.ST_METER_DATA_TG}"
            )

        for xml_el in root:
            mapped_tag = self._map_element_tag(xml_el)
            if mapped_tag != self.ST_METERED_DATA_TG:
                raise StandardizedMeterException(
                    f"ERROR: STANDARDIZED DATA: Unxpected metered tag {mapped_tag}. "
                    f"Expected root tag is {self.ST_METERED_DATA_TG}"
                )

            for data_el in xml_el.getchildren():
                mapped_dt_tag = self._map_element_tag(data_el)

                if mapped_dt_tag == self.ST_METERED_METER_URI_TG:
                    self.meter_uri = data_el.text
                elif mapped_dt_tag == self.ST_METERED_START_TIME_TG:
                    self.start_time = parse(
                        data_el.text, dt_format=self.meter_xml_data_format
                    )
                elif mapped_dt_tag == self.ST_METERED_END_TIME_TG:
                    self.end_time = parse(
                        data_el.text, dt_format=self.meter_xml_data_format
                    )
                elif mapped_dt_tag == self.ST_METERED_USAGE_TG:
                    self.usage = data_el.text
                elif mapped_dt_tag == self.AUDIT_TG:
                    self._data.audit = replace(
                        self._data.audit,
                        **self._parse_audit(
                            data_el,
                            allowed_fields=self._get_data_fields(self._data.audit),
                        ),
                    )
                else:
                    use = config.get("use", "local").strip().lower()

                    self._logger.warning(
                        f"Unexpected tag '{mapped_tag}' in the meter config "
                        f"file \"{config[use]['full_path']}\"."
                    )


if __name__ == "__main__":
    import json

    MAIN_LOGGER = Logger(
        name="Participant run",
        level="DEBUG",
        description="Participant RUN",
        trace_id=uuid.uuid4(),
    )

    def main():
        """Test entry point"""
        # pretty_print = pprint.PrettyPrinter(indent=4)
        meter = Meter()

        meter.created_date = "2021-10-11T15:00:00UTC"
        meter.start_time = "2021-10-11T12:00:00UTC"
        meter.end_time = "2021-10-11T12:59:00UTC"
        meter.created_by = "OpenWeather Connector"
        meter.usage = 10
        meter.meter_uri = "test"

        # meter.read_from_file(
        #     "standardization/sample_data/real_data/standardized/cloudiness/2021-12-28T18:00:00"
        # )
        # meter.read_from_bucket(
        #     bucket="prototype_develop-epbp_participant_0",
        #     subdirectory="openweather/standardized/cloudiness",
        #     filename="2022-01-05T21:00:00",
        #     binary_mode=False,
        # )
        json_repr = meter.as_json()
        filename = CFG.LOCAL_PATH.joinpath("standarized.json")
        with open(filename, "w", encoding="utf-8") as meter_fl:
            json.dump(json_repr, meter_fl, indent=4, ensure_ascii=False, default=str)
        xml_str = meter.as_str()
        filename = CFG.LOCAL_PATH.joinpath("standardized.xml")
        with open(filename, "wb") as xml_fl:
            xml_fl.write(xml_str)

    MAIN_LOGGER.debug(">" * 40)
    # import debugpy

    # debugpy.listen(5678)
    # debugpy.wait_for_client()  # blocks execution until client is attached
    # debugpy.breakpoint()
    main()
