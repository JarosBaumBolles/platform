"""Meter Configuration file parser."""
from dataclasses import asdict, dataclass, field, replace

from lxml import etree

from common.data_representation.config import (
    AuditData,
    BaseConfig,
    ConfigException,
    TagsData,
)


@dataclass
class MeteredDataLocationData:
    """Meter Data Location dataclass"""

    bucket: str = ""
    path: str = ""


@dataclass
class MeterData:
    """Meter Data dataclass"""

    type: str = ""
    meter_uri: str = ""
    meter_id: int = 0
    unitOfMeasure: str = ""  # pylint: disable=invalid-name
    updateFrequency: str = ""  # pylint: disable=invalid-name
    # pylint: disable=invalid-name
    meteredDataLocation: MeteredDataLocationData = field(
        default_factory=MeteredDataLocationData
    )
    tags: TagsData = field(default_factory=TagsData)
    audit: AuditData = field(default_factory=AuditData)
    # meta_info: MeterMetaInfo = field(default_factory=MeterMetaInfo)


# pylint: disable=W0223
class MeterConfig(BaseConfig):
    """Meter Configuration representation"""

    __description__ = "METER CONFIG"

    METER_TYPE_TG = "hbd:type"
    METER_UNIT_OF_MEASURE_TG = "hbd:unitOfMeasure"
    METER_UPDATE_FREQUENCY_TG = "hbd:updateFrequency"
    METER_DATALOCATION_TG = "hbd:meteredDataLocation"
    METER_URI_TG = "hbd:meterURI"

    TEXT_TAGS = {
        METER_TYPE_TG,
        METER_UNIT_OF_MEASURE_TG,
        METER_UPDATE_FREQUENCY_TG,
    }

    def __init__(self, path_info=None) -> None:
        super().__init__(path_info=path_info)
        self._data = MeterData()

    def parse_string_xml(self, data: str, config: dict) -> None:
        # pylint: disable=c-extension-no-member
        parser = etree.XMLParser(
            ns_clean=True, recover=False, encoding="utf-8", remove_comments=True
        )
        try:
            root = etree.fromstring(
                bytes(data, encoding="utf-8"), parser=parser
            )  # pylint: disable=c-extension-no-member
        except etree.XMLSyntaxError as err:  # pylint: disable=c-extension-no-member
            raise ConfigException from err

        text_params = {}

        for xml_el in root:
            mapped_tag = self._map_element_tag(xml_el)
            if mapped_tag == self.METER_URI_TG:
                self._data.meter_uri = xml_el.text.strip()
                self._data.meter_id = self.get_unique_id(self._data.meter_uri)

            elif mapped_tag == self.METER_DATALOCATION_TG:
                self._data.meteredDataLocation.path = xml_el.attrib[
                    f"{{{xml_el.nsmap[xml_el.prefix]}}}path"
                ]
                self._data.meteredDataLocation.bucket = xml_el.attrib[
                    f"{{{xml_el.nsmap[xml_el.prefix]}}}bucket"
                ]
            elif mapped_tag == self.AUDIT_TG:
                self._data.audit = replace(
                    self._data.audit,
                    **self._parse_audit(
                        xml_el, allowed_fields=self._get_data_fields(self._data.audit)
                    ),
                )
            elif mapped_tag == self.TAGS_TG:
                self._data.tags = replace(self._data.tags, **self._parse_tags(xml_el))
            elif mapped_tag in self.TEXT_TAGS:
                text_params[mapped_tag.split(":")[-1]] = xml_el.text.strip()
            else:
                use = config.get("use", "local").strip().lower()

                self._logger.warning(
                    f"Unexpected tag '{mapped_tag}' in the meter config file"
                    f" \"{config[use]['full_path']}\"."
                )

        self._data = replace(self._data, **text_params)

    def read_from_file(self, cfg_path: str = None) -> None:
        super().read_from_file(cfg_path)
        self._data.meta_info.meter_uri = self._config_file_info["local"]["path"]
        self._data.meta_info.meter_id = self.get_unique_id(
            self._data.meta_info.meter_uri
        )

    def as_json(self) -> dict:
        return asdict(self._data)

    def as_dataclass(self) -> dataclass:
        return replace(self._data)


if __name__ == "__main__":
    import json
    import pprint

    from common import settings as CFG

    def main():
        """Debug function"""
        pretty_print = pprint.PrettyPrinter(indent=4)
        cfg = MeterConfig()
        cfg.read_from_file(
            "standardization/sample_data/config/openweather_wattime"
            "/meter_openweather_wind_speed.xml"
        )
        # cfg.read_from_bucket(
        #     bucket="prototype_develop-epbp_participant_0",
        #     subdirectory="config",
        #     filename="meter_openweather_wind_speed.xml",
        #     binary_mode=False,
        # )
        json_cfg = cfg.as_json()
        filename = CFG.LOCAL_PATH.joinpath("meter_openweather_wind_speed.json")
        with open(filename, "wb") as meter_fl:
            json.dump(json_cfg, meter_fl, indent=4)
        pretty_print.pprint(json_cfg)

    main()
