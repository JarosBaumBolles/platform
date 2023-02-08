"""Participant Configuration file parser."""
import uuid
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Dict, Optional

from lxml import etree

from common.data_representation.config import (
    AuditData,
    BaseConfig,
    ConfigException,
    TagsData,
)
from common.data_representation.config.base_exceptions import (
    UnexpectedXmlElementException,
    XmlTypeException,
)
from common.data_representation.config.meter import MeterConfig
from common.data_representation.config.property import PropertyConfig
from common.logging import Logger


@dataclass
class ContactData:
    """Participant contact representation"""

    id: int = -1  # pylint: disable=invalid-name
    name: str = ""
    firstName: str = ""  # pylint: disable=invalid-name
    lastName: str = ""  # pylint: disable=invalid-name
    email: str = ""
    address: dict = field(default_factory=dict)
    jobTitle: str = ""  # pylint: disable=invalid-name
    phone: str = ""


@dataclass
class ConnectorData:
    """Participant meter representation"""

    meters: dict = field(default_factory=dict)
    function: str = ""
    timezone: str = ""
    fetchStrategy: dict = field(default_factory=dict)  # pylint: disable=invalid-name
    parameters: Dict[str, str] = field(default_factory=dict)
    rawDataLocation: dict = field(default_factory=dict)  # pylint: disable=invalid-name


@dataclass
class PropertiesData:
    """Participant prperties representation"""

    properties: dict = field(default_factory=dict)


# pylint: disable=W0223
class ParticipantConfig(BaseConfig):
    """Participant config parser"""

    __description__ = "PARTICIPANT CONFIG"

    NAME_TG = "hbd:name"
    CONTACT_TG = "hbd:contact"
    CONNECTORS_TG = "hbd:connectors"
    CONNECTOR_TG = "hbd:connector"
    PROPERTIES_TG = "hbd:properties"
    PROPERTIES_CHILD_TG = "hbd:propertyURI"
    CONNECTOR_METER_URI_TG = "hbd:meterURI"
    CONNECTOR_FUNC_TG = "hbd:function"
    CONNECTOR_TIMEZONE_TG = "hbd:timezone"
    CONNECTOR_FETCHSTRATEGY_TG = "hbd:fetchStrategy"
    CONNECTOR_FETCHSTRATEGY_PUSH_TG = "hbd:push"
    CONNECTOR_FETCHSTRATEGY_DESK_TG = "hbd:description"
    CONNECTOR_RAWDATALOCATION_TG = "hbd:rawDataLocation"
    CONNECTOR_PARAMETER_TG = "hbd:parameter"

    __alowed_uses__ = {"local": "local", "gcp": "gcp"}

    def __init__(self, path_info=None) -> None:
        super().__init__(path_info=path_info)
        self.contact_data = ContactData()

        self.connectors_data = []
        self.connectors_configs = []

        self.properties_data = PropertiesData()
        self.audit_data = AuditData()
        self.tags_data = TagsData()

    def parse_string_xml(self, data: str, config: dict):
        parser = etree.XMLParser(  # pylint: disable=c-extension-no-member
            ns_clean=True, recover=False, encoding="utf-8", remove_comments=True
        )
        try:
            root = etree.fromstring(  # pylint: disable=c-extension-no-member
                bytes(data, encoding="utf-8"), parser=parser
            )
        except etree.XMLSyntaxError as err:  # pylint: disable=c-extension-no-member
            msg = str(err)
            raise ConfigException(msg)  # pylint: disable=raise-missing-from

        for elmnt in root:
            elmnt_mpd_tag = self._map_element_tag(elmnt)
            if elmnt_mpd_tag == self.NAME_TG:
                self.contact_data.name = (elmnt.text or "").strip()
            elif elmnt_mpd_tag == self.CONTACT_TG:
                params = self._parse_contacts(elmnt)
                self.contact_data = replace(self.contact_data, **params)

            elif elmnt_mpd_tag == self.CONNECTORS_TG:
                try:
                    self._parse_connectors(elmnt, config)
                except XmlTypeException as err:
                    self._logger.error(
                        f"Can not parse element due to the error '{err}'. Skiping."
                    )
                    continue
                except UnexpectedXmlElementException as err:
                    self._logger.warning(f"Skip due to the {err}")
                    continue

            elif elmnt_mpd_tag == self.AUDIT_TG:
                params = self._parse_audit(
                    elmnt, allowed_fields=self._get_data_fields(self.audit_data)
                )
                self.audit_data = replace(self.audit_data, **params)

            elif elmnt_mpd_tag == self.TAGS_TG:
                params = self._parse_tags(elmnt)
                self.tags_data = replace(self.tags_data, **params)

            elif elmnt_mpd_tag == self.PROPERTIES_TG:
                try:
                    self._parse_properties(elmnt, config)
                except UnexpectedXmlElementException as err:
                    self._logger.warning(
                        f"Can not parse elemet due to the '{err}'. Skiping"
                    )
                except (XmlTypeException, ConfigException) as err:
                    self._logger.error(
                        f"Can not parse element due to the error '{err}'. Skiping."
                    )

    def _load_property_config(self, config) -> dataclass:
        property_cfg = PropertyConfig()

        use = config.get("use", "local").strip().lower()

        if use == "local":
            # We expect only file name with dot extensions in case of local use
            file_name = Path(config[use]["meter_config_name"]).name
            property_cfg.read_from_file(
                str(Path(config[use]["base"]).joinpath(file_name))
            )
        elif use == "gcp":
            bucket = config[use]["bucket"]
            file_path_str = config[use]["property_path"]
            file_path = Path(file_path_str)
            path = config[use]["path"]
            expected_filename = file_path.name
            expected_path = (
                f"{bucket}/{path.lstrip('/').rstrip('/')}/" f"{expected_filename}"
            )

            if file_path_str != expected_path:
                self._logger.error(
                    f"Expected path {expected_path} is not equal with config "
                    f"provided value {file_path_str}"
                )

            property_cfg.read_from_bucket(
                bucket=str(file_path.parent.parent.name).lstrip("/"),
                subdirectory=str(file_path.parent.name).lstrip("/"),
                filename=file_path.name,
                binary_mode=config[use]["binary_mode"],
            )
        else:
            self._logger.error(
                f"Unexpected local pointer {use}. Allowed values are local, gcp"
            )

        return property_cfg

    def _parse_plugin_parameter(self, param_el):
        param_el_mpd_tag = self._map_element_tag(param_el)
        if param_el_mpd_tag != self.CONNECTOR_PARAMETER_TG:
            return {}

        parameters = {}
        children = param_el.getchildren()
        key = param_el.attrib[f"{{{param_el.nsmap[param_el.prefix]}}}property"]
        if not children:
            parameters[key] = param_el.attrib[
                f"{{{param_el.nsmap[param_el.prefix]}}}value"
            ]
        else:
            parameters[key] = {}
            for child in children:
                child_params = self._parse_plugin_parameter(child)
                parameters[key].update(child_params)

        return parameters

    def _load_meter_config(self, config: dict) -> dataclass:
        meter_config = MeterConfig()
        use = config.get("use", "local").strip().lower()

        if use not in self.__alowed_uses__:
            raise ConfigException(
                "Unexpected local pointer {use}. Allowed values are 'local' or 'gcp'"
            )

        if use == "local":
            meter_config.read_from_file(
                Path(config[use]["base"]).joinpath(config[use]["meter_config_name"])
            )
        elif use == "gcp":
            bucket = config[use]["bucket"]
            file_path_str = config[use]["meter_config_name"]
            file_path = Path(file_path_str)
            path = config[use]["path"]
            expected_filename = file_path.name
            expected_path = (
                f"{bucket}/{path.lstrip('/').rstrip('/')}/" f"{expected_filename}"
            )

            if file_path_str != expected_path:
                self._logger.error(
                    f"Expected path {expected_path} is not equal with config "
                    f"provided value {file_path_str}"
                )

            meter_config.read_from_bucket(
                bucket=str(file_path.parent.parent.name).lstrip("/"),
                subdirectory=str(file_path.parent.name).lstrip("/"),
                filename=file_path.name,
                binary_mode=config[use]["binary_mode"],
            )

        return meter_config

    def _parse_contacts(
        self, contact_root_el: etree._Element  # pylint: disable=c-extension-no-member
    ) -> dict:
        # pylint: disable=c-extension-no-member
        if contact_root_el is None or not etree.iselement(contact_root_el):
            return {}
        allowed_fields = set(self._get_data_fields(self.contact_data))
        values = {}
        for ct_elem in contact_root_el.getchildren():
            mpd_ct_elem_tag = self._map_element_tag(ct_elem)
            field_name = mpd_ct_elem_tag.split(":")[-1]
            if field_name not in allowed_fields:
                self._logger.warning(
                    f"Found excessive contact children '{mpd_ct_elem_tag}' in "
                    "participant XML. Skipping"
                )
                continue

            values[field_name] = (
                ct_elem.text if field_name != "address" else ct_elem.attrib
            )
        return values

    # TODO: Refactor to use the same code for parsing connectors and properties
    def _parse_properties(
        self,
        participant_root_el: etree._Element,  # pylint: disable=c-extension-no-member
        config: dict,
    ) -> None:
        self._validate_xml_element(participant_root_el)

        mpd_properties_root_el = self._map_element_tag(participant_root_el)
        if mpd_properties_root_el != self.PROPERTIES_TG:
            raise UnexpectedXmlElementException(
                "Unexpected tag {mpd_properties_root_el} in properties section"
            )

        for idx, ct_el in enumerate(participant_root_el.getchildren(), 1):
            try:
                self._parse_property(ct_el, config)
            except (
                XmlTypeException,
                UnexpectedXmlElementException,
                ConfigException,
            ) as err:
                self._logger.warning(
                    f"Can not parse property #{idx} due to the error '{err}'. "
                    "Skipping"
                )

    def _parse_property(
        self,
        property_el: etree._Element,  # pylint: disable=c-extension-no-member
        config: dict,
    ) -> Optional[ConnectorData]:
        self._validate_xml_element(property_el)

        pr_ch_mp_tg = self._map_element_tag(property_el)
        if pr_ch_mp_tg != self.PROPERTIES_CHILD_TG:
            raise UnexpectedXmlElementException(
                f"Unexpected tag {pr_ch_mp_tg} in properties section."
            )
        raw_file_path = (property_el.text or "").strip()
        if not raw_file_path:
            raise ConfigException("Empty Property path.")

        config[config.get("use", "local")]["property_path"] = raw_file_path

        self.properties_data.properties[raw_file_path] = self._load_property_config(
            config
        )

    def _parse_connectors(
        self,
        connectors_root_el: etree._Element,  # pylint: disable=c-extension-no-member
        config: dict,
    ) -> None:
        self._validate_xml_element(connectors_root_el)

        mpd_connectors_root_el = self._map_element_tag(connectors_root_el)
        if mpd_connectors_root_el != self.CONNECTORS_TG:
            raise UnexpectedXmlElementException(
                "Unexpected tag {mpd_connectors_root_el} in connectors section"
            )

        for idx, ct_el in enumerate(connectors_root_el.getchildren(), 1):
            try:
                self.connectors_data.append(self._parse_connector(ct_el, config))
            except (
                XmlTypeException,
                UnexpectedXmlElementException,
                ConfigException,
            ) as err:
                self._logger.error(
                    f"Can not parse connector #{idx} due to the error '{err}'. "
                    "Skipping"
                )

    def _parse_connector(
        self,
        connector_root_el: etree._Element,  # pylint: disable=c-extension-no-member
        config: dict,
    ) -> Optional[ConnectorData]:
        self._validate_xml_element(connector_root_el)

        mpd_connector_root_el_tag = self._map_element_tag(connector_root_el)
        if mpd_connector_root_el_tag != self.CONNECTOR_TG:
            raise UnexpectedXmlElementException(
                f"Unexpected tag {mpd_connector_root_el_tag} in connectors "
                f"section of \"{config[config.get('use', 'local')]['full_path']}\" "
                "configuration file."
            )

        connector = ConnectorData()
        parameters = {}
        for cnctr_el in connector_root_el.iterchildren():
            mpd_cnctr_tag = self._map_element_tag(cnctr_el)

            if mpd_cnctr_tag == self.CONNECTOR_METER_URI_TG:
                config[config.get("use", "local")]["meter_config_name"] = cnctr_el.text
                meter_config = self._load_meter_config(config)
                connector.meters[cnctr_el.text] = meter_config.as_dataclass()

            elif mpd_cnctr_tag == self.CONNECTOR_FUNC_TG:
                connector.function = cnctr_el.text
            elif mpd_cnctr_tag == self.CONNECTOR_TIMEZONE_TG:
                connector.timezone = cnctr_el.text
            elif mpd_cnctr_tag == self.CONNECTOR_RAWDATALOCATION_TG:
                connector.rawDataLocation = {
                    "bucket": cnctr_el.attrib[
                        f"{{{cnctr_el.nsmap[cnctr_el.prefix]}}}bucket"
                    ],
                    "path": cnctr_el.attrib[
                        f"{{{cnctr_el.nsmap[cnctr_el.prefix]}}}path"
                    ],
                }
            elif mpd_cnctr_tag == self.CONNECTOR_FETCHSTRATEGY_TG:
                # TODO; Add Validation
                strategy_el = next(cnctr_el.iterchildren())

                connector.fetchStrategy = {
                    "type": self._map_element_tag(strategy_el).split(":")[-1],
                    "description": next(strategy_el.iterchildren()).text,
                }
            elif mpd_cnctr_tag == self.CONNECTOR_PARAMETER_TG:
                params = self._parse_plugin_parameter(cnctr_el)
                parameters.update(params)
        connector.parameters = parameters
        return connector

    def as_json(self):
        data = {
            "contact": asdict(self.contact_data),
            "connectors": list(map(asdict, self.connectors_data)),
            "audit": asdict(self.audit_data),
            "tags": asdict(self.tags_data),
            "properties": {},
        }

        for pr_name, prprt in self.properties_data.properties.items():
            data["properties"][pr_name] = prprt.as_json()
        return data


if __name__ == "__main__":
    import json
    from common import settings as CFG

    MAIN_LOGGER = Logger(
        name="Participant run",
        level="DEBUG",
        description="Participant RUN",
        trace_id=uuid.uuid4(),
    )

    def main():
        """Debug entrypoint"""

        MAIN_LOGGER.debug("-" * 40)

        # import debugpy

        # debugpy.listen(5678)
        # debugpy.wait_for_client()  # blocks execution until client is attached
        # debugpy.breakpoint()
        for bucket in CFG.DEBUG_BUCKETS:

            # data = cfg.read_from_file(
            #   "standardization/sample_data/real_data/config/"
            #   "participatt_0_openweather_wattime/participant.xml",
            # )
            cfg = ParticipantConfig()
            cfg.read_from_bucket(
                bucket=bucket,
                subdirectory="config/",
                filename="participant.xml",
                binary_mode=False,
            )
            json_cfg = cfg.as_json()
            filename = CFG.LOCAL_PATH.joinpath(f"{bucket}.json")
            with open(filename, "w", encoding="UTF-8") as prtcpnt_fl:
                json.dump(json_cfg, prtcpnt_fl, indent=4)

    main()
