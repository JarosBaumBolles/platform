"""Property Configuration file parser."""
import uuid
from collections import defaultdict
from dataclasses import dataclass, is_dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

from dataclass_factory import Factory
from lxml import etree

from common.data_representation.config import BaseConfig, ConfigException
from common.data_representation.config.base_exceptions import XmlTypeException
from common.data_representation.config.property.data_structure import (
    AdditionalInfo,
    MeterPropertyAssociationList,
    PropertyGeneralInfo,
    PropertyUse,
)
from common.data_representation.config.property.exceptions import (
    GeneralInfoParsingException,
    MpaListParsingException,
    PropertyUsesParsingException,
    UseDetailsParsingException,
    UseTimingParsingException,
)
from common.data_representation.config.property.property_schema import PROPERTY_SCHEMA
from common.logging import Logger


class PropertyConfig(BaseConfig):
    """Property config parser"""

    __description__ = "PROPERTY PARCING"

    def __init__(self, path_info: str = None, schema: dict = PROPERTY_SCHEMA) -> None:
        super().__init__(path_info=path_info)
        self._schema = schema
        self._factory = Factory()
        self._general_info: Optional[PropertyGeneralInfo] = None
        self._property_uses: Optional[PropertyUse] = None
        self._meter_property_association: Optional[MeterPropertyAssociationList] = None
        self._additional_info: Optional[AdditionalInfo] = None

    def parse_string_xml(self, data: Union[str, bytes], config: dict) -> None:
        # pylint: disable=c-extension-no-member
        parser = etree.XMLParser(
            ns_clean=True, recover=False, encoding="utf-8", remove_comments=True
        )
        try:
            if isinstance(data, str):
                data = bytes(data, encoding="utf-8")
            root = etree.fromstring(data, parser=parser)
        except etree.XMLSyntaxError as err:  # pylint: disable=c-extension-no-member
            raise ConfigException from err

        mp_root_tg = self._map_element_tag(root)

        if mp_root_tg != self._schema.property.tag.tag:
            raise ConfigException(
                f"Unexpected root tag '{mp_root_tg}'. The root property must "
                f"be '{self._schema.property.tag.tag}'. Skip parcing."
            )
        general_info = {}
        additional_info = {}
        for elmnt in root:
            elmnt_mpd_tag = self._map_element_tag(elmnt)
            if elmnt_mpd_tag not in self._schema.property.children_tags.tags:
                self._logger.warning(
                    f"Unexpected {elmnt_mpd_tag} tag in {mp_root_tg} section. "
                    "Skiping"
                )
                continue
            gnrl_schema = self._schema.property.general_info
            prprt_uses_schema = self._schema.property.property_uses
            mtr_prt_as_lst_schema = (
                self._schema.property.meter_property_association_list
            )
            additional_info_schema = self._schema.property.additional_info

            if elmnt_mpd_tag in gnrl_schema.children_tags.tags:
                try:
                    general_info.update(self._parse_general_info(elmnt, gnrl_schema))
                except (GeneralInfoParsingException, XmlTypeException) as err:
                    self._logger.error(
                        f"Can not parse general info due to the error '{err}'. "
                        "Skiping"
                    )
                    continue
            elif elmnt_mpd_tag == prprt_uses_schema.tag.tag:
                try:
                    result = self._parse_property_uses(elmnt, prprt_uses_schema)
                    self._property_uses = self._factory.load(result, PropertyUse)
                except (PropertyUsesParsingException, XmlTypeException) as err:
                    self._logger.error(
                        f"Can not parse property uses due to the error '{err}'. "
                        "Skiping"
                    )
                    continue
            elif elmnt_mpd_tag == mtr_prt_as_lst_schema.tag.tag:
                try:
                    result = self._parse_meter_property_association_list(
                        elmnt, mtr_prt_as_lst_schema
                    )
                    self._meter_property_association = self._factory.load(
                        result, MeterPropertyAssociationList
                    )
                except (MpaListParsingException, XmlTypeException) as err:
                    self._logger.error(
                        f"Can not parse MeterPropertyAssociationList due to "
                        f"the error '{err}'. Skiping"
                    )
                    continue

            elif elmnt_mpd_tag in additional_info_schema.children_tags.tags:
                try:
                    additional_info.update(
                        self._parse_additional_info(elmnt, additional_info_schema)
                    )
                except (GeneralInfoParsingException, XmlTypeException) as err:
                    self._logger.error(
                        f"Can not parse general info due to the error '{err}'."
                        " Skiping"
                    )
                    continue

        self._general_info = self._factory.load(general_info, PropertyGeneralInfo)
        self._general_info.property_id = self.get_unique_id(
            self._general_info.propertyURI
        )
        self._additional_info = self._factory.load(additional_info, AdditionalInfo)

    def _parse_additional_info(
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)

        mp_tg = self._map_element_tag(xml_el)
        if mp_tg not in self._schema.property.additional_info.children_tags.tags:
            raise GeneralInfoParsingException(
                f"Unexpected {mp_tg} parameter in additional info elements."
                f"Allowed elements are "
                f"{self._schema.property.additional_info.children_tags.tags}."
            )

        gnrl_prt_name = mp_tg.split(":")[-1]
        gnrl_prt_type = getattr(schema, gnrl_prt_name, "")
        if not gnrl_prt_name:
            raise GeneralInfoParsingException(
                f"Cannot find {gnrl_prt_name} in general info schema."
            )

        gnrl_prt_type = gnrl_prt_type.type
        getter_func = getattr(self, f"_get_{gnrl_prt_type}", "")
        if not callable(getter_func):
            raise GeneralInfoParsingException(
                f"Cannot find _get_{gnrl_prt_type} getter."
            )
        return {gnrl_prt_name: getter_func(xml_el)}

    def _parse_meter_property_association_list(
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)
        mpa_mp_tg = self._map_element_tag(xml_el)

        if (
            schema.tag.tag
            != self._schema.property.meter_property_association_list.tag.tag
        ):
            raise MpaListParsingException(
                "Wrong schema provided. Expected meterPropertyAssociationList schema "
            )

        if mpa_mp_tg != schema.tag.tag:
            raise MpaListParsingException(
                "Expect meterPropertyAssociationList element "
                f"{self._schema.property.meter_property_association_list.tag.tag} "
                f"instead {mpa_mp_tg}"
            )
        mpa_list_data = defaultdict(list)
        for mpa_el in xml_el.getchildren():
            mpa_el_mp_tg = self._map_element_tag(mpa_el)
            if mpa_el_mp_tg not in schema.children_tags.tags:
                self._logger.warning(
                    f"Unexpected {mpa_el_mp_tg} tag in {mpa_mp_tg} section." " Skiping"
                )
            mpa_name = mpa_el_mp_tg.split(":")[-1]
            result = self._parse_mpa_list_child(mpa_el, getattr(schema, mpa_name))
            mpa_list_data[mpa_name].append(result)
        return mpa_list_data

    def _parse_mpa_list_child(
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)

        mpa_ch_el_mp_tg = self._map_element_tag(xml_el)
        expected_schema = getattr(
            self._schema.property.meter_property_association_list,
            mpa_ch_el_mp_tg.split(":")[-1],
        )
        if schema.children_tags.tags != expected_schema.children_tags.tags:
            raise MpaListParsingException(
                "Wrong schema provided. Expected meterPropertyAssociationList schema "
            )
        mpa_data = {}
        for mpa_ch_el in xml_el.getchildren():
            mpa_ch_el_mp_tag = self._map_element_tag(mpa_ch_el)
            if mpa_ch_el_mp_tag not in schema.children_tags.tags:
                self._logger.warning(
                    f"Unexpected {mpa_ch_el_mp_tag} tag in {mpa_ch_el_mp_tg} "
                    "section. Skiping"
                )
                continue
            mpa_ch_name = mpa_ch_el_mp_tag.split(":")[-1]
            mpa_ch_type = getattr(schema, mpa_ch_name, "")
            if not mpa_ch_type:
                self._logger.warning(f"Cannot find {mpa_ch_name} in useDetails schema.")
                continue
            mpa_ch_type = mpa_ch_type.type
            getter_func = getattr(self, f"_get_{mpa_ch_type}", "")
            if not callable(getter_func):
                self._logger.warning(f"Cannot find _parse_{mpa_ch_type} getter.")
                continue
            mpa_data[mpa_ch_name] = getter_func(mpa_ch_el)
        return mpa_data

    def _parse_property_uses(
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)
        mp_tg = self._map_element_tag(xml_el)
        if mp_tg != self._schema.property.property_uses.tag.tag:
            raise PropertyUsesParsingException(
                "Unexpected {mp_tg} parameter in property uses elements."
                f"Allowed elements are {self._schema.property.property_uses.tag.tag}."
            )

        raw_data = defaultdict(list)

        for pr_el in xml_el.getchildren():
            pu_el_mp_tg = self._map_element_tag(pr_el)

            if pu_el_mp_tg not in schema.children_tags.tags:
                self._logger.warning(
                    f"Unexpected {pu_el_mp_tg} tag in {mp_tg} section. Skiping"
                )
            pu_prt_name = pu_el_mp_tg.split(":")[-1]

            result = self._parse_property_uses_child(
                pr_el, getattr(schema, pu_prt_name)
            )
            raw_data[pu_prt_name].append(result)
        return raw_data

    def _parse_property_uses_child(
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)
        pu_ch_el_mp_tg = self._map_element_tag(xml_el)
        if pu_ch_el_mp_tg not in self._schema.property.property_uses.children_tags.tags:
            raise PropertyUsesParsingException(
                f"Unexpected xml_el '{pu_ch_el_mp_tg}'"
                "as property uses child. Expected one from the list "
                f"{{{self._schema.property.property_uses.children_tags.tags}}}"
            )
        raw_data = {}
        for pu_ch_el in xml_el.getchildren():
            pu_ch_el_mp_tg = self._map_element_tag(pu_ch_el)
            if pu_ch_el_mp_tg not in schema.children_tags.tags:
                self._logger.warning(
                    f"Unexpected {pu_ch_el_mp_tg} tag in {pu_ch_el_mp_tg} "
                    "section. Skiping"
                )
            puch_ch_prt_name = pu_ch_el_mp_tg.split(":")[-1]
            pu_ch_processor = getattr(self, f"_parse_{puch_ch_prt_name}", "")
            if not callable(pu_ch_processor):
                self._logger.warning(f"Cannot find _parse_{puch_ch_prt_name} getter.")
                continue
            try:
                raw_data[puch_ch_prt_name] = pu_ch_processor(
                    pu_ch_el, getattr(schema, puch_ch_prt_name)
                )
            except UseDetailsParsingException as err:
                self._logger.error(
                    f"Can not parse {puch_ch_prt_name} in Property Uses due to "
                    f"the error '{err}'. Skiping"
                )
        return raw_data

    def _parse_useDetails(  # pylint: disable=invalid-name
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)

        us_dt_mp_tg = self._map_element_tag(xml_el)
        if us_dt_mp_tg != schema.tag.tag:
            raise UseDetailsParsingException(
                "Expect UseDetails element {schema.tag.tag} instead {us_dt_mp_tg}"
            )
        use_details = {}
        for val_el in xml_el.getchildren():
            val_el_mp_tg = self._map_element_tag(val_el)
            if val_el_mp_tg not in schema.children_tags.tags:
                self._logger.warning(
                    f"Unexpected {val_el_mp_tg} tag in {us_dt_mp_tg} section."
                    " Skiping"
                )
                continue
            val_el_name = val_el_mp_tg.split(":")[-1]
            val_el_type = getattr(schema, val_el_name, "")
            if not val_el_type:
                self._logger.warning(f"Cannot find {val_el_name} in useDetails schema.")
                continue
            val_el_type = val_el_type.type
            getter_func = getattr(self, f"_get_{val_el_type}", "")
            if not callable(getter_func):
                self._logger.error(f"Cannot find _get_{val_el_type} getter.")
                continue
            use_details[val_el_name] = getter_func(val_el)

        return use_details

    def _parse_useTiming(  # pylint: disable=invalid-name
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)
        us_dt_mp_tg = self._map_element_tag(xml_el)
        if us_dt_mp_tg != schema.tag.tag:
            raise UseTimingParsingException(
                "Expect UseDetails element {schema.tag.tag} instead {us_dt_mp_tg}"
            )
        use_timing = {}
        for us_tm_el in xml_el.getchildren():
            us_tm_el_mp_tg = self._map_element_tag(us_tm_el)
            if us_tm_el_mp_tg not in schema.children_tags.tags:
                self._logger.warning(
                    f"Unexpected {us_tm_el_mp_tg} tag in Use Timing section. " "Skiping"
                )
                continue
            us_tm_el_name = us_tm_el_mp_tg.split(":")[-1]
            us_tm_el_type = getattr(schema, us_tm_el_name, "")
            if not us_tm_el_type:
                self._logger.warning(
                    f"Cannot find {us_tm_el_name} in useDetails schema."
                )
                continue
            us_tm_el_type = us_tm_el_type.type
            getter_func = getattr(self, f"_get_{us_tm_el_type}", "")
            if not callable(getter_func):
                self._logger.error(f"Cannot find _get_{us_tm_el_type} getter.")
                continue
            use_timing[us_tm_el_name] = getter_func(us_tm_el)
        return use_timing

    def _parse_general_info(
        self,
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
        schema: dataclass,
    ) -> dict:
        self._validate_xml_element(xml_el)

        mp_tg = self._map_element_tag(xml_el)
        if mp_tg not in self._schema.property.general_info.children_tags.tags:
            raise GeneralInfoParsingException(
                f"Unexpected {mp_tg} parameter in general info elements."
                f"Allowed elements are {self._schema.property.general_info.children_tags.tags}."
            )

        gnrl_prt_name = mp_tg.split(":")[-1]
        gnrl_prt_type = getattr(schema, gnrl_prt_name, "")
        if not gnrl_prt_type or not is_dataclass(gnrl_prt_type):
            raise GeneralInfoParsingException(
                f"Cannot find {gnrl_prt_name} in general info schema."
            )

        gnrl_prt_type = gnrl_prt_type.type
        getter_func = getattr(self, f"_get_{gnrl_prt_type}", "")
        if not callable(getter_func):
            raise GeneralInfoParsingException(
                f"Cannot find _get_{gnrl_prt_type} getter."
            )
        return {gnrl_prt_name: getter_func(xml_el)}

    def _get_property_text(
        self, xml_el: etree._Element  # pylint: disable=c-extension-no-member
    ) -> str:
        self._validate_xml_element(xml_el)
        return self._get_text(xml_el.getchildren()[0])

    # pylint: disable=c-extension-no-member
    def _get_use_timing_generic(
        self, xml_el: etree._Element, allowed_tags: dict
    ) -> dict:
        if not etree.iselement(xml_el):
            raise XmlTypeException(
                "Expected xml_el parameter is etree._Element. "
                f"Recieved {type(xml_el)}. Skiping"
            )
        chilren_tags_processed_cnt = 0
        timing = {}
        for td_ch_el in xml_el.getchildren():
            td_ch_el_mp_tg = self._map_element_tag(td_ch_el)
            if td_ch_el_mp_tg not in allowed_tags:
                self._logger.warning(
                    f"Unexpected tag {td_ch_el_mp_tg} in useTiming section"
                )
                continue
            value_name = td_ch_el_mp_tg.split(":")[-1]
            timing[value_name] = self._get_text(td_ch_el)
            chilren_tags_processed_cnt += 1
        if chilren_tags_processed_cnt != len(allowed_tags):
            self._logger.error(
                f"Processed {chilren_tags_processed_cnt} instead "
                f"{len(allowed_tags)} required tag in daily section"
            )
        return timing

    def _get_use_timing_weekly(
        self, xml_el: etree._Element
    ) -> dict:  # pylint: disable=c-extension-no-member
        return self._get_use_timing_generic(
            xml_el,
            {
                "hbd:dayOfWeek",
                "hbd:options",
            },
        )

    def _get_use_timing_daily(
        self, xml_el: etree._Element
    ) -> dict:  # pylint: disable=c-extension-no-member
        return self._get_use_timing_generic(
            xml_el,
            {
                "hbd:startTime",
                "hbd:endTime",
                "hbd:options",
            },
        )

    def _get_property_repr(
        self, xml_el: etree._Element
    ) -> str:  # pylint: disable=c-extension-no-member
        if not etree.iselement(xml_el):
            raise XmlTypeException(
                "Expected xml_el parameter is etree._Element. "
                f"Recieved {type(xml_el)}. Skiping"
            )
        return self._get_text(xml_el.getchildren()[0])

    def _get_tags(
        self, xml_el: etree._Element
    ) -> dict:  # pylint: disable=c-extension-no-member
        self._validate_xml_element(xml_el)
        return self._parse_tags(xml_el)

    def _get_audit(
        self, xml_el: etree._Element
    ) -> dict:  # pylint: disable=c-extension-no-member
        self._validate_xml_element(xml_el)
        return self._parse_audit(xml_el)

    def _get_text(
        self, xml_el: etree._Element
    ) -> str:  # pylint: disable=c-extension-no-member
        self._validate_xml_element(xml_el)
        return (xml_el.text or "").strip()

    def _get_refrigeration_units(
        self, xml_el: etree._Element
    ) -> dict:  # pylint: disable=c-extension-no-member
        self._validate_xml_element(xml_el)
        return {
            "units": xml_el.attrib.get("units", ""),
            "value": self._get_decimal(xml_el.getchildren()[0]),
        }

    def _get_floor_area(
        self, xml_el: etree._Element
    ) -> dict:  # pylint: disable=c-extension-no-member
        self._validate_xml_element(xml_el)
        return {
            "units": xml_el.attrib.get("units", ""),
            "value": self._get_decimal(xml_el.getchildren()[0]),
        }

    def _get_property_decimal(
        self, xml_el: etree._Element
    ) -> str:  # pylint: disable=c-extension-no-member
        self._validate_xml_element(xml_el)
        return self._get_decimal(xml_el.getchildren()[0])

    def _get_decimal(
        self, xml_el: etree._Element
    ) -> Decimal:  # pylint: disable=c-extension-no-member
        self._validate_xml_element(xml_el)

        try:
            result = Decimal(xml_el.text.strip())
        except InvalidOperation as err:
            print(
                f"ERROR: {self.__description__}: Can not convert "
                f"'{xml_el.text.strip()}' to Decimal due to '{err}'. Set to zero"
            )
            result = Decimal("0")

        return result

    def _get_address(self, xml_el: etree._Element) -> dict:
        self._validate_xml_element(xml_el)
        allowed_attributes = {
            "address1",
            "address2",
            "city",
            "country",
            "postalCode",
            "state",
        }

        attr = {}

        for key, value in xml_el.attrib.items():
            if key not in allowed_attributes:
                print(
                    f"WARNING: {self.__description__}: Found excessive "
                    f"arribute {key}"
                    f"in elemet '{self._map_element_tag(xml_el)}' "
                    "in property XML. Skipping"
                )
                continue
            attr[key] = value.strip()
        return attr

    def as_json(self):
        data = {
            "general_info": self._general_info,
            "property_uses": self._property_uses,
            "meter_property_association_list": self._meter_property_association,
            "additional_info": self._additional_info,
        }
        if any(x is None for x in data.values()):
            self._logger.error(
                "Property instance is not initialized Please run read_from_file "
                "or read_from_bucket before."
            )
            return {
                "general_info": None,
                "property_uses": None,
                "meter_property_association_list": None,
                "additional_info": None,
            }

        return {
            "general_info": self._factory.dump(self._general_info),
            "property_uses": self._factory.dump(self._property_uses),
            "meter_property_association_list": self._factory.dump(
                self._meter_property_association
            ),
            "additional_info": self._factory.dump(self._additional_info),
        }

    def as_dataclass(self):
        """Represent config data as dataclass"""

    def as_xml(self):
        """Represent config data as XML"""


if __name__ == "__main__":
    MAIN_LOGGER = Logger(
        name="Participant run",
        level="DEBUG",
        description="Participant RUN",
        trace_id=uuid.uuid4(),
    )

    def main():
        """Debug entrypoint"""
        # pretty_print = pprint.PrettyPrinter(indent=4)
        MAIN_LOGGER.debug(">" * 40)

        cfg = PropertyConfig()

        # cfg.read_from_file(
        #     "standardization/sample_data/real_data/config/"
        #     "participant_1_density_orion/property_1.xml",
        # )

        MAIN_LOGGER.debug(">" * 40)
        cfg.read_from_bucket(
            bucket="prototype_develop-epbp_participant_1",
            subdirectory="config/",
            filename="property_1.xml",
            binary_mode=False,
        )
        # json_cfg = cfg.as_json()
        # print("")
        # with open("/tmp/participant.json", "wb") as prtcpnt_fl:
        #     json.dump(json_cfg, prtcpnt_fl, indent=4)
        # pretty_print.pprint(json_cfg)

    main()
