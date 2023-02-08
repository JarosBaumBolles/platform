""" Generic code for xml configs """
import hashlib
import logging
import re
import uuid
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field, fields, is_dataclass
from os import path
from typing import Optional, Union

from lxml import etree

from common.bucket_helpers import file_exists, get_file_contents
from common.data_representation.config.base_exceptions import (
    ConfigException,
    XmlTypeException,
)
from common.logging import Logger
from common.settings import OMIT_VALIDATION_BUCKET_NAMES

MAX_ID_VALUE = 9223372036854775807


@dataclass
class Address:
    """Adres data"""

    address1: str = ""
    address2: str = ""
    city: str = ""
    country: str = ""
    postalCode: str = ""  # pylint: disable=invalid-name
    state: str = ""


@dataclass
class AuditData:
    """Audit dataclass"""

    createdBy: str = ""  # pylint: disable=invalid-name
    createdDate: str = ""  # pylint: disable=invalid-name


@dataclass
class TagsData:
    """Tags dataclass"""

    ids: list = field(default_factory=list)


class BaseConfig:
    """Base configuration class"""

    __metaclass__ = ABCMeta

    __description__ = "BASE XML CONFIG"

    AUDIT_TG = "hbd:audit"
    TAGS_TG = "hbd:tags"
    HAYSTACK_TG = "haystack:haystack"
    HAYSTACK_ID_TG = "haystack:id"

    def __init__(self, path_info: str = None) -> None:
        path_info = path_info or {}

        self._config_file_info = {
            "local": {
                "path": path_info.get("local", {}).get("path", ""),
                "full_path": path_info.get("local", {}).get("path", ""),
            },
            "gcp": {
                "bucket": path_info.get("gcp", {}).get("bucket", ""),
                "path": path_info.get("gcp", {}).get("path", ""),
                "filename": path_info.get("gcp", {}).get("filename", ""),
                "binary_mode": path_info.get("gcp", {}).get("binary_mode", False),
                "full_path": (
                    f"{path_info.get('gcp', {}).get('bucket', '')}/"
                    f"{path_info.get('gcp', {}).get('path', '')}/"
                    f"{path_info.get('gcp', {}).get('filename', '')}"
                ),
            },
        }

        self._trace_id = uuid.uuid4()
        self._logger = Logger(
            description=self.__description__, 
            trace_id=self._trace_id
        )

    @staticmethod
    def _get_data_fields(data_obj):
        return (
            []
            if not is_dataclass(data_obj)
            else list(map(lambda x: x.name, fields(data_obj)))
        )

    @staticmethod
    def _map_element_tag(
        element: etree._Element,  # pylint: disable=c-extension-no-member
    ) -> str:
        # pylint: disable=c-extension-no-member
        if element is None or not etree.iselement(element):
            return ""

        if not element.prefix:
            return element.tag

        return element.tag.replace(
            f"{ {element.nsmap[element.prefix]} }".replace("'", ""),
            f"{element.prefix}:",
        )

    def read_from_file(self, cfg_path: str = None) -> None:
        """Read configuration file from local machine"""
        if cfg_path and path.exists(cfg_path) and path.isfile(cfg_path):
            self._config_file_info["local"]["path"] = cfg_path
            self._config_file_info["local"]["full_path"] = cfg_path

        # local_config_file = deepcopy(self._config_file_info["local"])
        self._config_file_info["local"]["base"] = path.dirname(cfg_path)
        self._config_file_info["use"] = "local"

        fl_path = self._config_file_info["local"]["path"]

        if not fl_path or not (path.exists(fl_path) or path.isfile(fl_path)):
            raise ConfigException(
                f"The given path {fl_path} does not exist or not a file"
            )

        with open(fl_path, "r", encoding="utf-8") as cfg_file:
            data = cfg_file.read()

        self.parse_string_xml(data, self._config_file_info)

    def read_from_bucket(
        self,
        bucket: str = "",
        subdirectory: str = "",
        filename: str = "",
        binary_mode: bool = False,
    ) -> None:
        """Read configuration file from GCP bucket"""
        try:
            self._validate_bucket_name(bucket)
        except (ValueError, TypeError) as err:
            msg = str(err)
            raise ConfigException(msg)  # pylint: disable=raise-missing-from

        if bucket and subdirectory and filename:
            self._config_file_info["gcp"].update(
                {
                    "bucket": bucket,
                    "path": subdirectory.lstrip("/"),
                    "filename": filename,
                    "binary_mode": binary_mode,
                    "full_path": f"{bucket}/{subdirectory.lstrip('/')}/{filename}",
                }
            )

        self._config_file_info["use"] = "gcp"

        is_cfg_exist = file_exists(
            bucket=self._config_file_info["gcp"]["bucket"],
            subdirectory=self._config_file_info["gcp"]["path"].lstrip("/"),
            file_name=self._config_file_info["gcp"]["filename"],
        )

        if not is_cfg_exist:
            raise ConfigException(
                f"The given path \"{self._config_file_info['gcp']['bucket']}/"
                f"{self._config_file_info['gcp']['path'].lstrip('/')}/"
                f"{self._config_file_info['gcp']['filename']}\" is not exist or not a file."
            )

        data = get_file_contents(
            bucket_name=self._config_file_info["gcp"]["bucket"],
            blob_path=path.join(
                self._config_file_info["gcp"]["path"],
                self._config_file_info["gcp"]["filename"],
            ),
            binary_mode=self._config_file_info["gcp"]["binary_mode"],
        )

        self.parse_string_xml(data, self._config_file_info)

    def _parse_audit(
        self,
        audit_root_el: etree._Element,  # pylint: disable=c-extension-no-member
        allowed_fields: Optional[dict] = None,
    ) -> dict:
        allowed_fields = allowed_fields or {}
        mapped_tag = self._map_element_tag(audit_root_el)
        if mapped_tag != self.AUDIT_TG:
            return {}

        values = {}
        for ct_el in audit_root_el.getchildren():
            mpd_ct_elem_tag = self._map_element_tag(ct_el)
            field_name = mpd_ct_elem_tag.split(":")[-1]

            if allowed_fields and field_name not in allowed_fields:
                self._logger.warning(
                    f"Found excessive audit children '{mpd_ct_elem_tag}' in "
                    "participant XML. Skipping"
                )
                continue
            values[field_name] = ct_el.text
        return values

    def _parse_tags(
        self, tags_root_el: etree._Element  # pylint: disable=c-extension-no-member
    ) -> dict:
        mapped_root_el_tag = self._map_element_tag(tags_root_el)
        if mapped_root_el_tag != self.TAGS_TG:
            return {}

        ids = set()
        for tg_el in tags_root_el.getchildren():
            tg_mapped_tag = self._map_element_tag(tg_el)
            if tg_mapped_tag == self.HAYSTACK_TG:
                for hstk_el in tg_el.getchildren():
                    hstk_mpd_tag = self._map_element_tag(hstk_el)
                    if hstk_mpd_tag == self.HAYSTACK_ID_TG:
                        ids.add(hstk_el.text)

        return {"ids": list(ids)}

    def get_unique_id(self, value: str) -> int:
        """Generate unique id based on meter config path

        Args:
            value (str): string value

        Returns:
            int: ID
        """

        # Known pylint issue https://github.com/PyCQA/pylint/issues/4039
        value = hashlib.shake_256(  # pylint: disable=too-many-function-args
            value.encode("utf-8")
        ).hexdigest(6)
        val_uuid = int(value, 16)
        if val_uuid > MAX_ID_VALUE:
            self._logger.warning(
                f"Generated meter id '{val_uuid}' more than allowed value "
                f"{MAX_ID_VALUE}. Meter config is located at {val_uuid}"
            )
            val_uuid = val_uuid & MAX_ID_VALUE
        return val_uuid

    @staticmethod
    def _validate_xml_element(
        xml_el: etree._Element,  # pylint: disable=c-extension-no-member
    ) -> None:
        if not etree.iselement(xml_el):  # pylint: disable=c-extension-no-member
            raise XmlTypeException(
                "Expected xml_el parameter is etree._Element. "
                f"Recieved {type(xml_el)}. Skiping"
            )

    @staticmethod
    def _validate_bucket_name(name: str) -> None:
        if not isinstance(name, str):
            raise TypeError("Bucket names must be a string.")

        if len(name.strip()) < 3:
            raise ValueError("Bucket names must be between 3 and 63 characters.")

        if (
            not re.match(r"^[a-z\d][a-z\d_-]+_[\d]{1,4}$", name)
            and name not in OMIT_VALIDATION_BUCKET_NAMES
        ):
            raise ValueError(
                "The bucket names must meet the following requirements:\n"
                "Bucket names can only contain lowercase letters, numeric characters, "
                "dashes (-), underscores (_). Spaces are not allowed.\n"
                "Bucket names must start and end with a number or letter.\n"
                "Bucket names must contain 3-63 characters. Names containing dots can "
                "contain up to 222 characters, but each dot-separated component can "
                "be no longer than 63 characters.\n"
            )

    @abstractmethod
    def parse_string_xml(self, data: str, config: dict):
        """Parse xml from string"""

    @abstractmethod
    def as_json(self):
        """Represent config data as json"""

    @abstractmethod
    def as_dataclass(self):
        """Represent config data as dataclass"""

    @abstractmethod
    def as_xml(self):
        """Represent config data as XML"""
