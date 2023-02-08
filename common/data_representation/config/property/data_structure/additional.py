""" Additional dat structures"""
from dataclasses import dataclass

from common.data_representation.config.property.data_structure.general import (
    AuditData,
    TagsData,
)


@dataclass
class AdditionalInfo:
    """ Additional Info dataclass"""
    tags: TagsData = TagsData()
    audit: AuditData = AuditData()
