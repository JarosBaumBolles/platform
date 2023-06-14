""" Additional dat structures"""
from dataclasses import dataclass, field

from common.data_representation.config.property.data_structure.general import (
    AuditData,
    TagsData,
)


@dataclass
class AdditionalInfo:
    """Additional Info dataclass"""

    tags: TagsData = field(default_factory=TagsData)
    audit: AuditData = field(default_factory=AuditData)
