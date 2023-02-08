"""Meter Proprty Data Structure"""
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from common.data_representation.config.property.data_structure.general import (
    AuditData,
    TagsData,
)


@dataclass
class MeterPropertyAssociationData:
    """Property data structure"""

    meterURI: str = ""  # pylint: disable=invalid-name
    externalWeight: Decimal = Decimal("1")  # pylint: disable=invalid-name
    weight: Decimal = Decimal("1")
    propertyRepresentation: str = ""  # pylint: disable=invalid-name
    tags: TagsData = TagsData()
    audit: AuditData = AuditData()


# pylint: disable=too-many-instance-attributes
@dataclass
class MeterPropertyAssociationList:
    """Meter Association List Data Structure"""

    energyMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    occupancyMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    averageEmissionsMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    marginalEmissionsMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    ambientTemperatureMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    ambientWindSpeedMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    ambientWindDirectionMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    ambientHumidityMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    ambientCloudCoverMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
    ambientDewPointMeterAssociation: Optional[  # pylint: disable=invalid-name
        List[MeterPropertyAssociationData]
    ] = None
