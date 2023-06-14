"""Data Centr structures"""
from dataclasses import dataclass, field
from typing import Optional

from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
    UseTiming,
)


@dataclass
class DatacenterUseDetails:
    """Data Centr use data structure"""

    totalGrossFloorArea: GrossFloorAreaData = field(  # pylint: disable=invalid-name
        default_factory=GrossFloorAreaData
    )
    estimatesApplied: str = ""  # pylint: disable=invalid-name
    coolingEquipmentRedundancy: str = ""  # pylint: disable=invalid-name
    itEnergyMeterConfiguration: str = ""  # pylint: disable=invalid-name
    upsSystemRedundancy: str = ""  # pylint: disable=invalid-name


@dataclass
class DatacenterPropertyUse:
    """Data Center Property structure"""

    useDetails: DatacenterUseDetails = field(  # pylint: disable=invalid-name
        default_factory=DatacenterUseDetails
    )
    useTiming: Optional[UseTiming] = None  # pylint: disable=invalid-name
