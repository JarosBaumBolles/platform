"""Property use data structures"""
from dataclasses import dataclass
from typing import List, Optional

from common.data_representation.config.property.data_structure.property_uses import (
    DatacenterPropertyUse,
    K12SchoolPropertyUse,
    OfficePropertyUse,
    ParkingPropertyUse,
    RetailPropertyUse,
    SupermarketPropertyUse,
)


@dataclass
class PropertyUse:
    """Property use data structure"""

    office: Optional[List[OfficePropertyUse]] = None  # pylint: disable=invalid-name
    parking: Optional[List[ParkingPropertyUse]] = None  # pylint: disable=invalid-name
    supermarket: Optional[
        List[SupermarketPropertyUse]
    ] = None  # pylint: disable=invalid-name
    retail: Optional[List[RetailPropertyUse]] = None  # pylint: disable=invalid-name
    datacenter: Optional[
        List[DatacenterPropertyUse]
    ] = None  # pylint: disable=invalid-name
    k12School: Optional[  # pylint: disable=invalid-name
        List[K12SchoolPropertyUse]
    ] = None
