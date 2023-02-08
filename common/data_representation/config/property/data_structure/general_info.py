"""Property General Info Data structures"""
from dataclasses import dataclass
from decimal import Decimal

from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
)


@dataclass
class PropertyAddress:
    """Property address data structure"""

    address1: str = ""
    address2: str = ""
    city: str = ""
    country: str = ""
    postalCode: str = ""  # pylint: disable=invalid-name
    state: str = ""


# pylint: disable=too-many-instance-attributes
@dataclass
class PropertyGeneralInfo:
    """Property general information"""

    name: str = ""
    primaryFunction: str = ""  # pylint: disable=invalid-name
    grossFloorArea: GrossFloorAreaData = (  # pylint: disable=invalid-name
        GrossFloorAreaData()
    )
    yearBuilt: Decimal = Decimal("0")  # pylint: disable=invalid-name
    address: PropertyAddress = PropertyAddress()
    numberOfBuildings: Decimal = Decimal("0")  # pylint: disable=invalid-name
    isFederalProperty: str = ""  # pylint: disable=invalid-name
    occupancyPercentage: Decimal = Decimal("0")  # pylint: disable=invalid-name
    propertyURI: str = ""  # pylint: disable=invalid-name
    property_id: int = 0
    audit: str = ""
    datacenterOver75kW: str = ""  # pylint: disable=invalid-name
