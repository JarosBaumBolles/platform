"""Supermarket Data Structures"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
    UseTiming,
)


# pylint: disable=too-many-instance-attributes
@dataclass
class SupermarketUseDetails:
    """Supermarket DAta Structure"""

    totalGrossFloorArea: GrossFloorAreaData = (  # pylint: disable=invalid-name
        GrossFloorAreaData()
    )
    weeklyOperatingHours: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWorkers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfComputers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfCashRegisters: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWalkInRefrigerationUnits: Decimal = Decimal(  # pylint: disable=invalid-name
        "0"
    )
    numberOfOpenClosedRefrigerationUnits: Decimal = (  # pylint: disable=invalid-name
        Decimal("0")
    )
    percentCooled: str = ""  # pylint: disable=invalid-name
    percentHeated: str = ""  # pylint: disable=invalid-name
    singleStore: str = ""  # pylint: disable=invalid-name
    exteriorEntranceToThePublic: str = ""  # pylint: disable=invalid-name
    areaOfAllWalkInRefrigerationUnits: GrossFloorAreaData = (  # pylint: disable=invalid-name
        GrossFloorAreaData()
    )
    lengthOfAllOpenClosedRefrigerationUnits: GrossFloorAreaData = (  # pylint: disable=invalid-name
        GrossFloorAreaData()
    )
    cookingFacilities: str = ""  # pylint: disable=invalid-name


@dataclass
class SupermarketPropertyUse:
    """Supermarket Data Structure"""

    useDetails: SupermarketUseDetails  # pylint: disable=invalid-name
    useTiming: Optional[UseTiming] = None  # pylint: disable=invalid-name
