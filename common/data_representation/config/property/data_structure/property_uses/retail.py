"""Retail Properties"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
    UseTiming,
)


@dataclass
class RetailUseDetails:
    """Retaill data"""

    totalGrossFloorArea: GrossFloorAreaData = field(  # pylint: disable=invalid-name
        default_factory=GrossFloorAreaData
    )
    weeklyOperatingHours: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWorkers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfComputers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfCashRegisters: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWalkInRefrigerationUnits: Decimal = Decimal(  # pylint: disable=invalid-name
        "0"
    )
    numberOfOpenClosedRefrigerationUnits: Decimal = Decimal(    # pylint: disable=invalid-name
        "0"
    )
    percentCooled: str = ""  # pylint: disable=invalid-name
    percentHeated: str = ""  # pylint: disable=invalid-name
    singleStore: str = ""  # pylint: disable=invalid-name
    exteriorEntranceToThePublic: str = ""  # pylint: disable=invalid-name
    areaOfAllWalkInRefrigerationUnits: GrossFloorAreaData = field(  # pylint: disable=invalid-name
        default_factory=GrossFloorAreaData
    )
    lengthOfAllOpenClosedRefrigerationUnits: GrossFloorAreaData = field(    # pylint: disable=invalid-name
        default_factory=GrossFloorAreaData
    )
    cookingFacilities: str = "" # pylint: disable=invalid-name


@dataclass
class RetailPropertyUse:
    """Retail Property"""

    useDetails: RetailUseDetails  # pylint: disable=invalid-name
    useTiming: Optional[UseTiming] = None  # pylint: disable=invalid-name
