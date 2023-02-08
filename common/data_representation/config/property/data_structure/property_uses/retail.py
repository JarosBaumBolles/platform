from dataclasses import dataclass
from decimal import Decimal
from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
    UseTiming,
)
from typing import Optional

@dataclass        
class RetailUseDetails:
    totalGrossFloorArea: GrossFloorAreaData = GrossFloorAreaData()  # pylint: disable=invalid-name
    weeklyOperatingHours: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWorkers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfComputers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfCashRegisters: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWalkInRefrigerationUnits: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfOpenClosedRefrigerationUnits: Decimal = Decimal("0")  # pylint: disable=invalid-name
    percentCooled: str = ""  # pylint: disable=invalid-name
    percentHeated: str = ""  # pylint: disable=invalid-name
    singleStore: str = ""  # pylint: disable=invalid-name
    exteriorEntranceToThePublic: str = ""  # pylint: disable=invalid-name
    areaOfAllWalkInRefrigerationUnits: GrossFloorAreaData = GrossFloorAreaData()  # pylint: disable=invalid-name
    lengthOfAllOpenClosedRefrigerationUnits: GrossFloorAreaData = GrossFloorAreaData()  # pylint: disable=invalid-name
    cookingFacilities: str = ""

@dataclass
class RetailPropertyUse:
    useDetails: RetailUseDetails  # pylint: disable=invalid-name
    useTiming: Optional[UseTiming] = None  # pylint: disable=invalid-name
