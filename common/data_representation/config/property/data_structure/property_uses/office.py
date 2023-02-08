"""Office Property Data Structures"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
    UseTiming,
)


@dataclass
class OfficeUseDetails:
    """Office Use Details structures"""
    totalGrossFloorArea: GrossFloorAreaData  # pylint: disable=invalid-name
    weeklyOperatingHours: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWorkers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfComputers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    percentOfficeCooled: str = ""  # pylint: disable=invalid-name
    percentOfficeHeated: str = ""  # pylint: disable=invalid-name


@dataclass
class OfficePropertyUse:
    """Office Property USe Structure"""
    useDetails: OfficeUseDetails  # pylint: disable=invalid-name
    useTiming: Optional[UseTiming] = None  # pylint: disable=invalid-name
