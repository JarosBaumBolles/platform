""" K12 School data stuctures"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
    UseTiming,
)


# pylint: disable=too-many-instance-attributes
@dataclass
class K12SchoolUseDetails:
    """K12 Scholl Property datastructure"""

    totalGrossFloorArea: GrossFloorAreaData = field(    # pylint: disable=invalid-name
        default_factory=GrossFloorAreaData
    )
    openOnWeekends: str = ""  # pylint: disable=invalid-name
    numberOfWalkInRefrigerationUnits: Decimal = Decimal(  # pylint: disable=invalid-name
        "0"
    )
    percentCooled: str = ""  # pylint: disable=invalid-name
    percentHeated: str = ""  # pylint: disable=invalid-name
    numberOfComputers: str = ""  # pylint: disable=invalid-name
    cookingFacilities: str = ""  # pylint: disable=invalid-name
    isHighSchool: str = ""  # pylint: disable=invalid-name
    monthsInUse: str = ""  # pylint: disable=invalid-name
    schoolDistrict: str = ""  # pylint: disable=invalid-name
    studentSeatingCapacity: Decimal = Decimal("0")  # pylint: disable=invalid-name
    numberOfWorkers: Decimal = Decimal("0")  # pylint: disable=invalid-name
    gymnasiumFloorArea: GrossFloorAreaData = field( # pylint: disable=invalid-name
        default_factory=GrossFloorAreaData
    )
    grossFloorAreaUsedForFoodPreparation: Decimal = Decimal(    # pylint: disable=invalid-name
        "0"
    )  # pylint: disable=invalid-name


@dataclass
class K12SchoolPropertyUse:
    """K12 Scholl Property datastructure"""

    useDetails: K12SchoolUseDetails  # pylint: disable=invalid-name
    useTiming: Optional[UseTiming] = None  # pylint: disable=invalid-name
