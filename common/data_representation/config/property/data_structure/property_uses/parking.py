"""Parking Data Structures"""
from dataclasses import dataclass
from typing import Optional

from common.data_representation.config.property.data_structure.general import (
    GrossFloorAreaData,
    UseTiming,
)


@dataclass
class ParkingUseDetails:
    """Parking Use Details structure"""
    supplementalHeating: str = ""  # pylint: disable=invalid-name
    openFootage: GrossFloorAreaData = (  # pylint: disable=invalid-name
        GrossFloorAreaData()
    )
    completelyEnclosedFootage: GrossFloorAreaData = (  # pylint: disable=invalid-name
        GrossFloorAreaData()
    )
    partiallyEnclosedFootage: GrossFloorAreaData = (  # pylint: disable=invalid-name
        GrossFloorAreaData()
    )


@dataclass
class ParkingPropertyUse:
    """Parking Property Use Data Structure"""
    useDetails: ParkingUseDetails  # pylint: disable=invalid-name
    useTiming: Optional[UseTiming] = None  # pylint: disable=invalid-name
