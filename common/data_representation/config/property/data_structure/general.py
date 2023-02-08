"""General Data Structures"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


@dataclass
class GrossFloorAreaData:
    """grossFloorArea information"""

    units: str = ""
    value: Decimal = Decimal("0")


@dataclass
class UseTimingDaily:
    """Use Daily timing structure"""

    startTime: str = ""  # pylint: disable=invalid-name
    endTime: str = ""  # pylint: disable=invalid-name
    options: str = ""  # pylint: disable=invalid-name


@dataclass
class UseTimingWeekly:
    """Use Weekly timing structure"""

    dayOfWeek: str = ""  # pylint: disable=invalid-name
    options: str = ""


@dataclass
class UseTiming:
    """Use Timing structure"""

    daily: Optional[UseTimingDaily] = None  # pylint: disable=invalid-name
    weekly: Optional[UseTimingWeekly] = None  # pylint: disable=invalid-name


@dataclass
class TagsData:
    """Tags data structure"""

    ids: list = field(default_factory=list)


@dataclass
class AuditData:
    """Audit structure"""

    createdBy: str = ""  # pylint: disable=invalid-name
    createdDate: str = ""  # pylint: disable=invalid-name
