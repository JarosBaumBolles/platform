""" JBB integrations common configuration data structures"""
# pylint: disable=logging-fstring-interpolation
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StorageInfo:
    """Storage Info"""

    bucket: str = ""
    path: str = ""


@dataclass
class FetchStrategy:
    """Fetch starategy"""

    type: str = ""
    description: str = ""


@dataclass
class ExtraInfo:
    """Extra config parameters"""

    participant_id: int = -1
    timezone: str = ""
    fetch_strategy: FetchStrategy = field(default_factory=FetchStrategy)
    raw: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class MeterCfg:
    """Meter config representation"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class ShiftHours:
    """Shift hours"""
    years: int = 0
    months: int = 0
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0
    microseconds: int = 0


@dataclass
class TimeShift:
    """Time shift"""
    shift: Optional[str] = ""
    force: bool = False
    shift_hours: ShiftHours = field(default_factory=ShiftHours)
