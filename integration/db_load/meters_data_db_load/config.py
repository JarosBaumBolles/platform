"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List, Optional

from integration.base_integration import StorageInfo


# TODO: Should be moved to base config in future
@dataclass
class ExtraInfo:
    """Extra config parameters"""
    participant_id: int = -1

@dataclass
class MetersExtra:
    timezone: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)

@dataclass
class MeterCfg:
    """Meter config representation"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    extra: MetersExtra = field(default_factory=MetersExtra)


@dataclass
class DbLoadCfg:  # pylint:disable=too-many-instance-attributes
    """Wattime config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
