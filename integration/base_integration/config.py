""" JBB integrations common configuration data structures"""
# pylint: disable=logging-fstring-interpolation
from dataclasses import dataclass, field


@dataclass
class FetchStrategy:
    """Fetch starategy"""

    type: str = ""
    description: str = ""


@dataclass
class StorageInfo:
    """Storage Info"""

    bucket: str = ""
    path: str = ""


@dataclass
class MeterCfg:
    """Meter config representation"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class ExtraInfo:
    """Extra config parameters"""

    participant_id: int = -1
    timezone: str = ""
    fetch_strategy: FetchStrategy = field(default_factory=FetchStrategy)
    raw: StorageInfo = field(default_factory=StorageInfo)
