"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List, Optional

from integration.base_integration import StorageInfo


@dataclass
class FetchStrategy:
    """Fetch starategy"""

    type: str = ""
    description: str = ""


@dataclass
class ExtraInfo:
    """Extra config parameters"""

    raw: StorageInfo = field(default_factory=StorageInfo)
    participant_id: int = -1
    timezone: str = ""
    fetch_strategy: FetchStrategy = field(default_factory=FetchStrategy)


@dataclass
class MeterCfg:
    """Meter config representation"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class WattimeCfg:  # pylint:disable=too-many-instance-attributes
    """Wattime config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    username: str = ""
    password: str = ""
    grid_regions_environment: str = ""
    grid_regions_name: str = ""
    gap_regeneration_window: int = -1


@dataclass
class FetchPayload:
    """Fetch request hour data"""

    meter_cfg: Optional[MeterCfg] = None
    file_name: Optional[str] = ""
    start_date: Optional[str] = ""
    end_date: Optional[str] = ""
