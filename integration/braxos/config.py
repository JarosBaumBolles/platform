"""Braxos Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List, Optional, Union

from integration.base_integration import StorageInfo
from paramiko.rsakey import RSAKey

# TODO: Should be moved to base config in future
@dataclass
class FetchStrategy:
    """Fetch starategy"""

    type: str = ""
    description: str = ""


# TODO: Should be moved to base config in future
@dataclass
class ExtraInfo:
    """Extra config parameters"""

    raw: StorageInfo = field(default_factory=StorageInfo)
    participant_id: int = -1
    timezone: str = ""
    fetch_strategy: FetchStrategy = field(default_factory=FetchStrategy)


# TODO: Should be moved to base config in future
@dataclass
class MeterCfg:
    """Meter config representation"""

    meter_name: str = ""
    meter_id: int = -1
    meter_uri: str = ""
    type: str = ""
    standardized: StorageInfo = field(default_factory=StorageInfo)


@dataclass
class BraxosCfg:  # pylint:disable=too-many-instance-attributes
    """Wattime config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    app_id: str = ""
    private_key: str = ""
    key: Union[str, RSAKey] = ""
    server: str = ""
    username: str = ""
    gap_regeneration_window: int = -1


@dataclass
class FetchPayload:
    """Fetch request hour data"""

    meter_cfg: Optional[MeterCfg] = None
    file_name: Optional[str] = ""
    start_date: Optional[str] = ""
    end_date: Optional[str] = ""
