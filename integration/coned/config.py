"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union

from integration.base_integration import StorageInfo


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
class ConedCfg:  # pylint:disable=too-many-instance-attributes
    """Coned config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    refresh_token: str = ""
    client_id: str = ""
    client_secret: str = ""
    subscription_id: str = ""
    usage_point_id: str = ""
    subscription_key: str = ""
    meter_reading_id: str = ""
    interval: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: Optional[Union[str, Dict[str, Any]]] = (
        "{\"shift\": \"None\", \"shift_hours\": {\"years\": 0, \"months\": 0, "
        "\"weeks\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, "
        "\"microseconds\": 0}}"
    )

@dataclass
class FetchPayload:
    """Fetch request hour data"""

    meter_cfg: Optional[MeterCfg] = None
    file_name: Optional[str] = ""
    start_date: Optional[str] = ""
    end_date: Optional[str] = ""
