"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List

from integration.base_integration import ExtraInfo, MeterCfg, TimeShift


@dataclass
class WillowCfg:  # pylint:disable=too-many-instance-attributes
    """Wattime config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    client_id: str = ""
    client_secret: str = ""
    site_id: str = ""
    point_id: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: TimeShift = field(default_factory=TimeShift)
