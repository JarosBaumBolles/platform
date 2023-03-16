"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List

from integration.base_integration import ExtraInfo, MeterCfg, TimeShift


@dataclass
class DensityCfg:  # pylint:disable=too-many-instance-attributes
    """Wattime config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    space_id: str = ""
    token: str = ""
    floor: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: TimeShift = field(default_factory=TimeShift)
