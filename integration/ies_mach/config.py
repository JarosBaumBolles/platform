"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List

from integration.base_integration import ExtraInfo, MeterCfg, TimeShift


@dataclass
class IesMachCfg:  # pylint:disable=too-many-instance-attributes
    """IesMach config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    datapoint_id: str = ""
    auth_token: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: TimeShift = field(default_factory=TimeShift)
