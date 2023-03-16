"""Nantum configuration data structure."""
from dataclasses import dataclass, field
from typing import List
from integration.base_integration import ExtraInfo, MeterCfg, TimeShift


@dataclass
class NantumCfg:  # pylint:disable=too-many-instance-attributes
    """Nantum config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    application_id: str = ""
    client_key: str = ""
    client_secret: str = ""
    metric: str = ""
    site: str = ""
    company_id: str = ""
    company_name: str = ""
    sensor_id: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: TimeShift = field(default_factory=TimeShift)
