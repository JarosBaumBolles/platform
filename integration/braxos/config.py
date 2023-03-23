"""Braxos Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List, Union

from paramiko.rsakey import RSAKey

from integration.base_integration import ExtraInfo, MeterCfg, TimeShift


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
    timestamp_shift: TimeShift = field(default_factory=TimeShift)
