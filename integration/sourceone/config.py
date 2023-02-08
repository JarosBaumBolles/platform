"""Nantum configuration data structure."""
from dataclasses import dataclass, field
from typing import List

from integration.base_integration import ExtraInfo, MeterCfg


@dataclass
class SourceoneCfg:  # pylint:disable=too-many-instance-attributes
    """sourceone config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    email: str = ""
    password: str = ""
    subject: str = ""
    host: str = ""
    sender: str = ""
    attachment: str = ""
    gap_regeneration_window: int = 2000
