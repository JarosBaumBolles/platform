"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import Any, List, Optional

from pendulum import DateTime

from integration.base_integration import MeterCfg


@dataclass
class FetchPayload:
    """Fetch request hour data"""

    file_name: str = ""
    meter_cfg: Optional[MeterCfg] = None
    meters_hours: Optional[List[DateTime]] = None


@dataclass
class DataFile:
    """Data file structure"""

    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""
    cfg: Any = ""
    missed_hours: List[DateTime] = field(default_factory=list)
