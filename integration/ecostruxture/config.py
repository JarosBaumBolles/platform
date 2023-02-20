"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from integration.base_integration import ExtraInfo, MeterCfg, TimeShift


@dataclass
class EcoStruxtureCfg:  # pylint:disable=too-many-instance-attributes
    """Ecostruxture config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    email: str = ""
    password: str = ""
    subject: str = ""
    attachment: str = ""
    host: str = ""
    sender: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: TimeShift = field(default_factory=TimeShift)
    meters_sheet_mapper: Optional[Union[str, Dict[str, any]]] = "{}"
