"""Nantum configuration data structure."""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union

from integration.base_integration import ExtraInfo, MeterCfg


@dataclass
class NantumCfg:  # pylint:disable=too-many-instance-attributes
    """Nantum config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    application_id: str = ""
    client_key: str = ""
    client_secret: str = ""
    # base_url: str = ""
    metric: str = ""
    site: str = ""
    company_id: str = ""
    company_name: str = ""
    sensor_id: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: Optional[Union[str, Dict[str, Any]]] = (
        "{\"shift\": \"None\", \"shift_hours\": {\"years\": 0, \"months\": 0, "
        "\"weeks\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, "
        "\"microseconds\": 0}}"
    )