"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import List

from integration.base_integration import ExtraInfo, MeterCfg, TimeShift


@dataclass
class OpenWeatherCfg:  # pylint:disable=too-many-instance-attributes
    """OpenWeather config"""

    meters: List[MeterCfg] = field(default_factory=list)
    extra: ExtraInfo = field(default_factory=ExtraInfo)
    app_id: str = ""
    city_environment: str = ""
    city_name: str = ""
    city_coordinates_latitude: str = ""
    city_coordinates_longitude: str = ""
    gap_regeneration_window: int = -1
    timestamp_shift: TimeShift = field(default_factory=TimeShift)
