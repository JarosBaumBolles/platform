"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pendulum import DateTime

from integration.base_integration import MeterCfg
from queue import Queue
from common.data_representation.standardized import Meter
from pathlib import Path

@dataclass
class RawFetchFile:
    """Fetch request hour data"""

    file_name: str = ""
    file_path: Path = ""
    mtr_cfgs: Queue = field(default_factory=Queue)

@dataclass
class StandardizedFile:
    """Data file structure"""
    file_name: str = ""
    path: Path = ""
    bucket: str = ""
    body: str = ""
    meter: Meter = field(default_factory=lambda: Meter())
    cfg: Any = ""    

@dataclass
class DataFile:
    """Data file structure"""

    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""
    cfg: Any = ""    