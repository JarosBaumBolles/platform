"""Configuration of Integration."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pendulum import DateTime

from integration.nantum.config import MeterCfg
from queue import Queue
from common.data_representation.standardized import Meter

@dataclass
class DataFile:
    """Data file structure"""
    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""
    cfg: Any = ""    
    meters: Queue = field(default_factory=Queue)
    timestamps: Queue = field(default_factory=Queue)


@dataclass
class StandardizedFile:
    """Data file structure"""
    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""
    meter: Meter = field(default_factory=lambda: Meter())
    cfg: Any = ""


@dataclass
class MoveFile:
    """Data file structure"""
    filename: str = ""
    bucket: str = ""
    path: str = ""
    destination_filename: str = ""
    destination_bucket: str = ""
    destination_path: str = ""    
    