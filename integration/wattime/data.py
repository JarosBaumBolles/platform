""" Data Structures module"""
from dataclasses import dataclass, field
from typing import Any
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