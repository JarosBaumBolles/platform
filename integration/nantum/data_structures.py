"""Configuration of Integration."""
from dataclasses import dataclass, field
from queue import Queue
from typing import Any, List, Optional
from common.data_representation.standardized import Meter


@dataclass
class NantumResponseReadingsPoint:
    """Nantum readings"""

    time: str = ""
    value: int = 0


@dataclass
class NantumResponseCalculate:
    """Nantum response calculate structure"""

    sensors: List[str] = field(default_factory=list)
    source: str = ""
    type: str = ""


@dataclass
class NantumResponseDoc:
    """Response doc structure"""

    _id: str = ""
    benchmark: bool = False
    building: str = ""
    building_dis: str = ""
    cid_a1: List = field(default_factory=list)
    cid_a2: List = field(default_factory=list)
    cid_a3: List = field(default_factory=list)
    cid_s1: str = ""
    color: int = 0
    company: str = ""
    cov: bool = False
    date: str = ""
    description: str = ""
    device_id: str = ""
    dis: str = ""
    hidden: bool = False
    identifier1: str = ""
    identifier2: str = ""
    identifier3: str = ""
    last_time: str = ""
    last_value: int = 100
    max_val: int = (0,)
    min_val: int = 0
    multiplier: int = 1
    name: str = ("",)
    prediction: bool = False
    calculate: Optional[NantumResponseCalculate] = None
    readings: List[NantumResponseReadingsPoint] = field(default_factory=list)


@dataclass
class NantumResponseStats:
    """Nantum response stats"""

    cursor: int = -1
    query_total: int = -1


@dataclass
class NantumResponse:
    """Nantum Response"""

    docs: List[NantumResponseDoc] = field(default_factory=list)
    stats: Optional[NantumResponseStats] = None


@dataclass
class DataFile:
    """Data file structure"""

    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""
    cfg: Any = ""


@dataclass
class RawFetchFile:
    """Data file structure"""

    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""


@dataclass
class DocFetchFile:
    """Nantum Fetch doc file. Used for standardization."""

    doc: NantumResponseDoc = field(default_factory=NantumResponseDoc)
    meters: Queue = field(default_factory=Queue)
    timestamps: Queue = field(default_factory=Queue)


@dataclass
class StandardizedFile:
    """Data file structure"""

    file_name: str = ""
    bucket: str = ""
    path: str = ""
    body: str = ""
    meter: Meter = field(default_factory=Meter)
    cfg: Any = ""
