
"""Db Load Structures of Integration."""
from dataclasses import dataclass, field
from typing import List, Optional

from integration.base_integration import StorageInfo


# TODO: Shoud be moved to base integration 
@dataclass
class FileIno:
    bucket: str = ""
    path: str = ""
    filename: str = ""