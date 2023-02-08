import sys
from decimal import Decimal

import pendulum as pdl
from common import settings as CFG

if sys.version_info < (3, 8):  # pragma: no cover
    from typing_extensions import Final
else:
    from typing import Final  # pragma: no cover

SIXTY: Final[Decimal] = Decimal("60")
HOUR_SECONDS: Final[Decimal] = SIXTY * SIXTY

DEFAULT_LOCAL_TIMEZONE = pdl.timezone(CFG.DEFAULT_LOCAL_TIMEZONE_NAME)
