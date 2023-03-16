""" Settings for service """

import os
import sys
import tempfile
from enum import Enum
from pathlib import Path

from environs import Env
from tzlocal import get_localzone
import pendulum as pdl

if sys.version_info < (3, 8):  # pragma: no cover
    from typing_extensions import Final
else:
    from typing import Final  # pragma: no cover


class Integration(Enum):
    """All available integrations we use"""

    OPENWEATHER = "openweather"
    WATTIME_MARGINAL = "wattime_marginal"
    WATTIME_AVERAGE = "wattime_average"
    ECOSTRUXTURE = "ecostruxture"
    NANTUM = "nantum"
    ORION = "orion"
    WILLOW = "willow"
    DENSITY = "density"
    DBLOAD = "dbload"
    DWUPDATE = "dw_update"
    BRAXOS = "braxos"
    IRISYS = "irisys"
    SOURCEONE = "sourceone"
    CONED = "coned"
    IES_MACH = "ies_mach"
    XLSX = "xlsx"
    GENERIC = "generic"  # TODO: remove this later


ROOT_DIR = Path(os.curdir).resolve()
env = Env(expand_vars=True)
env.read_env(ROOT_DIR.joinpath(".env"))

PARTICIPANT_ID = env.int("PARTICIPANT", 0)
PARTICIPANT = f"participant{PARTICIPANT_ID}"


GCP_REGION = env.str("GCP_REGION", "us-east4")
GCP_BIGQUERY_REGION = env.str("GCP_BIGQUERY_REGION", "us")
PROJECT = env.str("PROJECT", "develop-epbp")
DATASET = env.str("DATASET", "standardized_new")

# The prototype preffix must be removed after transition
if PROJECT == "develop-epbp":
    BUCKET_PREFFIX = f"prototype_{PROJECT}_participant_"
elif PROJECT == "production-epbp":
    BUCKET_PREFFIX = f"{PROJECT}_participant_"
else:
    raise RuntimeError("Unknown environment provided")

WEB_PORTAL_BUCKET = env.str("WEB_PORTAL_BUCKET", "web-portal-static-content")

DEBUG = env.bool("DEBUG", False)
DEBUG_PARTICIPANTS = list(
    filter(lambda x: x.isdecimal(), env.list("DEBUG_PARTICIPANTS", []))
)
DEBUG_BUCKETS = list(map(lambda x: f"{BUCKET_PREFFIX}{x}", DEBUG_PARTICIPANTS))
DEBUG_PORT = env.int("DEBUG_PORT", 5678)
LOCAL_RUN = env.bool("LOCAL_RUN", False)
LOCAL_PATH = Path(env.str("LOCAL_PATH", tempfile.gettempdir()))
LOCAL_AUTH_SECRET_PATH = str(LOCAL_PATH.joinpath("bq_secret.json"))

LANDING_ZONE_BUCKET = env.str("LANDING_ZONE_BUCKET", "hourlybuildingdata_landing_zone")

LOGGER_NAME = env.str("LOGGER_NAME", "jbb_logger")
LOCAL_LOGGER_FORMAT = env.str("LOCAL_LOGGER_FORMAT", " %(message)s")
LOGGER_LEVEL = env.str("LOGGER_LEVEL", "DEBUG")
# to the new architechture
BUCKET = f"{BUCKET_PREFFIX}{PARTICIPANT_ID}" if PROJECT else None

CONFIG_BASE_PATH = "config"
PARTICIPANT_CONFIG_NAME = "participant.xml"

DB_LOAD_FUNCTION_NAME = "connector_dbload"
DW_UPDATE_FUNCTION_NAME = "connector_dw_update"

OMIT_VALIDATION_BUCKET_NAMES = [LANDING_ZONE_BUCKET]

DB_LOAD_RESTORE_FUNCTION_NAME_TEMPLATE = "connector_dbload_restore_"

DB_LOAD_STANDARDIZE_PROCESSED_FOLDER = env.str(
    "DB_LOAD_STANDARDIZE_PROCESSED_FOLDER", "processed"
)

STANDARDIZED_METER_DATE_FORMAT = env.str(
    "STANDARDIZED_METER_DATE_FORMAT", "YYYY-MM-DD[T]HH:mm:ss"
)

PROCESSING_DATE_FORMAT = env.str("PROCESSING_DATE_FORMAT", "YYYY-MM-DD[T]HH:mm:ss")

FACIT_PROCESSING_DATE_FORMAT = env.str(
    "FACIT_PROCESSING_DATE_FORMAT", "YYYY-MM-DD[T]HH:mm:ss.SSS"
)

HOUR_ID_DATE_FORMAT = env.str("HOUR_ID_DATE_FORMAT", "YYYYMMDDHH")

DEFAULT_LOCAL_TIMEZONE_NAME: Final[str] = get_localzone().zone
ENVIRONMENT_TIME_ZONE = env.str("ENVIRONMENT_TIME_ZONE", DEFAULT_LOCAL_TIMEZONE_NAME)

DW_LOAD_FILES_BUCKET_LIMIT = os.environ.get("DW_LOAD_FILES_BUCKET_LIMIT", 10000)

UPDATE_PREFIX = "updates"
UPDATE_FILENAME_PREFFIX_TMPL = "{update_prefix}-"
UPDATE_FILENAME_TMPL = "{update_prefix}-{cnt}-{run_date}"
PROCESSED_PREFIX = "processed"

UTC_TIMEZONE = pdl.timezone("UTC")

CONED_CLIENT_ID = os.environ.get("CONED_CLIENT_ID")
CONED_CLIENT_SECRET =  os.environ.get("CONED_CLIENT_SECRET")
CONED_SUBSCRIPTION_KEY = os.environ.get("CONED_SUBSCRIPTION_KEY")
