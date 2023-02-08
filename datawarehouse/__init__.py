"""Data Warehouse Schem Init Module"""
from datawarehouse.schema.develop.calendar import (
    CALENDAR_JSON_SCHEMA as DEVELOP_CALENDAR_JSON_SCHEMA,
)
from datawarehouse.schema.develop.calendar import CALENDAR_SCHEMA
from datawarehouse.schema.develop.meters import (
    METERS_JSON_SCHEMA as DEVELOP_METERS_JSON_SCHEMA,
)
from datawarehouse.schema.develop.meters import METERS_SCHEMA
from datawarehouse.schema.develop.meters_association import (
    METERS_ASSOCIATION_JSON_SCHEMA as DEVELOP_METERS_ASSOCIATION_JSON_SCHEMA,
)
from datawarehouse.schema.develop.meters_association import METERS_ASSOCIATION_SCHEMA
from datawarehouse.schema.develop.meters_data import (
    METERS_DATA_JSON_SCHEMA as DEVELOP_METERS_DATA_JSON_SCHEMA,
)
from datawarehouse.schema.develop.meters_data import METERS_DATA_SCHEMA
from datawarehouse.schema.develop.participants import (
    PARTICIPANTS_JSON_SCHEMA as DEVELOP_PARTICIPANTS_JSON_SCHEMA,
)
from datawarehouse.schema.develop.participants import PARTICIPANTS_SCHEMA
from datawarehouse.schema.develop.properties import (
    PROPERTIES_JSON_SCHEMA as DEVELOP_PROPERTIES_JSON_SCHEMA,
)
from datawarehouse.schema.develop.properties import PROPERTIES_SCHEMA
from datawarehouse.schema.develop.regions import (
    REGIONS_JSON_SCHEMA as DEVELOP_REGIONS_JSON_SCHEMA,
)
from datawarehouse.schema.develop.regions import REGIONS_SCHEMA


# =================== PRODUCTIOn SCHEMAs ====================================

from datawarehouse.schema.production.tables.calendar import (
    CALENDAR_JSON_SCHEMA as PRODUCTION_CALENDAR_JSON_SCHEMA,
    CALENDAR_SCHEMA as PRODUCTION_CALENDAR_SCHEMA
)
from datawarehouse.schema.production.tables.meters_association import (
    METERS_ASSOCIATION_JSON_SCHEMA as PRODUCTION_METERS_ASSOCIATION_JSON_SCHEMA,
    METERS_ASSOCIATION_SCHEMA as PRODUCTION_METERS_ASSOCIATION_SCHEMA
)
from datawarehouse.schema.production.tables.meters_data import (
    METERS_DATA_JSON_SCHEMA as PRODUCTION_METERS_DATA_JSON_SCHEMA,
    METERS_DATA_SCHEMA as PRODUCTION_METERS_DATA_SCHEMA,
)
from datawarehouse.schema.production.tables.meters import (
    METERS_JSON_SCHEMA as PRODUCTION_METERS_JSON_SCHEMA,
    METERS_SCHEMA as PRODUCTION_METERS_SCHEMA
)

from datawarehouse.schema.production.tables.participants import (
    PARTICIPANTS_JSON_SCHEMA as PRODUCTION_PARTICIPANTS_JSON_SCHEMA,
    PARTICIPANTS_SCHEMA as PRODUCTION_PARTICIPANTS_SCHEMA,
    
)
from datawarehouse.schema.production.tables.properties import (
    PROPERTIES_JSON_SCHEMA as PRODUCTION_PROPERTIES_JSON_SCHEMA,
    PROPERTIES_SCHEMA as PRODUCTION_PROPERTIES_SCHEMA,
)
from datawarehouse.schema.production.tables.regions import (
    REGIONS_JSON_SCHEMA as PRODUCTION_REGIONS_JSON_SCHEMA,
    REGIONS_SCHEMA as PRODUCTION_REGIONS_SCHEMA,
)

from datawarehouse.schema.production.tables.number_letters_mapper import (
    NUMBER_LETTERS_MAPPER_JSON_SCHEMA as PRODUCTION_NUMBER_LETTERS_MAPPER_JSON_SCHEMA,
    NUMBER_LETTERS_MAPPER_SCHEMA as PRODUCTION_NUMBER_LETTERS_MAPPER_SCHEMA,

)



# TODO: Fix Development schemas
__all__ = [
    "DEVELOP_CALENDAR_JSON_SCHEMA",
    "DEVELOP_METERS_ASSOCIATION_JSON_SCHEMA",
    "DEVELOP_METERS_DATA_JSON_SCHEMA",
    "DEVELOP_METERS_JSON_SCHEMA",
    "DEVELOP_PARTICIPANTS_JSON_SCHEMA",
    "DEVELOP_PROPERTIES_JSON_SCHEMA",
    "DEVELOP_REGIONS_JSON_SCHEMA",

    "PRODUCTION_CALENDAR_JSON_SCHEMA",
    "PRODUCTION_CALENDAR_SCHEMA",
    "PRODUCTION_METERS_ASSOCIATION_JSON_SCHEMA",
    "PRODUCTION_METERS_ASSOCIATION_SCHEMA",
    "PRODUCTION_METERS_DATA_JSON_SCHEMA",
    "PRODUCTION_METERS_DATA_SCHEMA",
    "PRODUCTION_METERS_JSON_SCHEMA",
    "PRODUCTION_METERS_SCHEMA",
    "PRODUCTION_PARTICIPANTS_JSON_SCHEMA",
    "PRODUCTION_PARTICIPANTS_SCHEMA",
    "PRODUCTION_PROPERTIES_JSON_SCHEMA",
    "PRODUCTION_PROPERTIES_SCHEMA"
    "PRODUCTION_REGIONS_JSON_SCHEMA",
    "PRODUCTION_REGIONS_SCHEMA",

    "PRODUCTION_NUMBER_LETTERS_MAPPER_JSON_SCHEMA",
    "PRODUCTION_NUMBER_LETTERS_MAPPER_SCHEMA"
]
