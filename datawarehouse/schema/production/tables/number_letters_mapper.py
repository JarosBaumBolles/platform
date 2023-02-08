"""_number_letters_mapper table Schema"""
from datawarehouse.schema.common import format_schema

NUMBER_LETTERS_MAPPER_JSON_SCHEMA = [
    {
        "mode": "NULLABLE",
        "name": "number",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type": "INTEGER",
    },
    {
        "maxLength": "8",
        "mode": "NULLABLE",
        "name": "name",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type": "STRING",
    },
]

NUMBER_LETTERS_MAPPER_SCHEMA = format_schema(NUMBER_LETTERS_MAPPER_JSON_SCHEMA)
