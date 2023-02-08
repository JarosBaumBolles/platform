"""Regions table Schema"""
from datawarehouse.schema.common import format_schema

REGIONS_JSON_SCHEMA = [
    {
        "description": "Region identifier, internal and unique",
        "mode": "REQUIRED",
        "name": "region_id",
        "policyTags": {
            "names": [
                (
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/7580781401168557630"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Description of the region",
        "mode": "REQUIRED",
        "name": "description",
        "policyTags": {
            "names": [
                (
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/7580781401168557630"
                )
            ]
        },
        "type": "STRING",
    },
]

REGIONS_SCHEMA = format_schema(REGIONS_JSON_SCHEMA)
