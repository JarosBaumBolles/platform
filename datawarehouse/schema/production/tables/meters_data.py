"""Meters Data table Schema"""
from datawarehouse.schema.common import format_schema

METERS_DATA_JSON_SCHEMA = [
    {
        "description": "Reference to hour",
        "mode": "REQUIRED",
        "name": "ref_hour_id",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Reference to participant",
        "mode": "REQUIRED",
        "name": "ref_participant_id",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Reference to meter",
        "mode": "REQUIRED",
        "name": "ref_meter_id",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Metered data, using units of measure of the meter",
        "mode": "REQUIRED",
        "name": "data",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "FLOAT",
    },
]

METERS_DATA_SCHEMA = format_schema(METERS_DATA_JSON_SCHEMA)
