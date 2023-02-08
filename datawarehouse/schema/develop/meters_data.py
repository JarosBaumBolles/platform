"""Meters Data Schema"""
from datawarehouse.schema.common import format_schema

METERS_DATA_JSON_SCHEMA = [
    {
        "description": "Reference to hour",
        "mode": "REQUIRED",
        "name": "ref_hour_id",
        "policyTags": {
            "names": [
                (
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
                )
            ]
        },
        "type": "FLOAT",
    },
]

METERS_DATA_SCHEMA = format_schema(METERS_DATA_JSON_SCHEMA)
