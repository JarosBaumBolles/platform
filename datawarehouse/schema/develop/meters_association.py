"""Meters Association table Schema"""
from datawarehouse.schema.common import format_schema

METERS_ASSOCIATION_JSON_SCHEMA = [
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
        "description": "Reference to property that is associated with this meter",
        "mode": "REQUIRED",
        "name": "ref_property_id",
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
        "description": "Meter weight this hour",
        "mode": "REQUIRED",
        "name": "weight",
        "policyTags": {
            "names": [
                (
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/1645562431116087891"
                )
            ]
        },
        "type": "FLOAT",
    },
]

METERS_ASSOCIATION_SCHEMA = format_schema(METERS_ASSOCIATION_JSON_SCHEMA)
