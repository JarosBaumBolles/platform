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
        "description": "Reference to property that is associated with this meter",
        "mode": "REQUIRED",
        "name": "ref_property_id",
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
        "description": "Meter weight this hour",
        "mode": "REQUIRED",
        "name": "weight",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/1753522189281652513"
                )
            ]
        },
        "type": "FLOAT",
    },
]

METERS_ASSOCIATION_SCHEMA = format_schema(METERS_ASSOCIATION_JSON_SCHEMA)
