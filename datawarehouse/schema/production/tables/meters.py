"""Meters table Schema"""
from datawarehouse.schema.common import format_schema

METERS_JSON_SCHEMA = [
    {
        "description": "Meter identifier for the use in DW, internal and unique",
        "mode": "REQUIRED",
        "name": "meter_id",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/1753522189281652513"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Meter URI as it apperas in XML documents",
        "mode": "REQUIRED",
        "name": "meter_uri",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/1753522189281652513"
                )
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Reference to participant",
        "mode": "REQUIRED",
        "name": "ref_participant_id",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/1753522189281652513"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Meter type",
        "mode": "REQUIRED",
        "name": "type",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Unit of measure",
        "mode": "REQUIRED",
        "name": "unitOfMeasure",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Expected frequency of data updates",
        "mode": "REQUIRED",
        "name": "updateFrequency",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Haystack tags",
        "mode": "REQUIRED",
        "name": "haystack",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/"
                    "8809944157278434441/policyTags/7655947349683900594"
                )
            ]
        },
        "type": "STRING",
    },
]

METERS_SCHEMA = format_schema(METERS_JSON_SCHEMA)
