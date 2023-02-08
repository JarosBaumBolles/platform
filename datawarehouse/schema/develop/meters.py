"""Meters table Schema"""
from datawarehouse.schema.common import format_schema

METERS_JSON_SCHEMA = [
    {
        "description": "Meter identifier, internal and unique",
        "mode": "REQUIRED",
        "name": "meter_id",
        "policyTags": {
            "names": [
                (
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/1645562431116087891"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Meter URI as it appears in XML documents",
        "mode": "REQUIRED",
        "name": "meter_uri",
        "policyTags": {
            "names": [
                (
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/1645562431116087891"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/1645562431116087891"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
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
                    "projects/develop-epbp/locations/us/taxonomies/"
                    "1034364737699942884/policyTags/8345792757477959656"
                )
            ]
        },
        "type": "STRING",
    },
]

METERS_SCHEMA = format_schema(METERS_JSON_SCHEMA)
