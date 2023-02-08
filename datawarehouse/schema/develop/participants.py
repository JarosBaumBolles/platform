"""Participants table Schema"""
from datawarehouse.schema.common import format_schema

PARTICIPANTS_JSON_SCHEMA = [
    {
        "description": "Participant number, also used in GCP permissions",
        "mode": "REQUIRED",
        "name": "participant_id",
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
        "description": "Participant name to show in the platform",
        "mode": "REQUIRED",
        "name": "name",
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

PARTICIPANTS_SCHEMA = format_schema(PARTICIPANTS_JSON_SCHEMA)
