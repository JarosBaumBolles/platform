"""Paricipants table Schema"""
from datawarehouse.schema.common import format_schema

PARTICIPANTS_JSON_SCHEMA = [
    {
        "description": "Participant number, also used in GCP permissions",
        "mode": "REQUIRED",
        "name": "participant_id",
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
        "description": "Participant name to show in the platform",
        "mode": "REQUIRED",
        "name": "name",
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

PARTICIPANTS_SCHEMA = format_schema(PARTICIPANTS_JSON_SCHEMA)
