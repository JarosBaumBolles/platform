"""properties_mapper table Schema"""
from datawarehouse.schema.common import format_schema

PARTICIPANTS_MAPPER_JSON_SCHEMA = [
    {
        "description": "Reference to participant",
        "mode": "REQUIRED",
        "name": "ref_participant_id",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/1753522189281652513"
            ]
        },
        "type": "INTEGER"
    },
    {
        "description": "Reference to property",
        "mode": "REQUIRED",
        "name": "ref_property_id",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/1753522189281652513"
            ]
        },
        "type": "INTEGER"
    },
    {
        "description": "Property sha256 hash value",
        "mode": "REQUIRED",
        "name": "property_hash",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type": "STRING"
    },
    {
        "description": "Property mapper letter",
        "mode": "REQUIRED",
        "name": "property_letter",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type": "STRING"
    }
]


PARTICIPANTS_MAPPER_SCHEMA = format_schema(PARTICIPANTS_MAPPER_JSON_SCHEMA)