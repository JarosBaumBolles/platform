"""Properties table Schema"""
from datawarehouse.schema.common import format_schema

PROPERTIES_JSON_SCHEMA = [
    {
        "description": "Reference to the region where the property is located",
        "mode": "REQUIRED",
        "name": "ref_region_id",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Property identifier for the use in DW, internal and unique",
        "mode": "REQUIRED",
        "name": "property_id",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/1753522189281652513"
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Property URI usend in XML files",
        "mode": "REQUIRED",
        "name": "property_uri",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/1753522189281652513"
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
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/1753522189281652513"
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Property name",
        "mode": "REQUIRED",
        "name": "name",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building adress",
        "mode": "REQUIRED",
        "name": "address1",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building adress for the apartment, suite, unit number, or other address designation",
        "mode": "REQUIRED",
        "name": "address2",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building city",
        "mode": "REQUIRED",
        "name": "city",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building country",
        "mode": "REQUIRED",
        "name": "country",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building state",
        "mode": "REQUIRED",
        "name": "state",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building postal code",
        "mode": "REQUIRED",
        "name": "postal_code",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building footage, sq. ft.",
        "mode": "REQUIRED",
        "name": "footage",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Building footage category, fine-grained, scale of ten",
        "mode": "REQUIRED",
        "name": "footage_10_categories",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Building footage category, coarse-grained, scale of three",
        "mode": "REQUIRED",
        "name": "footage_3_categories",
        "policyTags": {
            "names": [
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/7655947349683900594"
            ]
        },
        "type": "STRING",
    },
]

PROPERTIES_SCHEMA = format_schema(PROPERTIES_JSON_SCHEMA)
