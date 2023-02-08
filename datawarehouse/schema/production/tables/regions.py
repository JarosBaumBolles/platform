"""Regions table Schema"""
from datawarehouse.schema.common import format_schema

REGIONS_JSON_SCHEMA = [
    {
        "description":"Region identifier, internal and unique",
        "mode":"REQUIRED",
        "name":"region_id",
        "policyTags":{
            "names":[
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type":"INTEGER"
    },

    {
        "description":"Country",
        "mode":"REQUIRED",
        "name":"country",
        "policyTags":{
            "names":[
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type":"STRING"
    },

    {
        "description":"State",
        "mode":"REQUIRED",
        "name":"state",
        "policyTags":{
            "names":[
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type":"STRING"
    },

    {
        "description":"Timezone",
        "mode":"REQUIRED",
        "name":"timezone",
        "policyTags":{
            "names":[
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type":"STRING"
    },

    {
        "description":"Description of the region",
        "mode":"REQUIRED",
        "name":"description",
        "policyTags":{
            "names":[
                "projects/production-epbp/locations/us/taxonomies/8809944157278434441/policyTags/8973266864876268710"
            ]
        },
        "type":"STRING"
    }
]

REGIONS_SCHEMA = format_schema(REGIONS_JSON_SCHEMA)
