"""Calendar table Schema"""
from datawarehouse.schema.common import format_schema

CALENDAR_JSON_SCHEMA = [
    {
        "description": (
            "Reference to the region, regions here mostly differ in their "
            "working days schedule"
        ),
        "mode": "REQUIRED",
        "name": "ref_region_id",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": (
            "Hour identifier in the form of YYMMDDHH, such as 21113000 for the "
            "first hour of 30 Nov 21, or 21113023 for the last hour of 30 Nov 21"
        ),
        "mode": "REQUIRED",
        "name": "hour_id",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Year, such as 2021",
        "mode": "REQUIRED",
        "name": "year",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Day of year, 1 - 365",
        "mode": "REQUIRED",
        "name": "day",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Date in month, 1 - 31",
        "mode": "REQUIRED",
        "name": "date",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Month number, 1 - 12",
        "mode": "REQUIRED",
        "name": "month",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Hour in a day, 0-23",
        "mode": "REQUIRED",
        "name": "hour",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Day of week, Mon = 0, Sun = 6",
        "mode": "REQUIRED",
        "name": "dow",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "INTEGER",
    },
    {
        "description": "Mon-Fri except public holidays",
        "mode": "REQUIRED",
        "name": "working_day",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "BOOLEAN",
    },
    {
        "description": "January-December",
        "mode": "REQUIRED",
        "name": "month_name",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Jan-Dec",
        "mode": "REQUIRED",
        "name": "month_name_abbr",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Sunday-Saturday",
        "mode": "REQUIRED",
        "name": "dow_name",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "STRING",
    },
    {
        "description": "Sun-Sat",
        "mode": "REQUIRED",
        "name": "dow_name_abbr",
        "policyTags": {
            "names": [
                (
                    "projects/production-epbp/locations/us/taxonomies/8809944157278434441"
                    "/policyTags/8973266864876268710"
                )
            ]
        },
        "type": "STRING",
    },
]


CALENDAR_SCHEMA = format_schema(CALENDAR_JSON_SCHEMA)
