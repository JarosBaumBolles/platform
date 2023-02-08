"""private_calendar_deduplication_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1658596573427",
    "description": "private_calendar_deduplication_view",
    "etag": "nDvSzkEpfG10dK+WyUGikA==",
    "id": "production-epbp:standardized_new.private_calendar_deduplication_view",
    "kind": "bigquery#table",
    "labels": {"hourlybuildingdata": "production"},
    "lastModifiedTime": "1658596573635",
    "location": "US",
    "numActiveLogicalBytes": "0",
    "numBytes": "0",
    "numLongTermBytes": "0",
    "numLongTermLogicalBytes": "0",
    "numRows": "0",
    "numTotalLogicalBytes": "0",
    "schema": {
        "fields": [
            {
                "mode": "NULLABLE", 
                "name": "hour_id", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "ref_region_id", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "year", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "day", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "date", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "month", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "hour", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "dow", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "working_day", 
                "type": "BOOLEAN"
            }, {
                "mode": "NULLABLE", 
                "name": "month_name", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "month_name_abbr", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "dow_name", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "dow_name_abbr", 
                "type": "STRING"
            }
        ]
    },
    "selfLink": (
        "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/"
        "datasets/standardized_new/tables/private_calendar_deduplication_view"
    ),
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "private_calendar_deduplication_view",
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT *
                FROM production-epbp.standardized_new.calendar
                GROUP BY hour_id,
                    ref_region_id,
                    year,
                    day,
                    date,
                    month,
                    hour,
                    dow,
                    working_day,
                    month_name,
                    month_name_abbr,
                    dow_name,
                    dow_name_abbr
        """,
        "useLegacySql": false,
    },
}
