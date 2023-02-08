"""private_presentation_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1652870805222",
    "description": "private_presentation_view",
    "etag": "FjfVLwDcPJOSX5/ZTG/mJw==",
    "id": "production-epbp:standardized_new.private_presentation_view",
    "kind": "bigquery#table",
    "labels": {
        "hourlybuildingdata": "production"
    },
    "lastModifiedTime": "1653323718709",
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
                "name": "timestamp",
                "type": "DATETIME"
            },
            {
                "mode": "NULLABLE",
                "name": "year",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "month",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "date",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "hour",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "footage",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "property_name",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "energy",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "occupancy",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "temperature",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "real_feel_temperature",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "humidity",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "wind_direction",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "wind_speed",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "cloud_cover",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "dew_point",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "emissions",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "average_grid_emissions",
                "type": "FLOAT"
            }
        ]
    },
    "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/datasets/standardized_new/tables/private_presentation_view",
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "private_presentation_view"
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT * FROM production-epbp.standardized_new.private_properties_view
            UNION ALL
            SELECT * FROM production-epbp.standardized_new.private_average_view        
        """,
        "useLegacySql": false
    }
}
