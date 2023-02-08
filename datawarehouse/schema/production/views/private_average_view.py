"""private_average_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1657016801447",
    "description": "private_average_view",
    "etag": "AKLLjTTX7qTwnQ/HAMihdA==",
    "id": "production-epbp:standardized_new.private_average_view",
    "kind": "bigquery#table",
    "labels": {
        "hourlybuildingdata": "production"
    },
    "lastModifiedTime": "1657016802073",
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
                "name": "property_letter",
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
    "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/datasets/standardized_new/tables/private_average_view",
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "private_average_view"
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT ppv.timestamp AS timestamp,
                ppv.year AS year,
                ppv.month AS month,
                ppv.date AS date,
                ppv.hour AS hour,
                AVG(ppv.footage) AS footage,
                'Average' AS property_name,
                '' AS property_letter,
                AVG(ppv.energy) AS energy,
                AVG(ppv.occupancy) AS occupancy,
                AVG(ppv.temperature) AS temperature,
                AVG(ppv.real_feel_temperature) AS real_feel_temperature,
                AVG(ppv.humidity) AS humidity,
                AVG(ppv.wind_direction) AS wind_direction,
                AVG(ppv.wind_speed) AS wind_speed,
                AVG(ppv.cloud_cover) AS cloud_cover,
                AVG(ppv.dew_point) AS dew_point,
                AVG(ppv.emissions) AS emissions,
                AVG(ppv.average_grid_emissions) AS average_grid_emissions
            FROM production-epbp.standardized_new.private_properties_view AS ppv
            GROUP BY ppv.timestamp,
                    ppv.year,
                    ppv.month,
                    ppv.date,
                    ppv.hour
            ORDER BY timestamp DESC        
        """,
        "useLegacySql": false
    }
}

