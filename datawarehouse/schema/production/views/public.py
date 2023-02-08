"""public view Schema"""

JSON_SCHEMA = {
    "creationTime": "1654116584401",
    "description": "public",
    "etag": "XlVUW7rAKD1xomeSoktpHw==",
    "id": "production-epbp:standardized_new.public",
    "kind": "bigquery#table",
    "labels": {
        "hourlybuildingdata": "production"
    },
    "lastModifiedTime": "1654116585873",
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
                "name": "Time",
                "type": "DATETIME"
            },
            {
                "mode": "NULLABLE",
                "name": "Year",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Month",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Date",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Hour",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Electricity",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Emissions",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Occupancy",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Temperature",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "RealFeelTemperature",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Humidity",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "WindDirection",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "WindSpeed",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "CloudCover",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "DewPoint",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "AverageGridEmissions",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Footage",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "RealProperty",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Property",
                "type": "STRING"
            }
        ]
    },
    "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/datasets/standardized_new/tables/public",
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "public"
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT timestamp AS Time,
                year AS Year,
                month AS Month,
                date AS Date,
                hour AS Hour,
                energy AS Electricity,
                emissions AS Emissions,
                occupancy AS Occupancy,
                temperature AS Temperature,
                real_feel_temperature AS RealFeelTemperature,
                humidity AS Humidity,
                wind_direction AS WindDirection,
                wind_speed AS WindSpeed,
                cloud_cover AS CloudCover,
                dew_point AS DewPoint,
                average_grid_emissions AS AverageGridEmissions,
                round(footage, -3) AS Footage,
                property_name AS RealProperty,
                CASE property_name
                WHEN 'Average' THEN 'Avg. Bldg'
                ELSE CONCAT('Bldg ', property_letter)
                END as Property,
            FROM production-epbp.standardized_new.private_presentation_view
        """,
        "useLegacySql": false
    }
}
