"""private_properties_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1657016429567",
    "description": "private_properties_view",
    "etag": "YwYAPKo0v4sxfQez+Z2HAg==",
    "id": "production-epbp:standardized_new.private_properties_view",
    "kind": "bigquery#table",
    "labels": {
        "hourlybuildingdata": "production"
    },
    "lastModifiedTime": "1657016430239",
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
                "type": "INTEGER"
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
    "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/datasets/standardized_new/tables/private_properties_view",
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "private_properties_view"
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT ppv.timestamp AS timestamp,
                ppv.year AS year,
                ppv.month AS month,
                ppv.date AS date,
                ppv.hour AS hour,
                ppv.footage AS footage,
                ppv.property_name AS property_name,
                ppv.property_letter AS property_letter,
                ev.electric AS energy,
                ov.occupancy AS occupancy,
                tv.sum_temperature AS temperature,
                rtv.sum_real_feel_temperature AS real_feel_temperature,
                hmv.sum_humidity AS humidity,
                awdv.sum_wind_direction AS wind_direction,
                awsv.sum_wind_speed AS wind_speed,
                accv.sum_cloud_cover AS cloud_cover,
                adpv.sum_dew_point AS dew_point,
                mgev.sum_grid_emissions AS emissions,
                agev.sum_grid_emissions AS average_grid_emissions,
            FROM production-epbp.standardized_new.portal_properties_view AS ppv
            LEFT JOIN production-epbp.standardized_new.portal_properties_electricity_view AS ev
                ON ppv.timestamp=ev.timestamp AND ppv.property_uri=ev.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_occupancy_view AS ov
                ON ppv.timestamp=ov.timestamp AND ppv.property_uri=ov.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_ambient_temperature_view AS tv
                ON ppv.timestamp=tv.timestamp AND ppv.property_uri=tv.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_ambient_real_feel_temperature_view AS rtv
                ON ppv.timestamp=rtv.timestamp AND ppv.property_uri=rtv.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_ambient_humidity_view AS hmv 
                ON ppv.timestamp=hmv.timestamp AND ppv.property_uri=hmv.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_ambient_wind_direction_view AS awdv 
                ON ppv.timestamp=awdv.timestamp AND ppv.property_uri=awdv.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_ambient_wind_speed_view AS awsv 
                ON ppv.timestamp=awsv.timestamp AND ppv.property_uri=awsv.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_ambient_cloud_cover_view AS accv 
                ON ppv.timestamp=accv.timestamp AND ppv.property_uri=accv.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_ambient_dew_point_view AS adpv 
                ON ppv.timestamp=adpv.timestamp AND ppv.property_uri=adpv.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_marginal_grid_emissions_view AS mgev 
                ON ppv.timestamp=mgev.timestamp AND ppv.property_uri=mgev.property_uri
            LEFT JOIN production-epbp.standardized_new.portal_properties_average_grid_emissions_view AS agev 
                ON ppv.timestamp=agev.timestamp AND ppv.property_uri=agev.property_uri       
        """,
        "useLegacySql": false
    }
}
