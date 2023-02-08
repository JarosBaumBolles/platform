"""portal_properties_ambient_wind_direction_view  view Schema"""

JSON_SCHEMA = {
    "creationTime": "1652814592921",
    "description": "portal_properties_ambient_wind_direction_view ",
    "etag": "vaMsx3jIFplWUQ/+/5uNLQ==",
    "id": "production-epbp:standardized_new.portal_properties_ambient_wind_direction_view",
    "kind": "bigquery#table",
    "labels": {
        "hourlybuildingdata": "production"
    },
    "lastModifiedTime": "1653323495578",
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
                "name": "property_uri",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "sum_wind_direction",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "avg_wind_direction",
                "type": "FLOAT"
            }
        ]
    },
    "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/datasets/standardized_new/tables/portal_properties_ambient_wind_direction_view",
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "portal_properties_ambient_wind_direction_view"
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT DISTINCT DATETIME(ca.year, ca.month, ca.date, ca.hour, 0, 0) AS timestamp,
                                    pr.property_uri AS property_uri,
                                    sum(DISTINCT DATA * weight) AS sum_wind_direction,
                                    avg(DISTINCT DATA * weight) AS avg_wind_direction,
                    FROM production-epbp.standardized_new.private_meters_association_deduplication_view AS ma
                    LEFT JOIN production-epbp.standardized_new.participants AS pa ON ma.ref_participant_id = pa.participant_id
                    LEFT JOIN production-epbp.standardized_new.private_calendar_deduplication_view AS ca ON ma.ref_hour_id=ca.hour_id
                    LEFT JOIN production-epbp.standardized_new.properties AS pr ON ma.ref_property_id=pr.property_id
                    LEFT JOIN production-epbp.standardized_new.meters AS mr ON ma.ref_meter_id=mr.meter_id
                    LEFT JOIN production-epbp.standardized_new.private_meters_data_deduplication_view AS md ON ma.ref_meter_id=md.ref_meter_id
                    AND ma.ref_hour_id=md.ref_hour_id
                    WHERE mr.type = 'Ambient Wind Direction'
                    AND md.ref_hour_id = ca.hour_id
                    GROUP BY pr.property_uri,
                            ca.year,
                            ca.month,
                            ca.date,
                            ca.hour
                    ORDER BY timestamp ASC    
        """,
        "useLegacySql": false
    }
}
