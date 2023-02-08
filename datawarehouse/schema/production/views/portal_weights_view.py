"""portal_weights_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1648631822355",
    "description": "portal_weights_view",
    "etag": "ziHLBGVLFEzu2Cxxv7bJoQ==",
    "id": "production-epbp:standardized_new.portal_weights_view",
    "kind": "bigquery#table",
    "labels": {
        "hourlybuildingdata": "production"
    },
    "lastModifiedTime": "1653323671323",
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
                "name": "property_uri",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "meter_uri",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "hour_id",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "TYPE",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "weight",
                "type": "FLOAT"
            }
        ]
    },
    "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/datasets/standardized_new/tables/portal_weights_view",
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "portal_weights_view"
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT prpt.property_uri AS property_uri,
                    mt.meter_uri AS meter_uri,
                    ma.ref_hour_id AS hour_id,
                    mt.type AS type,
                    ma.weight AS weight
                FROM production-epbp.standardized_new.private_meters_association_deduplication_view AS ma
                RIGHT JOIN (
                    SELECT ref_property_id,
                        ref_meter_id,
                        ref_participant_id,
                        MAX(ref_hour_id) AS hour_id
                    FROM production-epbp.standardized_new.private_meters_association_deduplication_view
                    GROUP BY ref_property_id,
                        ref_meter_id,
                        ref_participant_id,
                        ref_meter_id,
                        ref_participant_id
                    ) AS max_mt ON ma.ref_property_id = max_mt.ref_property_id
                        AND ma.ref_meter_id = max_mt.ref_meter_id
                        AND ma.ref_participant_id = max_mt.ref_participant_id
                        AND ma.ref_hour_id = max_mt.hour_id
                LEFT JOIN production-epbp.standardized_new.meters mt ON mt.meter_id=ma.ref_meter_id
                    AND (
                        ma.ref_participant_id = mt.ref_participant_id
                        OR mt.ref_participant_id=0
                    )
                LEFT JOIN production-epbp.standardized_new.properties AS prpt ON ma.ref_property_id = prpt.property_id
                WHERE ma.ref_meter_id = mt.meter_id
                    AND (
                        ma.ref_participant_id = mt.ref_participant_id
                        OR mt.ref_participant_id=0
                    )
        """,
        "useLegacySql": false
    }
}
