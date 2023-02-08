"""portal_private_dashboard_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1658597576323",
    "description": "portal_private_dashboard_view",
    "etag": "LqNOxKJsbZ7D19Fq7q3rSg==",
    "id": "production-epbp:standardized_new.portal_private_dashboard_view",
    "kind": "bigquery#table",
    "labels": {"hourlybuildingdata": "production"},
    "lastModifiedTime": "1658597576573",
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
                "name": "participant_id", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "participant_name", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "property_name", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "property_uri", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "address", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "property_country", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "property_state", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "property_footage", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "hour", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "timestamp", 
                "type": "DATETIME"
            }, {
                "mode": "NULLABLE", 
                "name": "meter_weight", 
                "type": "FLOAT"
            }, {
                "mode": "NULLABLE", 
                "name": "meter_uri", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "meter_type", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "unitOfMeasure", 
                "type": "STRING"
            }, {
                "mode": "NULLABLE", 
                "name": "meter_value", 
                "type": "FLOAT"
            }
        ]
    },
    "selfLink": (
        "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp"
        "/datasets/standardized_new/tables/portal_private_dashboard_view"
    ),
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "portal_private_dashboard_view",
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT  pt.participant_id AS participant_id,
                pt.name           AS participant_name,
                prpt.name         AS property_name,
                prpt.property_uri AS property_uri,
                CONCAT(
                        IFNULL(prpt.address1, ' '), ' ',
                        IFNULL(prpt.address2, ' '), ' ',
                        IFNULL(prpt.city, ' '), ' ',
                        IFNULL(prpt.state, ' '), ' ',
                        IFNULL(prpt.postal_code, ' ')
                    )             AS address,
                prpt.country      AS property_country,
                prpt.state        AS property_state,
                prpt.footage      AS property_footage,
                ma.ref_hour_id    AS hour,
                DATETIME(ca.year, ca.month, ca.date, ca.hour, 0, 0) AS timestamp,
                ma.weight         AS meter_weight,
                mt.meter_uri      AS meter_uri,
                mt.type           AS meter_type,
                mt.unitOfMeasure  AS unitOfMeasure,
                md.data           AS meter_value,
                FROM production-epbp.standardized_new.properties AS prpt
                    LEFT JOIN production-epbp.standardized_new.participants AS pt
                        ON pt.participant_id = prpt.ref_participant_id
                    LEFT JOIN production-epbp.standardized_new.private_meters_association_deduplication_view AS ma
                        ON ma.ref_participant_id = prpt.ref_participant_id
                            AND ma.ref_property_id = prpt.property_id
                    LEFT JOIN production-epbp.standardized_new.meters AS mt ON mt.meter_id = ma.ref_meter_id
                    LEFT JOIN production-epbp.standardized_new.private_meters_data_deduplication_view AS md
                        ON md.ref_meter_id = ma.ref_meter_id
                            AND md.ref_hour_id = ma.ref_hour_id
                            AND md.ref_participant_id=ma.ref_participant_id
                    LEFT JOIN production-epbp.standardized_new.regions AS rg
                        ON rg.region_id=prpt.ref_region_id
                    LEFT JOIN production-epbp.standardized_new.private_calendar_deduplication_view AS ca
                        ON ca.ref_region_id=prpt.ref_region_id
                            AND ca.hour_id = ma.ref_hour_id
                WHERE md.ref_participant_id=ma.ref_participant_id
                    AND prpt.ref_region_id=prpt.ref_region_id
                ORDER BY participant_id, participant_name, property_name DESC
        """,
        "useLegacySql": false,
    },
}
