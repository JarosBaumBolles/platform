"""portal_properties_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1653408871913",
    "description": "portal_properties_view",
    "etag": "5u8kDHCfamEdzlPzMg0bFw==",
    "id": "production-epbp:standardized_new.portal_properties_view",
    "kind": "bigquery#table",
    "labels": {
        "hourlybuildingdata": "production"
    },
    "lastModifiedTime": "1653408872035",
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
                "name": "TIMESTAMP",
                "type": "DATETIME"
            },
            {
                "mode": "NULLABLE",
                "name": "YEAR",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "MONTH",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "date",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "HOUR",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "property_uri",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "property_id",
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
            }            
        ]
    },
    "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/datasets/standardized_new/tables/portal_properties_view",
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "portal_properties_view"
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT DATETIME(ca.year, ca.month, ca.date, ca.hour, 0, 0) AS timestamp,
                ca.year AS year,
                ca.month AS month,
                ca.date AS date,
                ca.hour AS hour,
                pr.property_uri AS property_uri,
                pr.property_id AS property_id,
                pr.footage AS footage,
                pr.name AS property_name,
                IFNULL(prm.property_letter, TO_HEX(SHA256(pr.name))) as property_letter
            FROM production-epbp.standardized_new.properties AS pr
            LEFT JOIN production-epbp.standardized_new.private_calendar_deduplication_view ca 
            ON pr.ref_region_id = ca.ref_region_id
            LEFT JOIN production-epbp.standardized_new.properties_mapper AS prm 
            ON prm.ref_participant_id = pr.ref_participant_id
                AND prm.ref_property_id = pr.property_id    
        """,
        "useLegacySql": false
    }
}
