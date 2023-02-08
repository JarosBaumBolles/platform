"""private_meters_association_deduplication_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1658594295151",
    "description": "private_meters_association_deduplication_view",
    "etag": "Kq6i4kEf1whl6CD6YT9a8A==",
    "id": "production-epbp:standardized_new.private_meters_association_deduplication_view",
    "kind": "bigquery#table",
    "labels": {"hourlybuildingdata": "production"},
    "lastModifiedTime": "1658594295333",
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
                "name": "ref_hour_id", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "ref_participant_id", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "ref_meter_id", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "ref_property_id", 
                "type": "INTEGER"
            }, {
                "mode": "NULLABLE", 
                "name": "weight", 
                "type": "FLOAT"
            }
        ]
    },
    "selfLink": (
        "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/"
        "datasets/standardized_new/tables/"
        "private_meters_association_deduplication_view"
    ),
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "private_meters_association_deduplication_view",
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT *
                FROM standardized_new.meters_association
                GROUP BY ref_hour_id,
                    ref_participant_id,
                    ref_meter_id,
                    ref_property_id,
                    weight
        """,
        "useLegacySql": false,
    },
}
