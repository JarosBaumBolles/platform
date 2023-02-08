"""private_meters_data_deduplication_view view Schema"""

JSON_SCHEMA = {
    "creationTime": "1658591194969",
    "description": "private_meters_data_deduplication_view",
    "etag": "vu4H4rAE2u5rfxpCADFiGQ==",
    "id": "production-epbp:standardized_new.private_meters_data_deduplication_view",
    "kind": "bigquery#table",
    "labels": {"hourlybuildingdata": "production"},
    "lastModifiedTime": "1658591195180",
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
                "name": "data", 
                "type": "FLOAT"
            }            
        ]
    },
    "selfLink": (
        "https://bigquery.googleapis.com/bigquery/v2/projects/production-epbp/"
        "datasets/standardized_new/tables/"
        "private_meters_data_deduplication_view"
    ),
    "tableReference": {
        "datasetId": "standardized_new",
        "projectId": "production-epbp",
        "tableId": "private_meters_data_deduplication_view",
    },
    "type": "VIEW",
    "view": {
        "query": """
            SELECT *
                FROM production-epbp.standardized_new.meters_data
                GROUP BY ref_participant_id,
                    ref_hour_id,
                    ref_meter_id,
                    data
        """,
        "useLegacySql": false,
    },
}
