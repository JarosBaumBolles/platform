"""Utils to generate Schema from a given json schema"""
from google.cloud.bigquery import SchemaField


def format_schema(schema):
    """Generate schema from the given JSON schema"""
    return list(
        map(lambda row: SchemaField(row["name"], row["type"], row["mode"]), schema)
    )
