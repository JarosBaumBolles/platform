from google.cloud.bigquery.dbapi import connect


def get_db_connection():
    """Prepare bigquery storage client"""
    return connect()
