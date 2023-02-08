"""
This package has core logic related to dispatcher implementation.
"""

from os import path

import google.auth
from common import settings as CONF
from common.cache import lru_cache_expiring
from common.settings import PROJECT
# Project imports
from genericpath import exists
# GCP imports
from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache

API_PREFFIX = f'projects/{CONF.PROJECT}/locations/{CONF.GCP_REGION}'


@lru_cache_expiring(maxsize = 512, expires = 3600)
def _get_cloud_function_api_service():
    """
    Prepare API service to perform requests against.

    If the environment variable GOOGLE_APPLICATION_CREDENTIALS is set,
    ADC uses the service account file that the variable points to.

    If the environment variable GOOGLE_APPLICATION_CREDENTIALS isn't set,
    ADC uses the default service account that Compute Engine, Google Kubernetes Engine,
    App Engine, Cloud Run, and Cloud Functions provide.

    See more on https://cloud.google.com/docs/authentication/production
    """

    class MemoryCache(Cache):
        """ Cache to speed up logic and minimize external requests. """

        _cache = {}

        def get(self, url):
            return MemoryCache._cache.get(url)

        def set(self, url, content):
            MemoryCache._cache[url] = content

    credentials, _ = google.auth.default(
        ['https://www.googleapis.com/auth/cloud-platform']
    )

    return build('cloudfunctions', 'v1', credentials=credentials, cache=MemoryCache())


def call_cloud_function(function_name, function_body=None):
    """ Use GCP service to call cloud function. """
    return (
        _get_cloud_function_api_service()
        .projects()
        .locations()
        .functions()
        .call(
            name=f'{API_PREFFIX}/functions/{function_name}',
            body=function_body or {}
        ).execute()
    )


@lru_cache_expiring(maxsize = 512, expires = 3600)
def list_cloud_functions(project: str = None, region: str = None) -> dict:
    """Get list of all deployed functions for the given environment"""    
    parent = f'projects/{project}/locations/{region}' if all(x for x in (project, region,)) else API_PREFFIX

    response = (
        _get_cloud_function_api_service()
        .projects()
        .locations()
        .functions()
        .list(
            parent=parent
        ).execute()
    )

    if not response:
        return {}

    function_info = {}
    for func in response.get('functions', []):    
        key = func['name'].replace(f'{parent}/functions/', '')
        function_info[key] = {key:val for key, val in func.items() if key != 'name'}

    return function_info


@lru_cache_expiring(maxsize = 512, expires = 1300)
def is_cloud_function_exists(function_name: str, project: str = None, region: str = None) -> bool:
    """Check if cloud function is deployed in the environment"""
    return function_name.strip() in list_cloud_functions(project=project, region=region).keys()
