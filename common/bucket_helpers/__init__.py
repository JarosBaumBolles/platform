"""
This package has all helper logic related to bucket operations, including fetching and saving data.
"""

import uuid
from datetime import datetime, timedelta
from json import loads
from os import path
from pathlib import Path
from typing import List, Optional, Tuple, Union

from google.cloud.storage import Blob, Bucket, Client
from pendulum import DateTime

from common.cache import lru_cache_expiring
from common.date_utils import date_range_in_past, format_date, parse, truncate
from common.logging import Logger
from common.settings import PROCESSING_DATE_FORMAT

LOGGER = Logger(
    name="Bucket helpers",
    level="DEBUG",
    description="Bucket helpers",
    trace_id=uuid.uuid4(),
)


def require_client(client: Optional[Client] = None) -> Client:
    """Check client or verify over-ride."""
    return get_storadge_client() if client is None else client


@lru_cache_expiring(maxsize=512, expires=3600)
def get_storadge_client() -> Client:
    """Prepare bucket storage client"""
    return Client()


def file_exists(
    bucket: str,
    subdirectory: Optional[str] = None,
    file_name: Optional[str] = None,
    client: Optional[Client] = None,
) -> bool:
    """Determines whether or not this blob exists."""
    if file_name is None:
        return False

    storage_client = require_client(client)
    prefix = "/" if subdirectory is None else f'{subdirectory.rstrip("/").lstrip("/")}'

    return Blob(
        bucket=storage_client.get_bucket(bucket), name=path.join(prefix, file_name)
    ).exists(storage_client)


# TOOD: Should be removed as outdated
def upload_file(  # pylint:disable=too-many-arguments
    bucket_name: str,
    blob_text: str,
    integration: str,
    data_type: str,
    subdirectory: Optional[str] = None,
    file_name: Optional[str] = None,
) -> None:
    """Uploads a file with provided content to the bucket."""

    prefix = (
        "/" if subdirectory is None else f'/{subdirectory.rstrip("/").lstrip("/")}/'
    )

    if file_name is None:
        file_name = datetime.utcnow().strftime("%Y-%m-%dT%H:00:00")

    upload_file_to_bucket(
        bucket_name,
        blob_text,
        f"{integration}/{data_type}{prefix}",
        file_name=file_name,
    )


def upload_file_to_bucket(
    bucket_name: str,
    blob_text: str,
    blob_path: str,
    file_name: Optional[str] = None,
    client: Optional[Client] = None,
) -> None:
    """Uploads a file with provided content to the bucket."""
    blob_path = "/" if blob_path is None else f'{blob_path.rstrip("/").lstrip("/")}/'

    if file_name is None:
        file_name = datetime.utcnow().strftime("%Y-%m-%dT%H:00:00")

    storage_client = require_client(client)

    storage_client.get_bucket(bucket_name).blob(
        f"{blob_path}{file_name}"
    ).upload_from_string(blob_text)


@lru_cache_expiring(maxsize=256, expires=3600)
def get_file_contents(
    bucket_name: str,
    blob_path: str,
    binary_mode: bool = False,
    client: Optional[Client] = None,
) -> Union[bytes, str]:
    """Downloads file from the bucket and returns its content."""

    storage_client = get_storadge_client() if client is None else client

    blob = storage_client.get_bucket(bucket_name).blob(blob_path)

    if not binary_mode:
        return blob.download_as_string().decode("utf-8")

    return blob.download_as_bytes()


#  TOTO: Remove as outdated
def get_configuration(bucket_name, integration):
    """Downloads configuration file from the bucket and returns its content."""

    return loads(get_file_contents(bucket_name, "config/integrations.json"))[
        integration
    ]


def get_missed_standardized_files(  # pylint:disable=too-many-arguments
    bucket_name: str,
    bucket_path: Optional[str] = None,
    start_date: Optional[DateTime] = None,
    range_hours: int = 24,
    date_format: str = PROCESSING_DATE_FORMAT,
    except_values: Optional[Tuple[DateTime]] = None,
    client: Optional[Client] = None,
) -> List[DateTime]:
    """Check bucket contents for missing files representing polling responses."""

    storage_client = require_client(client)
    bucket = storage_client.bucket(bucket_name)
    bucket_path = Path("/" if not bucket_path else bucket_path.rstrip("/").lstrip("/"))

    start_date = start_date or parse()

    date_range = sorted(
        map(
            lambda x: format_date(x, date_format),
            date_range_in_past(
                start_date=truncate(start_date, level="hour"),
                hours=range_hours - 1,
                trunc_level="hour",
                except_values=except_values,
            ),
        )
    )

    fls = list_blobs_with_prefix(
        bucket_name=bucket,
        start_offset=str(bucket_path.joinpath(date_range[0])),
        end_offset=str(bucket_path.joinpath(date_range[-1])),
    )

    fnd_fls = set()

    for fl_blob in fls:
        flp = Path(fl_blob.name)
        if flp.parent == bucket_path:
            fnd_fls.add(flp.name)

    return list(
        map(
            lambda x: parse(x, PROCESSING_DATE_FORMAT),
            set(date_range).difference(fnd_fls),
        )
    )


def list_blobs_with_prefix(
    bucket_name: str,
    prefix: Optional[str] = None,
    delimiter: Optional[str] = None,
    client: Optional[Client] = None,
    force_dir: bool = True,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None,
    start_offset: Optional[str] = None,
    end_offset: Optional[str] = None,
    include_trailing_delimiter: Optional[bool] = None,
    versions: Optional[bool] = None,
    projection: str = "noAcl",
    fields: Optional[str] = None,
    page_size: Optional[int] = None,
) -> list:
    """Lists all the blobs in the bucket that begin with the prefix.

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder"
    """

    storage_client = require_client(client)
    dir_preffix = "/" if force_dir else ""
    if prefix:
        prefix = f'{prefix.lstrip("/").rstrip("/")}{dir_preffix}'

    blobs = storage_client.list_blobs(
        bucket_or_name=bucket_name,
        max_results=max_results,
        page_token=page_token,
        prefix=prefix,
        delimiter=delimiter,
        start_offset=start_offset,
        end_offset=end_offset,
        include_trailing_delimiter=include_trailing_delimiter,
        versions=versions,
        projection=projection,
        fields=fields,
        page_size=page_size,
    )
    return [blob for blob in blobs if blob.name != prefix] if prefix else list(blobs)


def delete_blob(  # pylint:disable=too-many-arguments,too-many-locals
    bucket_name: str,
    blob_name: str,
    client: Optional[Client] = None,
    if_generation_match=None,
    if_generation_not_match=None,
    if_metageneration_match=None,
    if_metageneration_not_match=None,
    quiet: bool = False,
) -> None:
    """delete a blob from one bucket to another with a new name."""

    client = require_client(client)
    fl_bucket = client.bucket(bucket_name)
    fl_blob = fl_bucket.blob(blob_name)
    fl_blob.delete(
        client=client,
        if_generation_match=if_generation_match,
        if_generation_not_match=if_generation_not_match,
        if_metageneration_match=if_metageneration_match,
        if_metageneration_not_match=if_metageneration_not_match,
    )
    if not quiet:
        LOGGER.info(
            f"INFO: Blob {fl_blob.name} in bucket {fl_bucket.name} "
            f"succesfully deleted."
        )


def delete_blobs_in_directory(  # pylint:disable=too-many-arguments,too-many-locals
    bucket_name: str,
    preffix: str,
    client: Optional[Client] = None,
    if_generation_match=None,
    if_generation_not_match=None,
    if_metageneration_match=None,
    if_metageneration_not_match=None,
    quiet: bool = False,
) -> None:
    """delete a blob from one bucket to another with a new name."""

    client = require_client(client)
    bucket = client.bucket(bucket_name)

    blobs = list_blobs_with_prefix(
        bucket_name=bucket_name, prefix=preffix, client=client
    )

    if blobs:
        bucket.delete_blobs(
            blobs=blobs,
            if_generation_match=if_generation_match,
            if_generation_not_match=if_generation_not_match,
            if_metageneration_match=if_metageneration_match,
            if_metageneration_not_match=if_metageneration_not_match,
        )
    if not quiet:
        LOGGER.info(f"Deleted blobs in  {bucket.name}/{preffix} succesfully deleted.")


def move_blob(  # pylint:disable=too-many-arguments,too-many-locals
    bucket_name: str,
    blob_name: str,
    destination_bucket: str,
    new_blob_name: str,
    client: Optional[Client] = None,
    if_generation_match: Optional[bool] = None,
    if_generation_not_match: Optional[bool] = None,
    if_metageneration_match: Optional[bool] = None,
    if_metageneration_not_match: Optional[bool] = None,
    if_source_generation_match: Optional[bool] = None,
    if_source_generation_not_match: Optional[bool] = None,
    if_source_metageneration_match: Optional[bool] = None,
    if_source_metageneration_not_match: Optional[bool] = None,
    quiet: bool = False,
) -> Blob:
    """Moves a blob from one bucket to another with a new name."""

    client = require_client(client)
    src_bucket = client.bucket(bucket_name)
    src_blob = src_bucket.blob(blob_name)
    new_bucket = client.bucket(destination_bucket)

    new_blob = src_bucket.copy_blob(
        blob=src_blob,
        destination_bucket=new_bucket,
        new_name=new_blob_name,
        client=client,
        if_generation_match=if_generation_match,
        if_generation_not_match=if_generation_not_match,
        if_metageneration_match=if_metageneration_match,
        if_metageneration_not_match=if_metageneration_not_match,
        if_source_generation_match=if_source_generation_match,
        if_source_generation_not_match=if_source_generation_not_match,
        if_source_metageneration_match=if_source_metageneration_match,
        if_source_metageneration_not_match=if_source_metageneration_not_match,
    )

    if not (src_bucket.name == new_bucket.name and src_blob.name == new_blob.name):
        src_blob.delete(
            client=client,
            if_generation_match=if_source_generation_match,
            if_generation_not_match=if_source_generation_not_match,
            if_metageneration_match=if_source_metageneration_match,
            if_metageneration_not_match=if_source_metageneration_not_match,
        )
    if not quiet:
        LOGGER.info(
            f"Blob {src_blob.name} in bucket {src_bucket.name} moved to blob "
            f"{new_blob.name} in bucket {new_bucket.name}."
        )
    return new_blob


def copy_blob(
    bucket_name: str,
    blob_name: str,
    destination_bucket_name: str,
    destination_blob_name: str,
    client: Optional[Client] = None,
) -> None:
    """Copies a blob from one bucket to another with a new name."""

    storage_client = require_client(client)

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    LOGGER.info(
        f"Blob {source_blob.name} in bucket {source_bucket.name} copied to blob "
        f"{blob_copy.name} in bucket {destination_bucket.name}."
    )


@lru_cache_expiring(maxsize=512, expires=3600)
def get_buckets_list(
    project: str, preffix: str, client: Optional[Client] = None
) -> list:
    """Retunn list of available buckets"""
    storage_client = require_client(client)
    return list(storage_client.list_buckets(prefix=preffix, project=project))
