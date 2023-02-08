""" Cleanup Meters data Errors in DataWarehouse"""
import shutil
import time
import uuid
from pathlib import Path

import pandas_gbq
import pydata_google_auth
from tqdm import tqdm

import common.settings as CFG
from common.big_query_utils.utils import get_bq_client
from common.bucket_helpers import get_file_contents
from common.cache import lru_cache_expiring
from common.data_representation.config.meter import MeterConfig
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate
from common.elapsed_time import elapsed_timer
from common.logging import Logger

LCL_BE_STND_PATH = CFG.LOCAL_PATH.joinpath("standardize")
FORCE_DELETE_ALL_BEFORE_DOWNLOAD = False
REWRITE_STANDARDIZED_FILES = False

RIGHT_COLUMNS_ORDER = [
    "ref_participant_id",
    "md_participant_id",
    "ref_property_id",
    "meter_name",
    "time",
    "all_dw_data",
    "meter_latest_value",
    "ref_meter_id",
    "ref_hour_id",
    "meter_uri",
    "bucket",
    "meter_data_bucket",
    "meter_data_path",
    "cnt",
]


UNIQ_KEY = [
    "md_participant_id",
    "meter_name",
    "time",
    "all_dw_data",
    "meter_latest_value",
    "meter_data_bucket",
    "meter_data_path",
]


SCOPES = (
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
)

RUN_DATE = format_date(parse(), "YYYY-MM-DD[T]HH-mm-ss")

REPORT_FILE_NAME = f"meters_data_duplicates_{RUN_DATE}.csv"
REPORT_CLR_FILE_NAME = f"unique_meters_data_duplicates_{RUN_DATE}.csv"

METERS_DT_ERRORS_QUERY = f"""
    WITH artifacts AS 
    (
        SELECT 
            ma.ref_participant_id, 
            ma.ref_property_id,
            ma.ref_meter_id, 
            ma.ref_hour_id, 
            count(md.data) as cnt,
            ARRAY_TO_STRING(
                ARRAY_AGG(
                    CAST(md.data as STRING)
                ), 
                "; "
            ) as all_dw_data,
            ARRAY_TO_STRING(
                ARRAY_AGG(
                    DISTINCT(
                        CAST(md.ref_participant_id as STRING)
                    )
                ), 
                "; "
            ) as md_participant_id
        FROM `{CFG.PROJECT}.{CFG.DATASET}.private_meters_association_deduplication_view_v1` as ma
        INNER JOIN `{CFG.PROJECT}.{CFG.DATASET}.private_meters_data_deduplication_view_v1` AS md
            ON ma.ref_meter_id=md.ref_meter_id
            AND ma.ref_hour_id=md.ref_hour_id

        GROUP BY 
            ma.ref_hour_id, 
            ma.ref_participant_id, 
            ma.ref_property_id,
            ma.ref_meter_id
        HAVING cnt >1
    )

    SELECT ar.*, mt.meter_uri FROM artifacts as ar
    INNER JOIN `{CFG.PROJECT}.{CFG.DATASET}.meters` as mt
        ON mt.meter_id = ar.ref_meter_id
    ORDER BY
        ref_participant_id DESC, 
        ref_property_id DESC,
        ref_meter_id DESC, 
        ref_hour_id DESC
"""


DELETE_METERS_DATA_QUERY_TMPL = """
    DELETE
    FROM `{project}.{dataset}.meters_data`
    WHERE ref_hour_id={ref_hour_id}
        AND ref_participant_id={ref_participant_id}
        AND ref_meter_id={ref_meter_id}
        AND data IN ({data})
"""


@lru_cache_expiring(maxsize=128, expires=900)
def get_google_credentials(scopes=SCOPES) -> None:
    """Get Google Credentials"""
    credentials = pydata_google_auth.get_user_credentials(
        scopes,
        # Set auth_local_webserver to True to have a slightly more convienient
        # authorization flow. Note, this doesn't work if you're running from a
        # notebook on a remote sever, such as over SSH or with Google Colab.
        auth_local_webserver=True,
    )
    return credentials


def df_get_times(row):
    """DataFrame Form date string"""
    return format_date(
        truncate(parse(str(row["ref_hour_id"]), "YYYYMMDDHH"), level="hour"),
        CFG.PROCESSING_DATE_FORMAT,
    )


def df_extract_config_path(row):
    """Get information about meter path and meter name"""
    bucket, _, meter = row["meter_uri"].split("/")
    return bucket, meter


def df_read_meter_cfg(row):
    """DataFrame. Load Meter path and bucket from config"""
    cfg = MeterConfig()
    cfg.read_from_bucket(
        bucket=row["bucket"],
        subdirectory="config",
        filename=row["meter_name"],
        binary_mode=False,
    )

    cfg_data = cfg.as_dataclass()

    return (cfg_data.meteredDataLocation.bucket, cfg_data.meteredDataLocation.path)


def df_download_standardized_files(row):
    """Download standardized files to the local storage"""
    local_path = LCL_BE_STND_PATH.joinpath(
        row["meter_data_bucket"], row["meter_data_path"]
    )

    std_fl = local_path.joinpath(row["time"])

    if REWRITE_STANDARDIZED_FILES or not std_fl.exists():
        std_fl_data = get_file_contents(
            bucket_name=row["meter_data_bucket"],
            blob_path=str(Path(row["meter_data_path"]).joinpath(row["time"])),
        )

        with open(std_fl, "w", encoding="utf-8") as mtr_fl:
            mtr_fl.write(std_fl_data)


def df_read_standardized_files(row):
    """Get latest meter value"""
    std_fl = LCL_BE_STND_PATH.joinpath(
        row["meter_data_bucket"], row["meter_data_path"], row["time"]
    )

    if std_fl.exists() and std_fl.is_file():
        mtr = Meter()
        mtr.read_from_file(str(std_fl))
        mtr_data = mtr.as_dataclass()
        return mtr_data.usage
    return None


def local_environment_config(row):
    """Setu local config"""
    local_path = LCL_BE_STND_PATH.joinpath(
        row["meter_data_bucket"], row["meter_data_path"]
    )
    local_path.mkdir(parents=True, exist_ok=True)


def df_get_superfluous_data(row):
    """DataFrame load superfluous meter values"""
    all_data = list(map(str.strip, row["all_dw_data"].split(";")))
    latest_value = str(row["meter_latest_value"])

    return ", ".join(list(filter(lambda x: x != latest_value, all_data)))


def df_build_delete(row):
    """Build query to delete superfluous meter data from datawarehouse"""
    return DELETE_METERS_DATA_QUERY_TMPL.format(
        project=CFG.PROJECT,
        dataset=CFG.DATASET,
        ref_hour_id=row["ref_hour_id"],
        ref_participant_id=row["md_participant_id"],
        ref_meter_id=row["ref_meter_id"],
        data=row["meter_superfluous_data"],
    )


def main(logger):
    """Loop runner"""
    if FORCE_DELETE_ALL_BEFORE_DOWNLOAD:
        logger.warning(f"Delete {LCL_BE_STND_PATH} before artifact anilizing.")
        if LCL_BE_STND_PATH.exists() and LCL_BE_STND_PATH.is_dir():
            shutil.rmtree(LCL_BE_STND_PATH, ignore_errors=True)
    # ================= CONFIGURATION ========================================

    credentials = get_google_credentials()

    # ================= SEARCHING DATA ARTIFACTS ============================
    logger.info("Analysing DataWarehouse for Meter Data Errors")
    with elapsed_timer() as bq_elapsed:
        ar_df = pandas_gbq.read_gbq(
            METERS_DT_ERRORS_QUERY,
            project_id="production-epbp",
            credentials=credentials,
        )

        ar_df["time"] = ar_df.apply(df_get_times, axis=1, result_type="expand")
        logger.info(
            "Completed DataWarehouse Meter Data Errors Analize",
            extra={"labels": {"elapsed_timer": bq_elapsed()}},
        )

    # ================= METER CONFIGURATION APPENDING ========================
    logger.info("Loading Meters Config Information")
    with elapsed_timer() as mt_elapsed:
        meters_df = ar_df["meter_uri"].to_frame()
        meters_df.drop_duplicates(subset=["meter_uri"], inplace=True)
        meters_df[["bucket", "meter_name"]] = meters_df.apply(
            df_extract_config_path, axis=1, result_type="expand"
        )

        logger.info("Loading Meters location Information")
        meters_df[["meter_data_bucket", "meter_data_path"]] = meters_df.apply(
            df_read_meter_cfg, axis=1, result_type="expand"
        )
        logger.info(
            "Completed Meters Config Information Loading",
            extra={"labels": {"elapsed_timer": mt_elapsed()}},
        )

    # =================== UNION ARTIFACTS AND METERS INFO ====================
    logger.info("Extending Meters Data Errors")
    with elapsed_timer() as union_elapsed:
        ar_df = ar_df.join(meters_df.set_index("meter_uri"), on="meter_uri")
        logger.info(
            "Completed Extending Meters Data Errors",
            extra={"labels": {"elapsed_timer": union_elapsed()}},
        )

    # =================== LOCAL STORAGE PREPARETION ===========================
    logger.info("Preparing local storage")
    with elapsed_timer() as st_elapsed:
        ar_df.apply(local_environment_config, axis=1, result_type="expand")
        logger.info(
            "Completed Local Storage Setup",
            extra={"labels": {"elapsed_timer": st_elapsed()}},
        )

    # =================== DOWNLOADING STANDARDIZE FILES========================
    logger.info(
        "Downloading latest standardized date into local storage"
        f"located at '{LCL_BE_STND_PATH}'"
    )
    with elapsed_timer() as st_dw_elapsed:
        ar_df.apply(df_download_standardized_files, axis=1, result_type="expand")
        logger.info(
            "Completed Downloading latest standardized date into local storage",
            extra={"labels": {"elapsed_timer": st_dw_elapsed()}},
        )
    # =================== PROCESSING STANDARDIZE FILES= ======================
    logger.info(
        "Processing latest standardized date from local storage "
        f"located at '{LCL_BE_STND_PATH}'"
    )
    with elapsed_timer() as st_dw_elapsed:
        ar_df["meter_latest_value"] = ar_df.apply(
            df_read_standardized_files, axis=1, result_type="expand"
        )
        ar_df = ar_df[RIGHT_COLUMNS_ORDER]

        logger.info(
            "Completed standardized data loading",
            extra={"labels": {"elapsed_timer": st_dw_elapsed()}},
        )
    # =================== SAVING FULL ERROR REPORT ========================
    fl_nm = CFG.LOCAL_PATH.joinpath(REPORT_FILE_NAME)
    logger.info(f"Saving full report into '{fl_nm}'" f"located at '{LCL_BE_STND_PATH}'")
    with elapsed_timer() as save_elapsed:
        ar_df.to_csv(str(fl_nm))
        logger.info(
            "The report saved.", extra={"labels": {"elapsed_timer": save_elapsed()}}
        )

    # =================== REMOVE DUPLICATES iN REPORT ========================
    logger.info("Removing duplicates from raw data." f"located at '{LCL_BE_STND_PATH}'")
    with elapsed_timer() as ex_elapsed:
        ar_df_clr = ar_df.drop_duplicates(
            subset=UNIQ_KEY, keep="first", inplace=False, ignore_index=True
        )
        logger.info(
            "Removed duplicates from raw data.",
            extra={"labels": {"elapsed_timer": ex_elapsed()}},
        )

    # =================== SAVING CLEARED FULL ERROR REPORT ========================
    fl_nm = CFG.LOCAL_PATH.joinpath(REPORT_CLR_FILE_NAME)
    logger.info(f"Saving full report into '{fl_nm}'" f"located at '{LCL_BE_STND_PATH}'")
    with elapsed_timer() as save_elapsed:
        ar_df_clr.to_csv(str(fl_nm))
        logger.info(
            "The report saved.", extra={"labels": {"elapsed_timer": save_elapsed()}}
        )

    ar_df_clr["meter_superfluous_data"] = ar_df_clr.apply(
        df_get_superfluous_data, axis=1, result_type="expand"
    )

    ar_df_clr["data_delete_query"] = ar_df_clr.apply(
        df_build_delete, axis=1, result_type="expand"
    )

    with get_bq_client() as conn:
        for query in tqdm(ar_df_clr["data_delete_query"]):
            res = conn.query(query)
            res = res.result()
            time.sleep(0.1)


if __name__ == "__main__":
    CONNECTOR_NAME = "Meter Data Artifacts"
    METERS_AMOUNT = 1
    DBG_LOGGER = Logger(
        name="debug",
        level="DEBUG",
        description="Meter Data Artifacts",
        trace_id=uuid.uuid4(),
    )
    DBG_LOGGER.info(f"Running {CONNECTOR_NAME} report.")
    import debugpy

    debugpy.listen(CFG.DEBUG_PORT)
    debugpy.wait_for_client()  # blocks execution until client is attached
    debugpy.breakpoint()
    with elapsed_timer() as dbg_elapsed:
        main(DBG_LOGGER)
        DBG_LOGGER.info(
            f"Completed {CONNECTOR_NAME} report",
            extra={"labels": {"elapsed_time": dbg_elapsed()}},
        )
