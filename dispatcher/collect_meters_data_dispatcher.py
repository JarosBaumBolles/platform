"""
Entry point for dispatcher based on scheduler events.
"""

import json
import uuid
from collections import Counter
from copy import deepcopy

from google.api_core.exceptions import Forbidden
from google.cloud import pubsub_v1

from common import settings as CFG
from common.bucket_helpers import get_buckets_list
from common.data_representation.config.base_exceptions import (
    ConfigException,
    XmlTypeException,
)
from common.data_representation.config.participant import ParticipantConfig
from common.dispatcher import is_cloud_function_exists
from common.elapsed_time import elapsed_timer
from common.logging import Logger


def main():
    """Entry point logic."""

    publisher = pubsub_v1.PublisherClient()

    logger = Logger(
        name="INTEGRATIONS DISPATCHER",
        level="DEBUG",
        description="INTEGRATIONS DISPATCHER",
        trace_id=uuid.uuid4(),
    )

    with elapsed_timer() as global_ellapsed:
        try:
            buckets = get_buckets_list(project=CFG.PROJECT, preffix=CFG.BUCKET_PREFFIX)
            logger.debug(
                "Loaded buckets list.",
                extra={"labels": {"elapsed_time": global_ellapsed()}},
            )
        except Forbidden as err:
            logger.error(f"Can not get buckets list due to the error '{err}'")
            buckets = []

        for bucket in buckets:
            logger.debug(f"Processing {bucket.name}")
            if CFG.DEBUG and bucket.name not in CFG.DEBUG_BUCKETS:
                logger.warning(
                    f"Enabled  DEBUG mode. Disabled {bucket.name} bucket. Skipping"
                )
                continue

            full_config_name = (
                f"{bucket.name}/{CFG.CONFIG_BASE_PATH}/{CFG.PARTICIPANT_CONFIG_NAME}"
            )
            # Entry Point for bucket operation multiprocessing
            participant_id = int(bucket.name.lstrip(CFG.BUCKET_PREFFIX))
            with elapsed_timer() as cnfg_prs_elpsd:
                try:
                    cfg = ParticipantConfig()
                    cfg.read_from_bucket(
                        bucket=bucket.name,
                        subdirectory=CFG.CONFIG_BASE_PATH,
                        filename=CFG.PARTICIPANT_CONFIG_NAME,
                        binary_mode=False,
                    )
                    json_cfg = cfg.as_json()
                except (ConfigException, XmlTypeException) as err:
                    logger.error(
                        f"Cannot parse participant {participant_id} configuration "
                        f"due to the {err}. Skipping."
                    )
                    continue
                except Forbidden as fb_err:
                    logger.error(
                        f"Can not read participant {participant_id} configuration"
                        " {bucket.name}/{CFG.CONFIG_BASE_PATH}/"
                        f"{CFG.PARTICIPANT_CONFIG_NAME} file du to the error "
                        f"'{fb_err}'. Skiping"
                    )
                    continue
                else:
                    logger.debug(
                        f"Completed {full_config_name} parsing.",
                        extra={"labels": {"elapsed_time": cnfg_prs_elpsd()}},
                    )

            if not json_cfg.get("connectors", []):
                logger.warning(
                    f"Parsed participant configuration '{full_config_name}' "
                    f"does not contain defined connectors"
                )
            if CFG.DEBUG:
                cnctrs_cnt = Counter()
            for cnctr_cfg in json_cfg.get("connectors", []):
                cnctr_fnctn = cnctr_cfg.get("function", "")
                if not cnctr_fnctn:
                    logger.warning(
                        f"Unexist connector name in {full_config_name}. " "Skip."
                    )
                    continue

                with elapsed_timer() as gcp_func_ex_elpsd:
                    is_connector_exists = is_cloud_function_exists(cnctr_fnctn)
                    logger.debug(
                        f"Completed check of cloud function {cnctr_fnctn} "
                        "deployment.",
                        extra={"labels": {"elapsed_time": gcp_func_ex_elpsd()}},
                    )
                if not CFG.DEBUG and not is_connector_exists:
                    logger.error(
                        f"The given connector '{cnctr_fnctn}' is not deployed "
                        f"to the '{CFG.PROJECT}' environment. Skipping"
                    )
                    continue

                with elapsed_timer() as payload_ellapsed:
                    payload = _get_meter_payload(cnctr_cfg, participant_id)
                    logger.debug(
                        f"The function {cnctr_fnctn} payload formed.",
                        extra={"labels": {"elapsed_time": payload_ellapsed()}},
                    )

                logger.info(f"Running {cnctr_fnctn}")

                with elapsed_timer() as gcp_func_call_ellapsed:
                    topic_path = publisher.topic_path(  # pylint: disable=no-member
                        CFG.PROJECT, cnctr_fnctn
                    )

                    if CFG.DEBUG:
                        json_cfg = {
                            "data": payload,
                        }
                        file_name = CFG.LOCAL_PATH.joinpath(
                            f"participant_payload_{bucket.name[-1]}_"
                            f"{cnctr_fnctn}_{cnctrs_cnt[cnctr_fnctn]}.json"
                        )
                        with open(file_name, "w", encoding="UTF-8") as prtcpnt_fl:
                            json.dump(json_cfg, prtcpnt_fl, indent=4)
                        cnctrs_cnt[cnctr_fnctn] += 1
                        continue

                    message_json = json.dumps(
                        {
                            "data": payload,
                        }
                    )
                    data = message_json.encode("utf-8")
                    logger.debug(f"TOPIC PATH {topic_path}")
                    future = publisher.publish(topic_path, data=data)
                    result = future.result()
                    logger.debug(
                        f"Data pushed to gcp topic {cnctr_fnctn}. "
                        f"with result {result}",
                        extra={"labels": {"elapsed_time": gcp_func_call_ellapsed()}},
                    )

    logger.debug(
        "Completed loop.", extra={"labels": {"elapsed_time": global_ellapsed()}}
    )
    return "Prototype scheduler executed successfully"


def _get_meter_payload(raw_cnctr_cfg: dict, participant_id: int) -> dict:
    """Generate connector payload

    Args:
        raw_mtr_cfg (dict): Meter full configuration

    Returns:
        dict: Connector payload
    """
    payload = {
        "meters": [],
        "extra": {
            "raw": {
                "bucket": raw_cnctr_cfg.get("rawDataLocation", {}).get("bucket", ""),
                "path": raw_cnctr_cfg.get("rawDataLocation", {}).get("path", ""),
            },
            "participant_id": participant_id,
            "timezone": raw_cnctr_cfg.get("timezone", ""),
            "fetch_strategy": deepcopy(raw_cnctr_cfg.get("fetchStrategy", {})),
        },
    }

    payload.update(raw_cnctr_cfg.get("parameters", {}))

    for meter_name, meter_info in raw_cnctr_cfg.get("meters", {}).items():
        payload["meters"].append(
            {
                "meter_name": meter_name,
                "meter_id": meter_info["meter_id"],
                "meter_uri": meter_info["meter_uri"],
                "type": meter_info["type"],
                "standardized": {
                    "bucket": meter_info.get("meteredDataLocation", {}).get(
                        "bucket", ""
                    ),
                    "path": meter_info.get("meteredDataLocation", {}).get("path", ""),
                },
            }
        )

    return payload


if __name__ == "__main__":

    main()
