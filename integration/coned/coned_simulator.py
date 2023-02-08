"""Coned redirect logic simulator"""
import uuid
from http import HTTPStatus

from flask import abort, redirect

from common.logging import Logger

DOMAIN = "https://hourlybuildingdata.com"

CONED_ENDPOINT = (
    "/accounts-billing/dashboard/billing-and-usage/"
    "share-my-data-connections/third-party-authorization/redirect"
)

REQUIERD_CONED_ARGS = {
    "client_id": "0oa11x435azYtnB7L0h8",
    "scope": (
        "FB=1_3_6_10_13_14_15_16_28_32_33_35_37_38_41_44;IntervalDuration="
        "Monthly;BlockDuration=Monthly;HistoryLength=63113904;"
        "|FB=1_3_4_5_7_10_13_14_18_32_33_35_37_38_41_44;"
        "IntervalDuration=Monthly_3600_900_300;BlockDuration=Monthly_Daily;"
        "HistoryLength=63113904;|FB=1_3_4_5_7_13_14_18_32_33_35_37_38_41_44;"
        "IntervalDuration=900_300;BlockDuration=Daily;"
        "HistoryLength=86400;|FB=1_3_13_14_46_47;"
    ),
}

REDIRECT_URL = f"{DOMAIN}/test/connect/1/redirect?code=1"


def main(request_arg):
    """Entrypoint"""
    logger = Logger(
        name="Coned Simulator",
        level="DEBUG",
        description="Coned Simulator",
        trace_id=uuid.uuid4(),
    )
    logger.info("Started request handling")
    request_path = request_arg.path.rstrip("/")
    logger.debug(f"Request path is {request_path}")
    if request_path not in (CONED_ENDPOINT,):
        logger.error(f"Wrong request path {request_path} provided.")
        return abort(
            HTTPStatus.BAD_REQUEST, f"Received Wrong request path {request_path}"
        )

    if not all(x in REQUIERD_CONED_ARGS for x in request_arg.args.keys()):
        logger.error(
            f"Wrong request arguments '{list(request_arg.args.keys())}' "
            f"provided. Expected arguments are {list(REQUIERD_CONED_ARGS.keys())}"
        )
        return abort(
            HTTPStatus.BAD_REQUEST,
            "Received Wrong request arguments "
            f"'{list(request_arg.args.keys())}' provided."
            f" Expected arguments are {list(REQUIERD_CONED_ARGS.keys())}",
        )

    for key in request_arg.args.keys():
        value = request_arg.args.get(key)
        expected_value = REQUIERD_CONED_ARGS.get(key, "")
        if value != expected_value:
            logger.error(
                f"Wrong request arguments value {key} - {value} provided."
                f" Expected arguments value is {expected_value}."
            )
            return abort(
                HTTPStatus.BAD_REQUEST,
                f"Wrong request arguments value {key} - {value} provided."
                f" Expected arguments value is {expected_value}.",
            )

    return redirect(REDIRECT_URL)
