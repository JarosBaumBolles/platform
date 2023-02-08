"""Pure coned simulator for local debug"""
import uuid
from http import HTTPStatus

from flask import Flask, abort, redirect, request

from common.logging import Logger

app = Flask(__name__)

LOGGER = Logger(
    name="Coned Simulator",
    level="DEBUG",
    description="Coned Simulator",
    trace_id=uuid.uuid4(),
)

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


@app.route(CONED_ENDPOINT, methods=["GET"])
def get():
    """Entry Point"""
    LOGGER.info("Started request handling")
    request_path = request.path.rstrip("/")
    LOGGER.debug(f"Request path is {request_path}")
    if request_path not in (CONED_ENDPOINT,):
        LOGGER.error(f"Wrong request path {request_path} provided.")
        return abort(
            HTTPStatus.BAD_REQUEST, f"Received Wrong request path {request_path}"
        )

    if not all(x in REQUIERD_CONED_ARGS for x in request.args.keys()):
        LOGGER.error(
            f"Wrong request arguments {list(request.args.keys())} provided."
            f" Expected arguments are {list(REQUIERD_CONED_ARGS.keys())}"
        )
        return abort(
            HTTPStatus.BAD_REQUEST,
            f"Received Wrong request arguments '{list(request.args.keys())}' provided."
            f" Expected arguments are {list(REQUIERD_CONED_ARGS.keys())}",
        )

    for key in request.args.keys():
        value = request.args.get(key)
        expected_value = REQUIERD_CONED_ARGS.get(key, "")
        if value != expected_value:
            LOGGER.error(
                f"Wrong request arguments value {key} - {value} provided."
                f" Expected arguments value is {expected_value}."
            )
            return abort(
                HTTPStatus.BAD_REQUEST,
                f"Wrong request arguments value {key} - {value} provided."
                f" Expected arguments value is {expected_value}.",
            )

    return redirect(REDIRECT_URL)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
