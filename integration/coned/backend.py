""" Backend logic for ConEd authentication flow. """

# Standard imports
import datetime
import json
import logging
import uuid
import xml.etree.ElementTree as ET
from re import finditer
from urllib.parse import parse_qs
from urllib.error import HTTPError

# Third-party imports
from flask import abort

# Project imports
from common.bucket_helpers import get_file_contents, upload_file
from common.logging import Logger
from common.request_helpers import HTTPRequestMethod, PayloadType, http_request
from common.settings import (
    CONED_CLIENT_ID as CLIENT_ID,
    CONED_CLIENT_SECRET as CLIENT_SECRET,
    CONED_SUBSCRIPTION_KEY as SUBSCRIPTION_KEY
)


DOMAIN = "https://hourlybuildingdata.com"

REDIRECT_PATH = "/connect/1/redirect"
SCOPE_PATH = "/connect/1/scope"
NOTIFICATION_PATH = "/connect/1/notifications"
AUTHENTICATE_PATH = "/connect/1/authenticate"
AUTH_HELPER_PATH = "/connect/1/auth_helper"
TEST_PREFIX = "/test"
TEST_REDIRECT_PATH = f"{TEST_PREFIX}{REDIRECT_PATH}"
TEST_SCOPE_PATH = f"{TEST_PREFIX}{SCOPE_PATH}"
TEST_NOTIFICATION_PATH = f"{TEST_PREFIX}{NOTIFICATION_PATH}"
TEST_AUTHENTICATE_PATH = f"{TEST_PREFIX}{AUTHENTICATE_PATH}"

ALLOWED_CONED_ENDPOINTS = (
    REDIRECT_PATH,
    SCOPE_PATH,
    NOTIFICATION_PATH,
    TEST_REDIRECT_PATH,
    TEST_SCOPE_PATH,
    TEST_NOTIFICATION_PATH,
    AUTHENTICATE_PATH,
    TEST_AUTHENTICATE_PATH,
    AUTH_HELPER_PATH,
)

# FIXME: TMP
SCOPE = f"""<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="utf-8" />
    <title>Scope Selection</title>
</head>

<body>
    <p>Customize your preferred scope by choosing options below:</p>

    <script>
        function submitScope() {{
            window.location.replace("https://www.coned.com/en/accounts-billing/dashboard/billing-and-usage/share-my-data-connections/third-party-authorization/redirect?client_id={CLIENT_ID}&scope=FB=1_3_6_10_13_14_15_16_28_32_33_35_37_38_41_44;IntervalDuration=Monthly;BlockDuration=Monthly;HistoryLength=63113904;|FB=1_3_4_5_7_10_13_14_18_32_33_35_37_38_41_44;IntervalDuration=Monthly_3600_900_300;BlockDuration=Monthly_Daily;HistoryLength=63113904;|FB=1_3_4_5_7_13_14_18_32_33_35_37_38_41_44;IntervalDuration=900_300;BlockDuration=Daily;HistoryLength=86400;|FB=1_3_13_14_46_47;");
            return true;
        }}
    </script>

    <form onsubmit="event.preventDefault(); submitScope()">
        <div>
            <input type="checkbox" id="consumption_scope" name="consumption_scope" checked>
            <label for="scales">Consumption Scope:
                FB=1_3_6_10_13_14_15_16_28_32_33_35_37_38_41_44;IntervalDuration=Monthly;BlockDuration=Monthly;HistoryLength=63113904;</label>
        </div>
        <div>
            <input type="checkbox" id="billing_scope" name="billing_scope" checked>
            <label for="scales">Billing Information Scope:
                FB=1_3_4_5_7_10_13_14_18_32_33_35_37_38_41_44;IntervalDuration=Monthly_3600_900_300;BlockDuration=Monthly_Daily;HistoryLength=63113904;</label>
        </div>
        <div>
            <input type="checkbox" id="realtime_scope" name="realtime_scope" checked>
            <label for="scales">Realtime Scope:
                FB=1_3_4_5_7_13_14_18_32_33_35_37_38_41_44;IntervalDuration=900_300;BlockDuration=Daily;HistoryLength=86400;</label>
        </div>
        <div>
            <input type="checkbox" id="customer_scope" name="customer_scope" checked>
            <label for="scales">Retail Customer Scope: FB=1_3_13_14_46_47;</label>
        </div>
        <br />
        <input type="submit" value="Request scope">
    </form>
</body>"""


def authenticate(request):
    """Authentification"""
    coned_users = json.loads(
        get_file_contents(
            "production-epbp_participant_0", "config/coned_credentials.json"
        )
    )
    data = request.data.decode("utf-8")
    user = json.loads(data)
    for coned_user in coned_users:
        if (
            coned_user["participant_id"] == int(user["participant_id"])
            and coned_user["password"] == user["password"]
        ):
            return "Success"
    return abort(400)


def process_callback_payload(request, is_testing_environment=False):
    """Callback function to retrive coned payload"""
    parsed_parameters = parse_qs(request.query_string.decode("utf-8"))
    code = parsed_parameters.get("code")
    if code:
        code = code[0]
        # NOTE: Special case for easier auth flow debug as ConEd is quite a
        # complicated thing to test
        if code == "1":
            return json.dumps(
                {
                    "access_token": (
                        "eyJraWQiOiItQnE2QV9vZ20yZVQ2Rk96MWI1Y2NJV1ZvMUo5eWZHWi"
                        "1raUQ1TDA1aGJZIiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0a"
                        "SI6IkFULi1xaXFMNFcyWXhGTWY5UDVyb0ZGbkVLa0pWbElwdTlaVVM2"
                        "Y1BEbTdKNDgub2FyMWh1ZWV0amNpOGkwWlgwaDciLCJpc3MiOiJodH"
                        "RwczovL2NvbmVkLm9rdGFwcmV2aWV3LmNvbS9vYXV0aDIvYXVzOTJ2"
                        "eDVnMzFRaHlyWWYwaDciLCJhdWQiOiJodHRwczovL2FwaXQuY29uZ"
                        "WQuY29tIiwiaWF0IjoxNjQ0NjA0ODk2LCJleHAiOjE2NDQ2MDg0OT"
                        "YsImNpZCI6IjBvYTExeDQzNWF6WXRuQjdMMGg4IiwidWlkIjoiMDB1"
                        "eDFlZDdkcW1JSmxxejYwaDciLCJzY3AiOlsiZGN4LmdiYy5yZWFkX"
                        "2Nvbm5lY3RNeURhdGEiLCJkY3guZ2JjLnJlYWRfbWFuYWdlbWVudFJ"
                        "FU1RTZXJ2aWNlcyIsIm9mZmxpbmVfYWNjZXNzIiwiZGN4LmdiYy5yZ"
                        "WFkX3JldGFpbEN1c3RvbWVyQnVsayIsImRjeC5nYmMucmVhZF9wd"
                        "XREZWxldGVBdXRob3JpemF0aW9uIiwiZGN4LmdiYy5yZWFkX3Jlc"
                        "291cmNlTGV2ZWxSRVNUIiwiZGN4LmdiYy5yZWFkX3B1dERlbGV0ZU"
                        "FwcGxpY2F0aW9uSW5mb3JtYXRpb25SZXNvdXJjZSIsImRjeC5nYmM"
                        "ucmVhZF9SRVNUZm9yQnVsayIsImRjeC5nYmMucmVhZF9yZXRhaWxD"
                        "dXN0b21lciJdLCJzdWIiOiJrbm93bGVzc0Bjb25lZC5jb20iLCJhY"
                        "2NvdW50cyI6WyIxMDAwMDEzMTgyNzE7OzAiLCIxMDAwMDI1MjcxN"
                        "Tg7OzEiXX0.UhXFNvdC0zx92at5FPfgknDu0dUzrUj3xjjBWI4XJP"
                        "9kwYlpFZQ1MfJ6ShY6KzLQp3UTT9I0tJEpalxCLaUa18QQJRupF7M"
                        "iOmgdUKqugP0Da0xYL5P-CJrpCOvlmGT4Mov-kY2oWRutXyz_sjCKH"
                        "oEmHsPKKYVDVLbXpoGPooSvZaQKLbZ8biIQqqcZupOvuhR2ZFpNUY"
                        "gIwjU_GXBi74pdbFUHWgyS_aip_4rtWh--GQnzrGvHS7ZUI6Gp9uo"
                        "dkxC50MhpuxKCbvF79T1rtP-bAVW-dAzBqFA81OZJIBcwma6r_nq"
                        "XrmCXo6IQipfWgwiy6of4FKvkF1w97PAAAA"
                    ),
                    "refresh_token": "y__B921veKgsxBrHJylOi4qX6xVpGXY8ItTeKSn1111",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": (
                        "FB=1_3_4_5_7_10_13_14_18_32_33_35_37_38_41_44;"
                        "IntervalDuration=Monthly_3600_900_300;"
                        "BlockDuration=Monthly_Daily;"
                        "HistoryLength=63113904; "
                        "FB=1_3_6_10_13_14_15_16_28_32_33_35_37_38_41_44;"
                        "IntervalDuration=Monthly;"
                        "BlockDuration=Monthly;"
                        "HistoryLength=63113904; "
                        "FB=1_3_4_5_7_13_14_18_32_33_35_37_38_41_44;"
                        "IntervalDuration=900_300;"
                        "BlockDuration=Daily;"
                        "HistoryLength=86400; "
                        "FB=1_3_13_14_46_47;"
                    ),
                    "resourceURI": (
                        "https://apit.coned.com/gbc/v1/resource/"
                        "Batch/Subscription/976"
                    ),
                    "authorizationURI": (
                        "https://apit.coned.com/gbc/v1/" "resource/Authorization/1482"
                    ),
                    "accountNumber": "NTg4ODA3MDQ5MDAw1111",
                }
            )

        response = http_request(
            (
                f'https://{"apit" if is_testing_environment else "api"}.'
                "coned.com/gbc/v1/oauth/v1/Token"
            ),
            payload={
                "grantType": "authorization_code",
                "clientId": CLIENT_ID,
                "clientSecret": CLIENT_SECRET,
                "redirectUri": (
                    f"{DOMAIN}{'/test' if is_testing_environment else ''}/"
                    "connect/1/redirect"
                ),
                "authCode": code,
            },
            headers={
                "ocp-apim-subscription-key": SUBSCRIPTION_KEY,
                "Content-Type": "application/json",
            },
            method=HTTPRequestMethod.POST,
            request_payload_type=PayloadType.JSON,
        )

        if not is_testing_environment:
            upload_file(
                "production-epbp_participant_0",
                response,
                "coned_auth_responses",
                datetime.datetime.utcnow().isoformat(),
            )
            return "ConEd authorization succeeded! Thank You!"

        return response
    if parsed_parameters.get("error"):
        return abort(
            500,
            f"Received OAuth authentication error \"{parsed_parameters['error'][0]}\"...",
        )
    return abort(
        400,
        "Registering OAuth authentication code, expected URL parameter '?code' or '?error'...",
    )


def main(request):
    """Entry Point"""
    logger = Logger(
        name="Coned",
        level="DEBUG",
        description="Coned",
        trace_id=uuid.uuid4(),
    )
    logger.info("Started request handling")
    request_path = request.path.rstrip("/")
    logger.debug(f"Request path is {request_path}")
    if request_path in (SCOPE_PATH, TEST_SCOPE_PATH):
        return SCOPE
    if request_path in (AUTHENTICATE_PATH, TEST_AUTHENTICATE_PATH):
        return authenticate(request)
    if request_path in (REDIRECT_PATH, TEST_REDIRECT_PATH):
        logger.debug("Obtaining access tokens for ConEd.")
        return process_callback_payload(request, request_path == TEST_REDIRECT_PATH)
    if request_path == AUTH_HELPER_PATH:
        refresh_token = request.form["refresh_token"]
        subscription_id = request.form["subscription_id"]

        response = []

        try:
            payload = {
                "grantType": "refresh_token",
                "clientId": CLIENT_ID,
                "clientSecret": CLIENT_SECRET,
                "refreshToken": refresh_token,
                "subscriptionId": subscription_id,
            }
            headers = {
                "ocp-apim-subscription-key": SUBSCRIPTION_KEY,
                "Content-Type": "application/json",
            }
            logger.debug(str(headers))
            api_response = http_request(
                "https://api.coned.com/gbc/v1/oauth/v1/Token",
                payload=payload,
                headers=headers,
                method=HTTPRequestMethod.POST,
                request_payload_type=PayloadType.JSON,
                response_payload_type=PayloadType.JSON,
            )

            headers = {
                "ocp-apim-subscription-key": SUBSCRIPTION_KEY,
                "Content-Type": "application/atom+xml",
                "Accept": "application/atom+xml",
            }
            headers["Authorization"] = f'Bearer {api_response["access_token"]}'

            usage_points_response = http_request(
                (
                    "https://api.coned.com/gbc/v1/resource/Subscription"
                    f"/{subscription_id}/UsagePoint"
                ),
                headers=headers,
            )

            response.append("[OK]: Auth data is valid")
        except HTTPError as error:
            return f"[ERROR]: Either refresh token or subscription ID is invalid. Received code: {error.code}, with message: {error.reason}"

        root = ET.fromstring(usage_points_response)
        response.append("USAGE POINTS:")
        for usage_point in root.findall(
            './/{http://www.w3.org/2005/Atom}entry//{http://www.w3.org/2005/Atom}link[@rel="self"]'
        ):
            usage_point_response = http_request(
                usage_point.attrib["href"] + "/MeterReading", headers=headers
            )
            usage_point_root = ET.fromstring(usage_point_response)

            usage_point_data = ""
            root = usage_point_root.findall(
                ".//{http://www.w3.org/2005/Atom}entry//"
                '{http://www.w3.org/2005/Atom}link[@rel="self"]'
            )
            for usage_point_details in root:
                usage_point_data += (
                    "Usage Point ID: <b>"
                    + next(
                        finditer(
                            ".*/UsagePoint/([0-9]+)/.*",
                            usage_point_details.attrib["href"],
                        )
                    ).groups()[0]
                    + "</b>; "
                )
                try:
                    usage_point_data += (
                        "Meter Reading: <b>"
                        + next(
                            finditer(
                                ".*/MeterReading/([0-9a-zA-Z_]+)$",
                                usage_point_details.attrib["href"],
                            )
                        ).groups()[0]
                        + "</b>; "
                    )
                except StopIteration:
                    usage_point_data += "Meter Reading: <b>No meter readings detected...</b>"
                break

            usage_root = usage_point_root.findall(
                ".//{http://www.w3.org/2005/Atom}entry//"
                '{http://www.w3.org/2005/Atom}link[@rel="related"]'
            )
            for usage_point_details in usage_root:
                if "KWH" in usage_point_details.attrib["href"]:
                    usage_point_data += (
                        "Reading Interval: <b>"
                        + next(
                            finditer(
                                ".*KWH%20([0-9]+)%20Minute.*",
                                usage_point_details.attrib["href"],
                            )
                        ).groups()[0]
                        + "</b>"
                    )
                    break

            response.append(usage_point_data)

        usage_points_address_response = http_request(
            f"https://api.coned.com/gbc/v1/resource/Customer/{subscription_id}",
            headers=headers,
        )

        root = ET.fromstring(usage_points_address_response)
        response.append("")
        response.append("ADDRESSES OF USAGE POINTS:")
        for usage_point_address in root.findall('.//{http://naesb.org/espi/customer}mainAddress'):
            (usage_point_address[0].text, usage_point_address[1].text, usage_point_address[2].text, usage_point_address[3].text)

            response.append(
                '\t<b>'
                f'{usage_point_address[0].text} '
                f'{usage_point_address[1].text} '
                f'{usage_point_address[2].text} '
                f'{usage_point_address[3].text}'
                '</b>'
            )

        return "<br/>".join(response)
    if request_path in (TEST_NOTIFICATION_PATH, NOTIFICATION_PATH):
        logging.debug("DEBUG: Received batch notification from ConEd.")
    return 'Received notification'
