"""Coned proxy object"""

import uuid
from common.logging import Logger
from flask import abort
import requests
from common.request_helpers import (
    HTTPRequestMethod,
    PayloadType,
    http_request,
    JBBRequestHelperException,
)

PROXY_ENDPOINTS = [
    "/coned/auth/token",
]

LOGER = Logger(
    description="Coned Proxy", 
    trace_id=uuid.uuid4()
)

def refresh_token(request):
    """Get refresh Token from """
    LOGER.info("Recieved refresh token request")
    LOGER.debug(
        f"Request params: {request.args} "
    )
    LOGER.debug(
        f"Request headers: {request.headers} "
    )

    LOGER.info("Retrieving refresh token 2")

    # result = requests.post(
    #     url="https://api.coned.com/gbc/v1/oauth/v1/Token",
    #     json = {
    #         "grantType": request.args.get("grantType"),
    #         "clientId": request.args.get("clientId"),
    #         "clientSecret": request.args.get("clientSecret"),
    #         "refreshToken": request.args.get("refreshToken"),
    #         "subscriptionId": request.args.get("subscriptionId"),
    #     },
    #     headers={
    #         "ocp-apim-subscription-key": request.headers.get("ocp-apim-subscription-key"),
    #         "Content-Type": request.headers.get("Content-Type"),
    #     }
    # )    


    payload = {
        "grantType": request.args.get("grantType"),
        "clientId": request.args.get("clientId"),
        "clientSecret": request.args.get("clientSecret"),
        "refreshToken": request.args.get("refreshToken"),
        "subscriptionId": request.args.get("subscriptionId"),
    }
    headers = {
        "ocp-apim-subscription-key": request.headers.get("ocp-apim-subscription-key"),
        "Content-Type": request.headers.get("Content-Type"),
    }

    LOGER.info(f"Payload is - {payload}")
    LOGER.info(f"Headers is - {headers}")

    response = http_request(
        "https://api.coned.com/gbc/v1/oauth/v1/Token",
        payload=payload,
        headers=headers,
        method=HTTPRequestMethod.POST,
        request_payload_type=PayloadType.JSON,
        response_payload_type=PayloadType.JSON,
    )        

    LOGER.info(
        f"Recieved token response. Status - {result.status_code}; Respons {result.json()}"
    )
    return "Success"