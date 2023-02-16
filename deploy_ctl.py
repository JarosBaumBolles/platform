#!/usr/bin/env python3
"""
Deployment and maintenance utility for HourlyBuildingDataPlatform.
Use cases:
- Validate sources with different utilities
- Perform deployment in parallel of different services
- Clear development environment of old data
"""

import argparse
import multiprocessing.pool
import pathlib
import subprocess
import sys
import uuid

from common import settings
from common.logging import Logger

# TODO: Parallel mode (no errors reported but very fast)
# from concurrent.futures.thread import ThreadPoolExecutor

BASE_SOURCE_URL = (
    "https://source.developers.google.com/projects/develop-epbp/"
    "repos/HourlyBuildingDataPlatform/moveable-aliases"
)

# BASE_SOURCE_URL = (
#     "https://source.cloud.google.com/develop-epbp/HourlyBuildingDataPlatform"
# )

LOGGER = Logger(
    name="DEPLOYMENT",
    level="DEBUG",
    description="DEPLOYMENT",
    trace_id=uuid.uuid4(),
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manage project deployment and perform maintenance"
    )
    parser.add_argument(
        "-B",
        "--branch",
        type=str,
        dest="branch",
        nargs="?",
        default="master",
        help="Use specific branch for deployment instead of master",
    )
    parser.add_argument(
        "-E",
        "--environment",
        type=str,
        dest="environment",
        choices=["develop", "production"],
        default="develop",
        help="Specify environment for deployment",
    )
    parser.add_argument(
        "-A",
        "--analysis",
        dest="perform_analysis",
        action="store_true",
        default=False,
        help="Analyse sources using tools: pylint, pycodestyle, safety, bandit, etc",
    )
    parser.add_argument(
        "-S",
        "--site",
        dest="site",
        action="store_true",
        default=False,
        help="Specify whether to deploy site",
    )
    parser.add_argument(
        "-I",
        "--integrations",
        type=str,
        nargs="*",
        choices=[
            "openweather",
            "orion",
            "willow",
            "density",
            "dbload",
            "dw_update",
            "braxos",
            "irisys",
            "sourceone",
            "wattime_marginal",
            "wattime_average",
            "nantum",
            "ies_mach",
            "xlsx",
            "ecostruxture",
            "coned",
            "facit"
        ],
        help="Specify target integrations to deploy",
    )
    parser.add_argument(
        "-D",
        "--dispatcher",
        dest="dispatcher",
        action="store_true",
        default=False,
        help="Specify whether to deploy dispatcher",
    )

    parser.add_argument(
        "-DB",
        "--db-load-dispatcher",
        dest="db_load_dispatcher",
        action="store_true",
        default=False,
        help="Specify whether to deploy db load dispatcher ",
    )

    parser.add_argument(
        "-DW",
        "--dw-update-dispatcher",
        dest="dw_update_dispatcher",
        action="store_true",
        default=False,
        help="Specify whether to deploy dw update dispatcher ",
    )

    parser.add_argument(
        "-EP",
        "--export-public-csv",
        dest="export_public_csv",
        action="store_true",
        default=False,
        help="Specify whether to deploy public data exporter ",
    )

    parser.add_argument(
        "-RE",
        "--reset-environment",
        dest="clear_environment",
        action="store_true",
        default=False,
        help="Reset development environment (delete collected data and recreate database schema)",
    )
    parser.add_argument(
        "-AS",
        "--all-services",
        dest="deploy_all_services",
        action="store_true",
        default=False,
        help="Deploy all services",
    )

    arguments = parser.parse_args()

    if arguments.perform_analysis:
        try:
            LOGGER.debug(
                subprocess.check_output(
                    f"{sys.executable} -m compileall . -q -e .", shell=True  # nosec
                ).decode("utf-8")
            )
        except subprocess.CalledProcessError as error:
            if error.returncode != 0:
                LOGGER.error(error.stdout.decode("utf-8"))
                LOGGER.error(
                    "Python syntax validation found issues...", file=sys.stderr
                )
                sys.exit(1)

        try:
            LOGGER.debug(
                subprocess.check_output(
                    f"{sys.executable} -m pip check", shell=True  # nosec
                ).decode("utf-8")
            )
        except subprocess.CalledProcessError as error:
            if error.returncode != 0:
                LOGGER.error(error.stdout.decode("utf-8"))
                LOGGER.debug(
                    "PIP dependency compatibility scan found issues...", file=sys.stderr
                )
                sys.exit(1)

        try:
            LOGGER.debug(
                subprocess.check_output(
                    (
                        f"PYTHONPATH=."
                        " "
                        f"{sys.executable}"
                        " "
                        "-m"
                        " "
                        "pylint"
                        " "
                        "--rcfile pylintrc"
                        " "
                        f"\"{str(pathlib.Path('.').parent.absolute()).rsplit('/', maxsplit=1)[-1]}\""  # pylint:disable=line-too-long
                    ),
                    shell=True,  # nosec
                ).decode("utf-8")
            )
        except subprocess.CalledProcessError as error:
            if error.returncode != 0:
                LOGGER.error(error.stdout.decode("utf-8"))
                LOGGER.debug(
                    "PyLint source validation found issues...", file=sys.stderr
                )
                sys.exit(1)

        try:
            LOGGER.debug(
                subprocess.check_output(
                    f"{sys.executable} -m safety check", shell=True  # nosec
                ).decode("utf-8")
            )
        except subprocess.CalledProcessError as error:
            if error.returncode != 0:
                LOGGER.error(error.stdout.decode("utf-8"))
                LOGGER.debug(
                    "Safety package vulnerability scan found issues...", file=sys.stderr
                )

        try:
            LOGGER.debug(
                subprocess.check_output(
                    f"{sys.executable} -m bandit -r .", shell=True  # nosec
                ).decode("utf-8")
            )
        except subprocess.CalledProcessError as error:
            if error.returncode != 0:
                LOGGER.error(error.stdout.decode("utf-8"))
                LOGGER.debug(
                    "Bandit source vulnerability scan found issues...", file=sys.stderr
                )

    PROJECT = f"{arguments.environment}-epbp"
    PUSH_INTEGRATIONS = (
        "ecostruxture",
        "orion",
        "sourceone",
        "braxos",
        "dbload",
        "xlsx",
    )

    THREAD_POOL = multiprocessing.pool.ThreadPool(10)

    if arguments.deploy_all_services or arguments.integrations:
        for integration in settings.Integration:
            if arguments.deploy_all_services or (
                arguments.integrations and integration.value in arguments.integrations
            ):
                # IMPORTANT: GCP service user name is limited to 30 characters
                #            and does not allow "_" symbol.
                #            This change is for specific case of wattime integration
                #            that is split into two connectors but operates on
                #            the same bucket root directory. Basically it is one
                #            service user but split in two connectors because
                #            of integration differences and logic complexity.

                SERVICE_USER = f'cloud-functions-{integration.value.split("_")[0]}'
                TOPIC = f"connector_{integration.value}"

                memory = f"{'4096MB' if integration.value in PUSH_INTEGRATIONS else '2048MB'}"

                THREAD_POOL.apply_async(
                    lambda command: subprocess.check_call(command, shell=True),  # nosec
                    args=(
                        (
                            f"gcloud iam service-accounts create {SERVICE_USER} --display-name='Service user for {integration.value} integration' --project {PROJECT}; "
                            f"gcloud pubsub topics delete {TOPIC} --project {PROJECT}; "
                            f"gcloud pubsub topics create {TOPIC} --message-retention-duration=1h --message-storage-policy-allowed-regions=us-east4 --project {PROJECT}; "
                            "gcloud functions deploy "
                            f"connector_{integration.value} "
                            f"--service-account {SERVICE_USER}@{arguments.environment}-epbp.iam.gserviceaccount.com "  # pylint:disable=line-too-long
                            f"--source {BASE_SOURCE_URL}/{arguments.branch}/paths/ "
                            f"--trigger-topic {TOPIC} "
                            "--runtime python39 "
                            f"--memory {memory} "
                            "--timeout 540 "
                            "--region us-east4 "
                            "--no-allow-unauthenticated "
                            f"--set-env-vars PROJECT={PROJECT} "
                            "--ingress-settings=internal-and-gclb "
                            f"--project {PROJECT} "
                            f"--entry-point start_{integration.value}_connector"
                        ),
                    ),
                )

    if arguments.deploy_all_services or arguments.dispatcher:
        SERVICE_USER = "integration-dispatcher"
        TOPIC = "dispatcher"
        THREAD_POOL.apply(
            lambda command: subprocess.check_call(command, shell=True),  # nosec
            args=(
                (
                    f"gcloud iam service-accounts create {SERVICE_USER} --display-name='Integration dispatcher service user' --project {PROJECT}; "
                    f"gcloud pubsub topics delete {TOPIC} --project {PROJECT}; "
                    f"gcloud pubsub topics create {TOPIC} --message-retention-duration=1h --message-storage-policy-allowed-regions=us-east4 --project {PROJECT}; "
                    "gcloud functions deploy "
                    f"dispatcher "
                    f"--service-account {SERVICE_USER}@{arguments.environment}-epbp.iam.gserviceaccount.com "
                    f"--source {BASE_SOURCE_URL}/{arguments.branch}/paths/ "
                    f"--trigger-topic {TOPIC} "
                    "--runtime python39 "
                    "--memory 4096MB "
                    "--timeout 540 "
                    "--region us-east4 "
                    "--no-allow-unauthenticated "
                    f"--set-env-vars PROJECT={PROJECT} "
                    "--ingress-settings=internal-and-gclb "
                    f"--project {PROJECT} "
                    "--entry-point start_collect_meters_data_dispatcher"
                ),
            ),
        )

    if arguments.deploy_all_services or arguments.export_public_csv:
        SERVICE_USER = "export-public-csv"
        TOPIC = "export_csv"
        THREAD_POOL.apply(
            lambda command: subprocess.check_call(command, shell=True),  # nosec
            args=(
                (
                    f"gcloud iam service-accounts create {SERVICE_USER} --display-name='Export Public Data service user' --project {PROJECT}; "
                    f"gcloud pubsub topics delete {TOPIC} --project {PROJECT}; "
                    f"gcloud pubsub topics create {TOPIC} --message-retention-duration=1h --message-storage-policy-allowed-regions=us-east4 --project {PROJECT}; "
                    "gcloud functions deploy "
                    f"export_public_csv "
                    f"--service-account {SERVICE_USER}@{arguments.environment}-epbp.iam.gserviceaccount.com "
                    f"--source {BASE_SOURCE_URL}/{arguments.branch}/paths/ "
                    f"--trigger-topic {TOPIC} "
                    "--runtime python39 "
                    "--memory 4096MB "
                    "--timeout 540 "
                    "--region us-east4 "
                    "--no-allow-unauthenticated "
                    f"--set-env-vars PROJECT={PROJECT} "
                    "--ingress-settings=internal-and-gclb "
                    f"--project {PROJECT} "
                    "--entry-point start_export_public_data"
                ),
            ),
        )

    if arguments.deploy_all_services or arguments.dw_update_dispatcher:
        SERVICE_USER = "dw-update-dispatcher"
        TOPIC = "dw_update_dispatcher"
        THREAD_POOL.apply(
            lambda command: subprocess.check_call(command, shell=True),  # nosec
            args=(
                (
                    f"gcloud iam service-accounts create {SERVICE_USER} --display-name='Database dispatcher service user' --project {PROJECT}; "
                    f"gcloud pubsub topics delete {TOPIC} --project {PROJECT}; "
                    f"gcloud pubsub topics create {TOPIC} --message-retention-duration=1h --message-storage-policy-allowed-regions=us-east4 --project {PROJECT}; "
                    "gcloud functions deploy "
                    f"dw_update_dispatcher "
                    f"--service-account {SERVICE_USER}@{arguments.environment}-epbp.iam.gserviceaccount.com "
                    f"--source {BASE_SOURCE_URL}/{arguments.branch}/paths/ "
                    f"--trigger-topic {TOPIC} "
                    "--runtime python39 "
                    "--memory 4096MB "
                    "--timeout 540 "
                    "--region us-east4 "
                    "--no-allow-unauthenticated "
                    f"--set-env-vars PROJECT={PROJECT} "
                    "--ingress-settings=internal-and-gclb "
                    f"--project {PROJECT} "
                    "--entry-point start_dw_update_dispatcher"
                ),
            ),
        )

    if arguments.deploy_all_services or arguments.db_load_dispatcher:
        SERVICE_USER = "database-dispatcher"
        TOPIC = "dbload_dispatcher"
        THREAD_POOL.apply(
            lambda command: subprocess.check_call(command, shell=True),  # nosec
            args=(
                (
                    f"gcloud iam service-accounts create {SERVICE_USER} --display-name='Database dispatcher service user' --project {PROJECT}; "
                    f"gcloud pubsub topics delete {TOPIC} --project {PROJECT}; "
                    f"gcloud pubsub topics create {TOPIC} --message-retention-duration=1h --message-storage-policy-allowed-regions=us-east4 --project {PROJECT}; "
                    "gcloud functions deploy "
                    f"db_load_dispatcher "
                    f"--service-account {SERVICE_USER}@{arguments.environment}-epbp.iam.gserviceaccount.com "
                    f"--source {BASE_SOURCE_URL}/{arguments.branch}/paths/ "
                    f"--trigger-topic {TOPIC} "
                    "--runtime python39 "
                    "--memory 4096MB "
                    "--timeout 540 "
                    "--region us-east4 "
                    "--no-allow-unauthenticated "
                    f"--set-env-vars PROJECT={PROJECT} "
                    "--ingress-settings=internal-and-gclb "
                    f"--project {PROJECT} "
                    "--entry-point start_db_load_meters_data_dispatcher"
                ),
            ),
        )

    if arguments.deploy_all_services or arguments.site:
        THREAD_POOL.apply(
            lambda command: subprocess.check_call(command, shell=True),  # nosec
            args=(
                (
                    "gcloud functions deploy "
                    "web-backend "
                    f"--service-account {arguments.environment}-epbp@appspot.gserviceaccount.com "
                    f"--source {BASE_SOURCE_URL}/{arguments.branch}/paths/ "
                    "--trigger-http "
                    "--runtime python39 "
                    "--memory 2048MB "
                    "--region us-east4 "
                    "--allow-unauthenticated "
                    f"--project {PROJECT} "
                    # "--vpc-connector gcp-connector "
                    "--timeout 540 "                    
                    "--ingress-settings=internal-and-gclb "
                    # "--egress-settings all "
                    "--entry-point handle_request"
                ),
            ),
        )


    THREAD_POOL.close()
    THREAD_POOL.join()
