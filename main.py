"""
Module containing entry points for all services we have developed.
"""


def start_collect_meters_data_dispatcher(*_args, **_kwargs):
    """Metrics connectors dispatcher"""

    from dispatcher.collect_meters_data_dispatcher import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main()


def start_db_load_meters_data_dispatcher(*_args, **_kwargs):
    """Entry point for prototype scheduler dispatcher"""

    from dispatcher.db_load_meters_data_dispatcher import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main()


def start_dw_update_dispatcher(*_args, **_kwargs):
    """Entry point for prototype scheduler dispatcher"""

    from dispatcher.participants_info_update_dispatcher import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main()


def start_export_public_data(*_args, **_kwargs):
    """Entry point for export public data in public.xml"""

    from export_public_data import main  # pylint:disable=import-outside-toplevel

    return main()


def start_openweather_connector(event, context):
    """Entry point for OpenWeather integration."""

    from integration.openweather.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_dbload_connector(event, context):
    """Entry point for DB load integration."""
    from integration.db_load.meters_data_db_load.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_dbrestore_connector(event, context):
    """Entry point for DB restore integration."""

    from integration.db_load.meters_data_db_load.restore import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_dw_update_connector(event, context):
    """Entry point for DB load integration."""
    from integration.db_load.dw_update.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_orion_connector(event, context):
    """Entry point for Orion integration."""

    from integration.orion.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_willow_connector(event, context):
    """Entry point for Willow integration."""

    from integration.willow.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_density_connector(event, context):
    """Entry point for Density integration."""

    from integration.density.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_braxos_connector(event, context):
    """Entry point for Braxos integration."""

    from integration.braxos.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_irisys_connector(event, context):
    """Entry point for Irisys integration."""

    from integration.irisys.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_sourceone_connector(event, context):
    """Entry point for SourceOne integration."""

    from integration.sourceone.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_wattime_marginal_connector(event, context):
    """Entry point for Wattime Marginal Emissions integration."""

    from integration.wattime.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event=event, context=context, integration_type="marginal") or ""


def start_wattime_average_connector(event, context):
    """Entry point for Wattime Average Emissions integration."""

    from integration.wattime.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event=event, context=context, integration_type="average") or ""


def start_nantum_connector(event, context):
    """Entry point for Nantum integration."""

    from integration.nantum.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_ecostruxture_connector(event, context):
    """Entry point for Ecostruxture integration."""

    from integration.ecostruxture.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_ies_mach_connector(event, context):
    """Entry point for IES MACH integration."""

    from integration.ies_mach.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_coned_connector(event, context):
    """Entry point for ConEd integration."""

    from integration.coned.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_xlsx_connector(event, context):
    """Entry point for XLSX integration."""

    from integration.xlsx.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def start_facit_connector(event, context):
    """Entry point for Facit integration"""

    from integration.facit.connector import (
        main,
    )  # pylint:disable=import-outside-toplevel

    return main(event, context) or ""


def handle_request(request):
    """Entry point for site backend. Must be called "handle_request"."""

    from flask import abort  # pylint:disable=import-outside-toplevel

    from integration.coned.backend import ALLOWED_CONED_ENDPOINTS
    from integration.coned.backend import (
        main as coned_main,
    )  # pylint:disable=import-outside-toplevel
    from integration.coned.coned_simulator import CONED_ENDPOINT
    from integration.coned.coned_simulator import (
        main as coned_simulatro_main,
    )  # pylint:disable=import-outside-toplevel

    request_path = request.path.rstrip("/")

    # We connect all URL dispatchers here and run logic until anything will be
    # returned (that means we have a match)
    if request_path in ALLOWED_CONED_ENDPOINTS:
        response = coned_main(request)
        return response

    if request_path == CONED_ENDPOINT:
        response = coned_simulatro_main(request_arg=request)
        return response

    # Add more URL dispatchers here. Ordering them properly will affect
    # performance in a good way.

    # Nothing was returned which means none of our URL dispatchers worked.
    # This is not site nor callback, return 404
    return abort(404)
