# -*- coding: utf-8 -*-

"""Run the query web app with ``python -m indra_cogex.apps.queries_web``.

This allows the queries web application to be run as a standalone module, which will
skip the typical startup activities associated with running the full INDRA-Cogex application.
In this mode, most pages, including the landing page, will not be available, but it's
still possible to navigate directly to the api docs at /apidocs and investigate the API.
"""

from indra_cogex.apps.queries_web.cli import cli

if __name__ == "__main__":
    cli()
