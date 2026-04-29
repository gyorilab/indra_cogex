# -*- coding: utf-8 -*-

"""Run the trial results web app with ``python -m indra_cogex.apps.trial_results.cli``.

While this doesn't load the full CoGEx web app, it allows the trial results web
application to be run as a standalone module, skipping the typical startup
activities associated with running the full INDRA-CoGEx application.
In this mode, most pages, including the INRA-CoGEx landing page, will not be
available, but it's still possible to navigate directly to the trial results
landing page docs at /trial-results/.
"""

from .cli import cli

if __name__ == "__main__":
    cli()
