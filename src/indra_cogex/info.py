"""A submodule for getting versioning information about INDRA CoGEx.

Use this module either in the console or Jupyter notebook like:

>>> from indra_cogex.info import env
>>> env()
"""

import os
import sys
from functools import lru_cache
from importlib import metadata
from subprocess import CalledProcessError, check_output  # noqa: S404
from typing import Optional, Tuple

from indra.util.get_version import get_version as get_indra_version


@lru_cache(maxsize=1)
def get_git_hash() -> str:
    """Get the git hash."""
    rv = _run("git", "rev-parse", "HEAD")
    if rv is None:
        return "UNHASHED"
    return rv


@lru_cache(maxsize=1)
def get_git_branch() -> Optional[str]:
    """Get the git branch."""
    return _run("git", "branch", "--show-current")


def _run(*args: str) -> Optional[str]:
    with open(os.devnull, "w") as devnull:
        try:
            ret = check_output(  # noqa: S603,S607
                args,
                cwd=os.path.dirname(__file__),
                stderr=devnull,
            )
        except (CalledProcessError, FileNotFoundError):
            return None
        else:
            return ret.strip().decode("utf-8")


def env_table(tablefmt: str = "github", headers: Tuple[str, str] = ("Key", "Value")) -> str:
    """Generate a table describing the environment in which INDRA CoGEx is being run."""
    import platform
    import time

    from tabulate import tabulate

    indra_version, indra_hash = get_indra_version(with_git_hash=True).split("-")

    rows = [
        ("OS", os.name),
        ("Platform", platform.system()),
        ("Release", platform.release()),
        ("Time", str(time.asctime())),
        ("Python", f"{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}"),
        ("INDRA Version", indra_version),
        ("INDRA Git Hash", indra_hash),
        ("INDRA CoGEx", metadata.version("indra_cogex")),
        ("INDRA CoGEx Git Hash", get_git_hash()),
        ("INDRA CoGEx Branch", get_git_branch()),
    ]
    return tabulate(rows, tablefmt=tablefmt, headers=headers)


def env_html():
    """Output the environment table as HTML for usage in Jupyter."""
    from IPython.display import HTML

    return HTML(env_table(tablefmt="html"))


def env(file=None):
    """Print the env or output as HTML if in Jupyter.

    :param: The file to print to if not in a Jupyter setting. Defaults to sys.stdout
    :returns: A :class:`IPython.display.HTML` if in a Jupyter notebook setting, otherwise none.
    """
    if _in_jupyter():
        return env_html()
    else:
        print(env_table(), file=file)  # noqa:T001


def _in_jupyter() -> bool:
    try:
        get_ipython = sys.modules["IPython"].get_ipython  # type: ignore
        if "IPKernelApp" not in get_ipython().config:
            raise ImportError("console")
        if "VSCODE_PID" in os.environ:
            raise ImportError("vscode")
    except Exception:
        return False
    else:
        return True


if __name__ == '__main__':
    env()
