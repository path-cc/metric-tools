import os
import subprocess
from collections.abc import Mapping
from typing import Optional


def _run(
    args: list[str],
    *,
    check: bool = True,
    extra_env: Optional[Mapping[str, Optional[str]]] = None,
) -> subprocess.CompletedProcess:
    """Run a subprocess, capturing stdout/stderr as text.

    If *extra_env* is given, the subprocess environment is a copy of
    ``os.environ`` with those keys overlaid.  Values of ``None`` cause the
    key to be removed from the environment; all other values are converted
    to ``str``.
    """
    env: Optional[dict[str, str]] = None
    if extra_env is not None:
        env = dict(os.environ)
        for k, v in extra_env.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = str(v)
    return subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=check,
        env=env,
    )
