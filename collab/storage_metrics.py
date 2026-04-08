"""
pelican_origin_finder.py

Outer-script functions (requires: Python 3.9, kubectl available on PATH).
Finds pods containing Pelican Origin containers in a given Kubernetes namespace,
then locates the Pelican Server binary inside each such container.
"""

import json
import re
import subprocess
from collections.abc import Generator
from dataclasses import dataclass
from typing import Optional, Sequence

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class Origin:
    """Information about how to exec into an origin container."""

    namespace: str
    pod_name: str
    container_name: str


@dataclass
class Export:
    """A storage prefix/federation prefix combo, plus whether it's public or not."""

    storage_prefix: str
    federation_prefix: str
    public: bool
    size: Optional[int] = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Image name substrings that identify a Pelican Origin container.
ORIGIN_IMAGE_NAMES: tuple[str, ...] = ("osdf-origin", "origin")

# The script to run inside the pod to get usage info
INNER_SCRIPT = "inner.py"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class Error(Exception):
    """Base exception class"""


class InnerScriptError(Error):
    """Something went wrong with the inner script executed inside the container"""


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    """Run a subprocess, capturing stdout/stderr as text."""
    return subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=check,
    )


def _run_in_origin(
    origin: Origin,
    args: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess:

    cmd = [
        "kubectl",
        "exec",
        "--namespace",
        origin.namespace,
        "--container",
        origin.container_name,
        origin.pod_name,
    ]
    return _run(cmd + ["--"] + args, check=check)


def _current_namespace() -> Optional[str]:
    """Return the current Kubernetes namespace"""
    ret = _run(["kubectl", "config", "get-contexts"])
    for line in ret.stdout.splitline():
        if line.startswith("*"):
            namespace = re.split(r"\s+", line)[4]
            return namespace
    return None


def _is_origin_container(container: dict) -> bool:
    """
    Return True if *container* (a Kubernetes container spec dict) looks like a
    Pelican Origin container based on its image name (one of ORIGIN_IMAGE_NAMES).
    """
    full_image: str = container.get("image", "")
    # This assumes that image names always have the registry
    parts = re.split(r"[:@/]", full_image)
    try:
        image = parts[2]
        return image in ORIGIN_IMAGE_NAMES
    except IndexError:
        return False


# ---------------------------------------------------------------------------
# Core public API
# ---------------------------------------------------------------------------


def examine_pod(
    namespace: str,
    pod: dict,
) -> Optional[Origin]:
    """
    Examine a single pod manifest (as returned by the Kubernetes API / kubectl)
    and determine whether it hosts a Pelican Origin container.

    If it does, locate the Pelican Server binary inside that container and
    return a :class:`Origin`.  Returns *None* if the pod does not
    contain a recognised Pelican Origin container or if the binary cannot be
    found.

    Parameters
    ----------
    pod:
        A dict representing the pod's JSON manifest (e.g. from
        ``kubectl get pod <n> -o json``).

    Returns
    -------
    Origin | None
    """
    try:
        pod_name: str = pod["metadata"]["name"]
        containers: list[dict] = pod["spec"]["containers"]
    except KeyError:
        return None

    for container in containers:
        if not _is_origin_container(container):
            continue

        container_name: str = container["name"]

        return Origin(
            namespace=namespace,
            pod_name=pod_name,
            container_name=container_name,
        )

    return None


def find_pelican_origin_pods(namespace: str) -> Generator[Origin]:
    """
    List all pods in *namespace* and return information about every pod that
    contains a Pelican Origin container with a discoverable Pelican Server
    binary.

    Parameters
    ----------
    namespace:
        The Kubernetes namespace to search (e.g. ``"pelican"``).

    Yields
    ------
    Origin
        The info about one pod: the namespace, name, container name, and path
        to pelican-server binary inside the container.

    Raises
    ------
    subprocess.CalledProcessError
        If the initial ``kubectl get pods`` call fails (e.g. bad namespace,
        missing credentials).
    json.JSONDecodeError
        If kubectl returns unexpected output.
    """
    result = _run(
        [
            "kubectl",
            "get",
            "pods",
            "--namespace",
            namespace,
            "--output",
            "json",
        ]
    )

    pod_list: dict = json.loads(result.stdout)
    pods: list[dict] = pod_list.get("items", [])

    for pod in pods:
        info = examine_pod(namespace, pod)
        if info is not None:
            yield info


def get_origin_export_dirs(
    origin: Origin,
) -> list[Export]:
    """
    Run ``<binary> config dump --json`` inside the Origin container and parse
    the exported storage directories, splitting them by access type.

    Two config formats are supported and may both be present simultaneously;
    results from each are merged into the returned lists.

    New-style (``Origin.Exports``):
        Each export entry carries its own ``capabilities`` list.  An entry is
        classified as *public* if ``"PublicReads"`` appears in that list;
        otherwise it is *authenticated*.

    Old-style (``Origin.ExportVolumes``):
        Each element is a string of the form ``"<storage_path>:<federation_path>"``.
        The left-hand side is the storage prefix.  Items where either side is
        not an absolute path (i.e. does not start with ``/``) are ignored.
        All valid storage prefixes share the same access type, determined by
        ``Origin.EnablePublicReads``: ``true`` → public, ``false`` (or absent)
        → authenticated.

    ``Origin.EnablePublicReads`` has no effect on new-style ``Origin.Exports``
    entries.

    Parameters
    ----------
    origin:
        A `Origin` identifying the namespace, pod, container,
        and binary to use.

    Returns
    -------
    list[Export]
        A list of each export.

    Raises
    ------
    subprocess.CalledProcessError
        If the ``kubectl exec`` call fails.
    json.JSONDecodeError
        If the binary does not return valid JSON.
    """
    result = _run_in_origin(
        origin,
        [
            "sh",
            "-c",
            "bin=$(command -v pelican-server 2>/dev/null || command -v osdf-server 2>/dev/null || command -v pelican 2>/dev/null); "
            "$bin config dump --json",
        ],
    )

    config: dict = json.loads(result.stdout)
    origin_cfg: dict = config.get("Origin") or {}

    exports: list[Export] = []

    # ------------------------------------------------------------------
    # New-style: Origin.Exports
    # Each entry carries its own capabilities list.
    # ------------------------------------------------------------------
    for export in origin_cfg.get("Exports") or []:
        try:
            capabilities: list[str] = export["capabilities"]
            storage_prefix: str = export["storageprefix"]
            federation_prefix: str = export["federationprefix"]
        except KeyError:
            # Malformed export
            continue

        exports.append(
            Export(
                storage_prefix=storage_prefix,
                federation_prefix=federation_prefix,
                public=("PublicReads" in capabilities),
            )
        )

    # ------------------------------------------------------------------
    # Old-style: Origin.ExportVolumes
    # Access type is uniform across all entries, governed by
    # Origin.EnablePublicReads.
    # ------------------------------------------------------------------
    export_volumes: list[str] = origin_cfg.get("ExportVolumes") or []
    volumes_are_public: bool = bool(origin_cfg.get("EnablePublicReads"))

    for volume in export_volumes:
        if volume.count(":") != 2:
            # Malformed volume
            continue
        storage_prefix, _, federation_prefix = volume.partition(":")
        if not storage_prefix.startswith("/") or not federation_prefix.startswith("/"):
            continue

        exports.append(
            Export(
                storage_prefix=storage_prefix,
                federation_prefix=federation_prefix,
                public=volumes_are_public,
            )
        )

    return exports


def copy_inner_script_to_origin(origin: Origin):
    """
    Copy the inner script to the temp directory in *origin*
    so it can be run later.
    """
    return _run(
        [
            "kubectl",
            "cp",
            "-c",
            origin.container_name,
            INNER_SCRIPT,
            f'{origin.namespace}/{origin.pod_name}:/tmp/{INNER_SCRIPT}',
        ]
    )


def run_inner_script(origin: Origin, *args: str, copy=True) -> dict:
    """
    Runs the inner script in the pod on the storage path, returning the result.

    Parameters
    ----------
    origin:
        The origin to run the script inside.
    args:
        Arguments to the script.
    copy:
        True if we should copy the inner script first.

    Returns
    -------
    dict
        The results from running the script.

    Raises
    ------
    InnerScriptError
        If something goes wrong with the inner script.
    """
    if copy:
        copy_inner_script_to_origin(origin)
    ret = _run_in_origin(origin, [f"/tmp/{INNER_SCRIPT}"] + list(args), check=False)
    try:
        results = json.loads(ret.stdout)
    except json.JSONDecodeError as err:
        raise InnerScriptError(
            f"inner script failed to return parseable output: {err}.\n"
            f"stdout: {ret.stdout}\n"
            f"stderr: {ret.stderr}\n"
        ) from err

    return results


def get_exports_for_pod(origin: Origin) -> tuple[str, list[Export]]:
    copy_inner_script_to_origin(origin)
    result = run_inner_script(origin, copy=False)
    if result['status'] == "ok":
        return result['sitename'], result['exports']
    else:
        raise InnerScriptError(f"Inner script returned error: {result['error']}")



def interactive_exec(origin: Origin, cmd: Sequence[str] = ("bash",)) -> int:
    """
    Debugging function: interactively exec into an origin to take a look around.

    Parameters
    ----------
    origin
        The origin to exec into

    cmd
        The command to run

    Returns
    -------
    int
        The return code of the process.
    """
    if isinstance(cmd, str):
        cmd = [cmd]
    proc = subprocess.Popen(
        [
            "kubectl",
            "exec",
            "--namespace",
            origin.namespace,
            "--container",
            origin.container_name,
            "-it",
            origin.pod_name,
            "--",
        ]
        + list(cmd)
    )
    return proc.wait()
