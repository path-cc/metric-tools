"""
pelican_origin_finder.py

Outer-script functions (requires: Python 3.9, kubectl available on PATH).
Finds pods containing Pelican Origin containers in a given Kubernetes namespace,
then locates the Pelican Server binary inside each such container.
"""

import json
import subprocess
from dataclasses import dataclass
from typing import Optional

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class PelicanOriginInfo:
    """Result returned for each pod that hosts a Pelican Origin container."""

    pod_name: str
    container_name: str
    binary_path: str


@dataclass
class OriginExportDirs:
    """Storage directories exported by a Pelican Origin, split by access type."""

    public: list[str]
    authenticated: list[str]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Image name substrings that identify a Pelican Origin container.
ORIGIN_IMAGE_NAMES: tuple[str, ...] = ("osdf-origin", "origin")

#: Candidate binary names in preference order.
PELICAN_BINARY_CANDIDATES: tuple[str, ...] = (
    "pelican-server",
    "osdf-server",
    "pelican",
)


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


def _is_origin_container(container: dict) -> bool:
    """
    Return True if *container* (a Kubernetes container spec dict) looks like a
    Pelican Origin container based on its image name (one of ORIGIN_IMAGE_NAMES).
    """
    full_image: str = container.get("image", "")
    # This assumes that image names always have the registry
    parts = re.split(r"[:@/]", full_image)
    image = parts[2]
    return image in ORIGIN_IMAGE_NAMES


def _find_binary_in_container(
    namespace: str,
    pod_name: str,
    container_name: str,
) -> Optional[str]:
    """
    Use ``kubectl exec`` to search for the Pelican Server binary inside
    *container_name* of *pod_name*.

    Candidates are tried in preference order; the first one found via
    ``command -v`` (POSIX built-in, available without any extra packages) is
    returned.  Returns *None* if none of the candidates are found or if the
    exec fails entirely.
    """
    for binary in PELICAN_BINARY_CANDIDATES:
        try:
            result = _run(
                [
                    "kubectl",
                    "exec",
                    pod_name,
                    "--namespace",
                    namespace,
                    "--container",
                    container_name,
                    "--",
                    "sh",
                    "-c",
                    f"command -v {binary}",
                ],
                check=True,
            )
            path = result.stdout.strip()
            if path:
                return path
        except subprocess.CalledProcessError:
            # Binary not found in this container or exec failed — try the next.
            continue

    return None


# ---------------------------------------------------------------------------
# Core public API
# ---------------------------------------------------------------------------


def examine_pod(
    namespace: str,
    pod: dict,
) -> Optional[PelicanOriginInfo]:
    """
    Examine a single pod manifest (as returned by the Kubernetes API / kubectl)
    and determine whether it hosts a Pelican Origin container.

    If it does, locate the Pelican Server binary inside that container and
    return a :class:`PelicanOriginInfo`.  Returns *None* if the pod does not
    contain a recognised Pelican Origin container or if the binary cannot be
    found.

    Parameters
    ----------
    namespace:
        The Kubernetes namespace the pod lives in.
    pod:
        A dict representing the pod's JSON manifest (e.g. from
        ``kubectl get pod <n> -o json``).

    Returns
    -------
    PelicanOriginInfo | None
    """
    pod_name: str = pod["metadata"]["name"]
    containers: list[dict] = pod.get("spec", {}).get("containers", [])

    for container in containers:
        if not _is_origin_container(container):
            continue

        container_name: str = container["name"]
        binary_path = _find_binary_in_container(namespace, pod_name, container_name)

        if binary_path is None:
            # Origin container found but no known binary — skip rather than
            # returning incomplete information.
            continue

        return PelicanOriginInfo(
            pod_name=pod_name,
            container_name=container_name,
            binary_path=binary_path,
        )

    return None


def find_pelican_origin_pods(namespace: str) -> list[PelicanOriginInfo]:
    """
    List all pods in *namespace* and return information about every pod that
    contains a Pelican Origin container with a discoverable Pelican Server
    binary.

    Parameters
    ----------
    namespace:
        The Kubernetes namespace to search (e.g. ``"pelican"``).

    Returns
    -------
    list[PelicanOriginInfo]
        One entry per qualifying pod.  May be empty.

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

    found: list[PelicanOriginInfo] = []
    for pod in pods:
        info = examine_pod(namespace, pod)
        if info is not None:
            found.append(info)

    return found


def get_origin_export_dirs(
    origin: PelicanOriginInfo,
) -> OriginExportDirs:
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
        A `PelicanOriginInfo` identifying the namespace, pod, container,
        and binary to use.

    Returns
    -------
    OriginExportDirs
        Two lists of ``storageprefix`` strings: one for public namespaces and
        one for authenticated namespaces.

    Raises
    ------
    subprocess.CalledProcessError
        If the ``kubectl exec`` call fails.
    json.JSONDecodeError
        If the binary does not return valid JSON.
    """
    result = _run(
        [
            "kubectl",
            "exec",
            origin.pod_name,
            "--namespace",
            origin.namespace,
            "--container",
            origin.container_name,
            "--",
            origin.binary_path,
            "config",
            "dump",
            "--json",
        ]
    )

    config: dict = json.loads(result.stdout)
    origin_cfg: dict = config.get("Origin") or {}

    public: list[str] = []
    authenticated: list[str] = []

    # ------------------------------------------------------------------
    # New-style: Origin.Exports
    # Each entry carries its own capabilities list.
    # ------------------------------------------------------------------
    for export in origin_cfg.get("Exports") or []:
        capabilities: list[str] = export.get("capabilities") or []
        storage_prefix: str = export["storageprefix"]

        if "PublicReads" in capabilities:
            public.append(storage_prefix)
        else:
            authenticated.append(storage_prefix)

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
        storage_path, _, federation_path = volume.partition(":")
        if not storage_path.startswith("/") or not federation_path.startswith("/"):
            continue

        if volumes_are_public:
            public.append(storage_path)
        else:
            authenticated.append(storage_path)

    return OriginExportDirs(public=public, authenticated=authenticated)


def copy_script_to_origin(origin: PelicanOriginInfo):
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


def run_inner_script(origin: PelicanOriginInfo, *args: str) -> dict:
    """
    Runs the inner script in the pod on the storage path, returning the result.
    Script must already have been copied.

    Parameters
    ----------
    origin:
        The origin to run the script inside.
    args:
        Arguments to the script.

    Returns
    -------
    dict
        The results from running the script.

    Raises
    ------
    InnerScriptError
        If something goes wrong with the inner script.
    """
    ret = _run(
        [
            "kubectl",
            "exec",
            "-n",
            origin.namespace,
            "-c",
            origin.container_name,
            origin.pod_name,
            "--",
            f"/tmp/{INNER_SCRIPT}",
        ] + list(args),
        check=False,
    )
    try:
        results = json.loads(ret.stdout)
    except json.JSONDecodeError as err:
        raise InnerScriptError(
            f"inner script failed to return parseable output: {err}.\n"
            f"stdout: {ret.stdout}\n"
            f"stderr: {ret.stderr}\n"
        ) from err

    return results


def interactive_exec(origin: PelicanOriginInfo, cmd: Sequence[str] = ("bash",)) -> int:
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
            "-n",
            origin.namespace,
            "-c",
            origin.container_name,
            "-it",
            origin.pod_name,
            "--",
        ]
        + list(cmd)
    )
    return proc.wait()
