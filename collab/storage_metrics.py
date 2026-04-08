#!/usr/bin/env python3
"""
pelican_origin_finder.py

Outer-script functions (requires: Python 3.9, kubectl available on PATH).
Finds pods containing Pelican Origin containers in a given Kubernetes namespace,
then locates the Pelican Server binary inside each such container.
"""

import argparse
import configparser
import dataclasses
import json
import re
import subprocess
import sys
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
    context: str


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
        "--context",
        origin.context,
        "exec",
        "--namespace",
        origin.namespace,
        "--container",
        origin.container_name,
        origin.pod_name,
    ]
    return _run(cmd + ["--"] + args, check=check)


def _check_namespace_access(cluster: str, context: str, namespace: str) -> bool:
    """
    Return True if we have permission to get pods and exec into pods in *namespace*.
    Prints an error to stderr and returns False if either check fails.
    """
    checks = [
        [
            "kubectl",
            "--context",
            context,
            "auth",
            "can-i",
            "get",
            "pods",
            "--namespace",
            namespace,
        ],
        [
            "kubectl",
            "--context",
            context,
            "auth",
            "can-i",
            "create",
            "pods/exec",
            "--namespace",
            namespace,
        ],
    ]
    for cmd in checks:
        ret = _run(cmd, check=False)
        if ret.returncode != 0 or ret.stdout.strip() != "yes":
            print(
                f"ERROR: insufficient permissions in cluster={cluster!r} "
                f"namespace={namespace!r}: {' '.join(cmd[5:])} -> "
                f"{ret.stdout.strip() or ret.stderr.strip()}",
                file=sys.stderr,
            )
            return False
    return True


def _current_context() -> str:
    """Return the current Kubernetes context, or raise Error if it cannot be determined."""
    ret = _run(["kubectl", "config", "current-context"])
    context = ret.stdout.strip()
    if not context:
        raise Error("Could not determine current Kubernetes context")
    return context


def _namespace_for_context(context: Optional[str] = None) -> str:
    """
    Return the namespace configured for *context*, or raise Error if it cannot
    be determined.  If *context* is not given, uses the current context.
    """
    if context is None:
        context = _current_context()
    ret = _run(["kubectl", "config", "get-contexts", "--no-headers", context])
    for line in ret.stdout.splitlines():
        parts = re.split(r"\s+", line.strip())
        # Strip leading '*' marker for the active context
        if parts and parts[0] == "*":
            parts = parts[1:]
        # Columns: NAME CLUSTER AUTHINFO [NAMESPACE]
        if len(parts) >= 4:
            return parts[3]
    raise Error(f"Could not determine namespace for context {context!r}")


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


def _printflush(*args, **kwargs):
    """Print and flush immediately."""
    file = kwargs.pop("file", sys.stdout)
    print(*args, **kwargs, file=file)
    file.flush()


# ---------------------------------------------------------------------------
# Core public API
# ---------------------------------------------------------------------------


def examine_pod(
    pod: dict,
    context: Optional[str] = None,
    namespace: Optional[str] = None,
) -> Optional[Origin]:
    """
    Examine a single pod manifest (as returned by the Kubernetes API / kubectl)
    and determine whether it hosts a Pelican Origin container.

    If it does, return a :class:`Origin`.  Returns *None* if the pod does not
    contain a recognised Pelican Origin container.

    Parameters
    ----------
    pod:
        A dict representing the pod's JSON manifest (e.g. from
        ``kubectl get pod <n> -o json``).
    context:
        The Kubernetes context.  Defaults to the current context.
    namespace:
        The Kubernetes namespace.  Defaults to the namespace configured for
        *context*.

    Returns
    -------
    Origin | None
    """
    if context is None:
        context = _current_context()
    if namespace is None:
        namespace = _namespace_for_context(context)

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
            context=context,
        )

    return None


def find_pelican_origin_pods(
    context: Optional[str] = None,
    namespace: Optional[str] = None,
) -> Generator[Origin]:
    """
    List all pods in *namespace* and return information about every pod that
    contains a Pelican Origin container with a discoverable Pelican Server
    binary.

    Parameters
    ----------
    context:
        The Kubernetes context to use.  Defaults to the current context.
    namespace:
        The Kubernetes namespace to search.  Defaults to the namespace
        configured for *context*.

    Yields
    ------
    Origin
        The info about one pod: the namespace, name, container name, and path
        to pelican-server binary inside the container.

    Raises
    ------
    Error
        If the context or namespace cannot be determined.
    subprocess.CalledProcessError
        If the initial ``kubectl get pods`` call fails (e.g. bad namespace,
        missing credentials).
    json.JSONDecodeError
        If kubectl returns unexpected output.
    """
    if context is None:
        context = _current_context()
    if namespace is None:
        namespace = _namespace_for_context(context)

    result = _run(
        [
            "kubectl",
            "--context",
            context,
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
        pod_name = pod.get("metadata", {}).get("name", "<unknown>")
        info = examine_pod(pod, context, namespace)
        if info is not None:
            phase = pod.get("status", {}).get("phase", "<unknown>")
            if phase == "Running":
                yield info
            else:
                _printflush(
                    f"Origin {pod_name!r}: not Running (phase={phase!r})",
                    file=sys.stderr,
                )


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
        if volume.count(":") != 1:
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
            "--context",
            origin.context,
            "cp",
            "-c",
            origin.container_name,
            INNER_SCRIPT,
            f"{origin.namespace}/{origin.pod_name}:/tmp/{INNER_SCRIPT}",
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


def get_exports_for_pod(origin: Origin) -> tuple[str, list[dict]]:
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
            "--context",
            origin.context,
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


def _parse_args(
    argv,
) -> tuple[argparse.Namespace, list[tuple[str, configparser.SectionProxy]]]:
    """
    Parse CLI arguments, validate them, read config.ini, and build the cluster list.

    Returns
    -------
    args:
        Parsed argument namespace.
    clusters:
        Ordered list of ``(cluster_name, config_section)`` pairs to process.
    """
    parser = argparse.ArgumentParser(
        description="Collect Pelican Origin storage metrics from Kubernetes clusters."
    )
    parser.add_argument(
        "--nautilus", action="store_true", help="Only process the nautilus cluster"
    )
    parser.add_argument(
        "--tiger", action="store_true", help="Only process the tiger cluster"
    )
    parser.add_argument(
        "--tempest", action="store_true", help="Only process the tempest cluster"
    )
    parser.add_argument(
        "-n",
        type=int,
        default=None,
        metavar="N",
        help="Stop after processing N origins per cluster",
    )
    parser.add_argument(
        "-s",
        type=int,
        default=0,
        metavar="N",
        help="Skip the first N origins per cluster (to resume a previous run)",
    )
    parser.add_argument(
        "-p",
        "--pod",
        action="append",
        default=[],
        metavar="PREFIX",
        help="Only process pods whose name starts with PREFIX (may be given multiple times)",
    )
    args = parser.parse_args(argv)

    if args.s and args.pod:
        parser.error("-s and --pod are mutually exclusive")

    # If no cluster flag is set, process all clusters
    any_cluster = args.nautilus or args.tiger or args.tempest
    run_nautilus = args.nautilus or not any_cluster
    run_tiger = args.tiger or not any_cluster
    run_tempest = args.tempest or not any_cluster

    cfg = configparser.ConfigParser()
    cfg.read("config.ini")

    clusters = []
    if run_nautilus and "nautilus" in cfg:
        clusters.append(("nautilus", cfg["nautilus"]))
    if run_tiger and "tiger" in cfg:
        clusters.append(("tiger", cfg["tiger"]))
    if run_tempest and "tempest" in cfg:
        clusters.append(("tempest", cfg["tempest"]))

    return args, clusters


def _process_namespace(
    cluster_name: str,
    context: str,
    namespace: str,
    fh,
    args: argparse.Namespace,
    cluster_count: int,
    cluster_skipped: int,
) -> tuple[int, int]:
    """
    Process all origins in one namespace: check access, list pods, apply filters,
    collect exports, and append results to *fh*.

    Parameters
    ----------
    cluster_name:
        Human-readable cluster name (used in error messages).
    context:
        kubectl context for this cluster.
    namespace:
        Kubernetes namespace to search.
    fh:
        Open file handle to append JSON lines to.
    args:
        Parsed CLI arguments (uses ``args.n``, ``args.s``, ``args.pod``).
    cluster_count:
        Number of origins already processed in this cluster run.
    cluster_skipped:
        Number of origins already skipped (for ``-s`` resumption).

    Returns
    -------
    tuple[int, int]
        Updated ``(cluster_count, cluster_skipped)``.
    """
    if not _check_namespace_access(cluster_name, context, namespace):
        return cluster_count, cluster_skipped

    try:
        origins = list(find_pelican_origin_pods(context=context, namespace=namespace))
    except Exception as err:
        print(
            f"ERROR: failed to list pods in cluster={cluster_name!r} "
            f"namespace={namespace!r}: {err}",
            file=sys.stderr,
        )
        return cluster_count, cluster_skipped

    for origin in origins:
        if args.n is not None and cluster_count >= args.n:
            break
        if args.pod and not any(origin.pod_name.startswith(p) for p in args.pod):
            continue
        if cluster_skipped < args.s:
            cluster_skipped += 1
            continue

        exports = None
        sitename = None
        ok = True
        try:
            _printflush(f"{origin.pod_name}: Getting exports...")
            sitename, exports = get_exports_for_pod(origin)
        except Exception as err:
            print(f"ERROR: {origin.pod_name}: {err}", file=sys.stderr)
            ok = False

        _printflush(f"{origin.pod_name} {'ok' if ok else 'FAIL'}")
        fh.write(
            json.dumps(
                {
                    "origin": dataclasses.asdict(origin),
                    "exports": exports,
                    "sitename": sitename,
                }
            )
            + "\n"
        )
        fh.flush()
        cluster_count += 1

    return cluster_count, cluster_skipped


def main(argv=None) -> int:
    args, clusters = _parse_args(argv)

    for cluster_name, section in clusters:
        context = section["context"]
        namespaces = section["namespaces"].split()
        out_file = section["file"]
        cluster_count = 0
        cluster_skipped = 0

        with open(out_file, "a") as fh:
            for namespace in namespaces:
                if args.n is not None and cluster_count >= args.n:
                    break
                cluster_count, cluster_skipped = _process_namespace(
                    cluster_name,
                    context,
                    namespace,
                    fh,
                    args,
                    cluster_count,
                    cluster_skipped,
                )

    return 0


if __name__ == "__main__":
    sys.exit(main())
