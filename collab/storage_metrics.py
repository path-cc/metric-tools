#!/usr/bin/env python3
"""
pelican_origin_finder.py

Outer-script functions (requires: Python 3.9, kubectl available on PATH).
Finds pods containing Pelican Origin containers in a given Kubernetes namespace,
then locates the Pelican Server binary inside each such container.
"""

import argparse
import configparser
import json
import os
import re
import subprocess
import sys
from collections.abc import Generator, Mapping
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

    @property
    def deployment(self) -> str:
        return "-".join(self.pod_name.split("-")[:-2])


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


def handle_s3_exports(s3_result: dict) -> list[dict]:
    """Stub: extract S3 exports from the inner script's 's3' result dict."""
    return s3_result['exports']


def get_exports_for_pod(
    origin: Origin, prefix_pairs: Optional[list[tuple[str, str]]] = None
) -> tuple[str, list[dict]]:
    copy_inner_script_to_origin(origin)
    if prefix_pairs is not None:
        args = ["scan"] + [f"{s}:{f}" for s, f in prefix_pairs]
        result = run_inner_script(origin, *args, copy=False)
    else:
        result = run_inner_script(origin, copy=False)
    if result['status'] != "ok":
        raise InnerScriptError(f"Inner script returned error: {result['error']}")
    storagetype = result['storagetype']
    if storagetype == "posix":
        exports = result['posix']['exports']
    elif storagetype == "s3":
        exports = handle_s3_exports(result['s3'])
    else:
        print(
            f"WARNING: {origin.pod_name}: unknown storage type {storagetype!r}, skipping exports",
            file=sys.stderr,
        )
        exports = []
    return result['sitename'], exports


def get_s3_bucket_size(
    bucket: str,
    endpoint: str,
    *,
    region: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
) -> int:
    """
    Return the total size in bytes of an S3 bucket, using the ``aws`` CLI.

    First tries a HEAD-bucket debug probe: some S3 implementations (e.g. Ceph
    RGW) report the bucket size in a response header visible in ``--debug``
    output.  If no parseable size is found there, falls back to summing all
    object sizes via ``list-objects-v2``.

    Parameters
    ----------
    bucket:
        The S3 bucket name.
    endpoint:
        The S3 endpoint URL.
    region:
        Value for ``AWS_DEFAULT_REGION``; defaults to empty string.
    access_key:
        Value for ``AWS_ACCESS_KEY_ID``; defaults to empty string.
    secret_key:
        Value for ``AWS_SECRET_ACCESS_KEY``; defaults to empty string.

    Returns
    -------
    int
        Total size in bytes (0 if the bucket is empty).

    Raises
    ------
    subprocess.CalledProcessError
        If the bucket is inaccessible or an ``aws`` CLI call fails.
    """
    extra_env = {
        "AWS_DEFAULT_REGION": region or "",
        "AWS_ACCESS_KEY_ID": access_key or "",
        "AWS_SECRET_ACCESS_KEY": secret_key or "",
        "AWS_S3_ADDRESSING_STYLE": "path",
    }
    # Step 1: HEAD bucket probe.  The debug output may contain a line like
    # "x-rgw-bytes-used: 12345678" on Ceph-backed clusters.
    ret = _run(
        [
            "aws",
            "s3api",
            "head-bucket",
            "--bucket",
            bucket,
            "--endpoint-url",
            endpoint,
            "--debug",
        ],
        check=False,
        extra_env=extra_env,
    )
    for line in (ret.stdout + ret.stderr).splitlines():
        if re.search(r"bucket.*(size|bytes)", line, re.IGNORECASE):
            m = re.search(r"\b(\d+)\b", line)
            if m:
                _printflush(f"{bucket}: size from HEAD probe")
                return int(m.group(1))

    # Step 2: Sanity probe — verify list access before the expensive full scan.
    _run(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--endpoint-url",
            endpoint,
            "--max-keys",
            "0",
        ],
        extra_env=extra_env,
    )

    # Step 3: Full sum.
    ret = _run(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--endpoint-url",
            endpoint,
            "--query",
            "sum(Contents[].Size)",
            "--output",
            "text",
        ],
        extra_env=extra_env,
    )
    text = ret.stdout.strip()
    _printflush(f"{bucket}: size from full object sum")
    if text in ("None", ""):
        return 0
    return int(text)


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


def print_exports_table(path: str, *, si: bool = False) -> None:
    """
    Read a .jsonl file produced by this script and print a table of exports.

    Columns printed: federation_prefix, public, size (in TiB by default).
    Exports where any of those three fields is missing or null are skipped.

    Parameters
    ----------
    path:
        Path to the .jsonl file (e.g. ``"nautilus.jsonl"``).
    si:
        If True, display size in SI terabytes (10^12 bytes) instead of TiB (2^40 bytes).
    """
    divisor = 1e12 if si else 2**40
    size_header = "size (TB)" if si else "size (TiB)"

    rows: list[tuple[str, str, str]] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            for exp in entry.get("exports") or []:
                fed = exp.get("federation_prefix")
                pub = exp.get("public")
                size = exp.get("size")
                if fed is None or pub is None or size is None:
                    continue
                rows.append((fed, str(pub), f"{size / divisor:.2f}"))

    if not rows:
        print("(no data)")
        return

    headers = ("federation_prefix", "public", size_header)
    col_widths = [
        max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(headers)
    ]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    sep = "  ".join("-" * w for w in col_widths)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*row))


def _parse_args(
    argv,
) -> tuple[
    argparse.Namespace,
    list[tuple[str, configparser.SectionProxy]],
    dict[str, list[tuple[str, str]]],
]:
    """
    Parse CLI arguments, validate them, read config.ini, and build the cluster list.

    Returns
    -------
    args:
        Parsed argument namespace.
    clusters:
        Ordered list of ``(cluster_name, config_section)`` pairs to process.
    sub_ns_map:
        Dict mapping section names like "CLUSTER:POD_PREFIX" to lists of
        (storage_prefix, federation_prefix) tuples.
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

    # Parse [cluster:pod-prefix] sections for sub-namespace mapping
    sub_ns_map: dict[str, list[tuple[str, str]]] = {}
    known_clusters = {"nautilus", "tiger", "tempest"}
    for section_name in cfg.sections():
        if ":" not in section_name:
            continue
        cluster_name, pod_prefix = section_name.split(":", 1)
        if cluster_name not in known_clusters:
            continue

        section = cfg[section_name]
        prefix_pairs: list[tuple[str, str]] = []

        # Parse numbered storage_prefix_N / federation_prefix_N pairs
        n = 1
        while True:
            storage_key = f"storage_prefix_{n}"
            federation_key = f"federation_prefix_{n}"
            if storage_key not in section or federation_key not in section:
                break
            storage_prefix = section[storage_key]
            federation_prefix = section[federation_key]
            prefix_pairs.append((storage_prefix, federation_prefix))
            n += 1

        if prefix_pairs:
            sub_ns_map[section_name] = prefix_pairs

    return args, clusters, sub_ns_map


def _get_sub_ns_prefixes(
    sub_ns_map: dict[str, list[tuple[str, str]]],
    cluster_name: str,
    pod_name: str,
) -> Optional[list[tuple[str, str]]]:
    """
    Find prefix pairs for a pod from the sub-namespace map.

    Iterates through sub_ns_map for keys starting with "cluster_name:".
    Returns the prefix list for the first key where pod_name.startswith(pod_prefix).
    Returns None if no match.

    Parameters
    ----------
    sub_ns_map:
        Dict mapping "CLUSTER:POD_PREFIX" to lists of (storage_prefix, federation_prefix) tuples.
    cluster_name:
        The cluster name to search for.
    pod_name:
        The pod name to match against pod_prefix.

    Returns
    -------
    list[tuple[str, str]] | None
        The prefix pairs if a match is found, otherwise None.
    """
    prefix = f"{cluster_name}:"
    for key in sub_ns_map:
        if not key.startswith(prefix):
            continue
        pod_prefix = key[len(prefix) :]
        if pod_name.startswith(pod_prefix):
            return sub_ns_map[key]
    return None


def _process_namespace(
    cluster_name: str,
    context: str,
    namespace: str,
    fh,
    args: argparse.Namespace,
    sub_ns_map: dict[str, list[tuple[str, str]]],
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
    sub_ns_map:
        Sub-namespace mapping dict from _parse_args.
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
        prefix_pairs = _get_sub_ns_prefixes(sub_ns_map, cluster_name, origin.pod_name)

        # Determine if pod should be processed or skipped
        should_process = prefix_pairs is not None or cluster_name == "nautilus"

        if should_process:
            try:
                _printflush(f"{origin.pod_name}: Getting exports...")
                if prefix_pairs is not None:
                    # Matched Tiger/Tempest pod: run inner.py with scan mode
                    sitename, exports = get_exports_for_pod(
                        origin, prefix_pairs=prefix_pairs
                    )
                else:
                    # Nautilus pods: auto-discovery (no args)
                    sitename, exports = get_exports_for_pod(origin)
            except Exception as err:
                print(f"ERROR: {origin.pod_name}: {err}", file=sys.stderr)
                ok = False

            _printflush(f"{origin.pod_name} {'ok' if ok else 'FAIL'}")
            fh.write(
                json.dumps(
                    {
                        "origin": origin.deployment,
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
    args, clusters, sub_ns_map = _parse_args(argv)

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
                    sub_ns_map,
                    cluster_count,
                    cluster_skipped,
                )

    return 0


if __name__ == "__main__":
    sys.exit(main())
