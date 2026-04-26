#!/usr/bin/env python3
"""
storage_metrics.py

Outer-script functions (requires: Python 3.9, kubectl available on PATH).
Finds pods containing Pelican Origin containers in a given Kubernetes namespace,
then locates the Pelican Server binary inside each such container.
"""

import argparse
import configparser
import json
import sys
from typing import Optional

from collab_types import Export, InnerScriptError, Origin
from helpers import _run
from k8s import (
    _check_namespace_access,
    _run_in_origin,
    examine_pod,
    find_pelican_origin_pods,
    interactive_exec,
)
from output import print_exports_table
from s3 import get_s3_bucket_size, handle_s3_exports

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The script to run inside the pod to get usage info
INNER_SCRIPT = "inner.py"


# ---------------------------------------------------------------------------
# Core public API
# ---------------------------------------------------------------------------


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
                print(f"{origin.pod_name}: Getting exports...", flush=True)
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

            print(f"{origin.pod_name} {'ok' if ok else 'FAIL'}", flush=True)
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
