#!/usr/bin/env python3
"""
storage_metrics.py

Outer-script functions (requires: Python 3.9, kubectl available on PATH).
Finds pods containing Pelican Origin containers in a given Kubernetes namespace,
then locates the Pelican Server binary inside each such container.
"""

import argparse
import configparser
import fnmatch
import json
import sys
from typing import Optional

from k8s import check_namespace_access, find_pelican_origin_pods
from output import print_exports_table
from pelican import get_exports_for_pod


def match_collab(fed_prefix: str, collab_map: dict[str, list[str]]) -> Optional[str]:
    """Return the collab name whose prefix list contains a startswith match, else None."""
    for collab, patterns in collab_map.items():
        if any(fed_prefix.startswith(p) for p in patterns):
            return collab
    return None


def parse_args(
    argv,
) -> tuple[
    argparse.Namespace,
    list[tuple[str, configparser.SectionProxy]],
    dict[str, list[tuple[str, str]]],
    dict[str, list[str]],
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
    collab_map:
        Dict mapping collab name to list of federation prefix patterns (from
        the ``[collabs]`` config section).  Empty dict if the section is absent.
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
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print progress messages to stderr",
    )
    parser.add_argument(
        "--table",
        action="store_true",
        help="Print a table of exports to stdout after querying each cluster",
    )
    parser.add_argument(
        "-i",
        "--input",
        action="append",
        default=[],
        metavar="FILE",
        help="Read data from FILE instead of querying clusters (may be given multiple times); implies --table",
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
    cfg.optionxform = str  # preserve key case
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

    collab_map: dict[str, list[str]] = {}
    if "collabs" in cfg:
        for collab_name, prefixes_str in cfg["collabs"].items():
            collab_map[collab_name] = prefixes_str.split()

    return args, clusters, sub_ns_map, collab_map


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


def _process_origin(
    cluster_name: str,
    origin,
    prefix_pairs: Optional[list[tuple[str, str]]],
    fh,
    args: argparse.Namespace,
) -> None:
    """Fetch exports for one origin and append the result to *fh*."""
    exports = None
    sitename = None
    ok = True
    try:
        if args.verbose:
            print(
                f"[{cluster_name}] {origin.pod_name}: Getting exports...",
                file=sys.stderr,
                flush=True,
            )
        if prefix_pairs is not None:
            sitename, exports = get_exports_for_pod(origin, prefix_pairs=prefix_pairs)
        else:
            sitename, exports = get_exports_for_pod(origin)
    except Exception as err:
        print(f"ERROR: {origin.pod_name}: {err}", file=sys.stderr)
        ok = False

    if args.verbose:
        print(
            f"[{cluster_name}] {origin.pod_name}: {'ok' if ok else 'FAIL'}",
            file=sys.stderr,
            flush=True,
        )
    fh.write(
        json.dumps({"origin": origin.deployment, "exports": exports, "sitename": sitename})
        + "\n"
    )
    fh.flush()


def _process_namespace(
    cluster_name: str,
    context: str,
    namespace: str,
    fh,
    args: argparse.Namespace,
    sub_ns_map: dict[str, list[tuple[str, str]]],
    cluster_count: int,
    cluster_skipped: int,
    exclude_globs: Optional[list[str]] = None,
) -> tuple[int, int, int, int]:
    """
    Process all origins in one namespace: check access, list pods, apply filters,
    collect exports, and append results to *fh*.

    Returns
    -------
    tuple[int, int, int, int]
        Updated ``(cluster_count, cluster_skipped, eligible, excluded)`` where
        *eligible* is the number of pods that would have been processed and
        *excluded* is how many of those were silently skipped by *exclude_globs*.
    """
    if exclude_globs is None:
        exclude_globs = []

    if not check_namespace_access(cluster_name, context, namespace):
        return cluster_count, cluster_skipped, 0, 0

    try:
        origins = list(find_pelican_origin_pods(context=context, namespace=namespace))
    except Exception as err:
        print(
            f"ERROR: failed to list pods in cluster={cluster_name!r} "
            f"namespace={namespace!r}: {err}",
            file=sys.stderr,
        )
        return cluster_count, cluster_skipped, 0, 0

    eligible = 0
    excluded = 0

    for origin in origins:
        if args.n is not None and cluster_count >= args.n:
            break

        explicitly_selected = bool(args.pod) and any(
            origin.pod_name.startswith(p) for p in args.pod
        )
        if args.pod and not explicitly_selected:
            continue
        if cluster_skipped < args.s:
            cluster_skipped += 1
            continue

        prefix_pairs = _get_sub_ns_prefixes(sub_ns_map, cluster_name, origin.pod_name)
        if not (prefix_pairs is not None or cluster_name == "nautilus"):
            continue

        eligible += 1

        if not explicitly_selected and any(
            fnmatch.fnmatch(origin.deployment, g) for g in exclude_globs
        ):
            excluded += 1
            continue

        _process_origin(cluster_name, origin, prefix_pairs, fh, args)
        cluster_count += 1

    return cluster_count, cluster_skipped, eligible, excluded


def main(argv=None) -> int:
    args, clusters, sub_ns_map, collab_map = parse_args(argv)
    table = args.table or bool(args.input)

    if args.input:
        for input_file in args.input:
            if table:
                print_exports_table(input_file, collab_map=collab_map)
                sys.stdout.flush()
        return 0

    for cluster_name, section in clusters:
        context = section["context"]
        namespaces = section["namespaces"].split()
        out_file = section["file"]
        exclude_globs = section.get("exclude_origins", "").split()
        cluster_count = 0
        cluster_skipped = 0
        cluster_eligible = 0
        cluster_excluded = 0

        with open(out_file, "a") as fh:
            for namespace in namespaces:
                if args.n is not None and cluster_count >= args.n:
                    break
                cluster_count, cluster_skipped, eligible, excluded = _process_namespace(
                    cluster_name,
                    context,
                    namespace,
                    fh,
                    args,
                    sub_ns_map,
                    cluster_count,
                    cluster_skipped,
                    exclude_globs,
                )
                cluster_eligible += eligible
                cluster_excluded += excluded

        if cluster_eligible > 0 and cluster_eligible == cluster_excluded:
            print(f"All pods for {cluster_name} skipped.", file=sys.stderr)

        if table:
            print_exports_table(out_file, collab_map=collab_map)
            sys.stdout.flush()

    return 0


if __name__ == "__main__":
    sys.exit(main())
