#!/usr/bin/env python3
"""
Inner script for reporting exported volumes in a Pelican Origin and their
sizes.  Queries the Pelican Origin config for export information
(Federation Prefix, Storage Prefix, various S3 info) and prints it to
stdout as JSON.

Has three modes:

- POSIX mode: for Origins with StorageType "posix", returns the size of the
  exported directories by measuring them with `du` or CephFS's `ceph.dir.rbytes`
  xattr.

- Scan mode: similar to POSIX mode but scans subdirectories of the export
  directories based on a provided list of storage:federation prefix mappings
  for cases where different subdirectories of the storage prefix belong to
  "subnamespaces" in the federation that are owned by different collaborations
  and so should be counted separately.

- S3 mode: for Origins with StorageType "s3", does not return sizes (since
  we don't have a way to get them from within the pod), but returns the list
  of exported buckets and S3 connection info (service URL, region, endpoint,
  keys) in the config.

  **Note:** This does print private information to stdout; it's up to the
  caller to handle that securely.

Usage:

    POSIX/S3 mode:

        inner.py

    Scan mode:

        inner.py scan <storage_prefix1>:<federation_prefix1> <storage_prefix2>:<federation_prefix2> ...


The output JSON has the following shape:
{
    "status": "ok" or "error",
    "error": "error message if status is error, else empty string",
    "sitename": Xrootd.SiteName from config,
    "storagetype": Origin.StorageType from config,
    "posix": {  // POSIX or Scan mode only, otherwise null
        "exports": [
            {
                "storage_prefix": ...,
                "federation_prefix": ...,
                "public": ...,
                "size": ...,
                "error": ...,
            },
            ...
        ]
    },
    "s3": {  // S3 only, otherwise null
        "serviceurl": ...,
        "region": ...,
        "accesskey": ...,
        "secretkey": ...,
        "exports": [
            {
                "s3bucket": ...,
                "federation_prefix": ...,
                "public": ...,
            },
            ...
        ]
    },
    "time": Timestamp of the data gather time in ISO8601 format (e.g. "2023-01-01T00:00:00Z")
}
"""

import dataclasses
import datetime
import errno
import json
import os
import subprocess
import sys
from typing import Optional


@dataclasses.dataclass
class Export:
    """A storage prefix/federation prefix combo, plus whether it's public or not."""

    federation_prefix: str
    public: bool
    storage_prefix: Optional[str] = None
    s3bucket: Optional[str] = None
    size: Optional[int] = None
    error: Optional[str] = None


def get_binary_name() -> str:
    """Return the first available Pelican/OSDF CLI binary name in PATH."""
    return (
        os.popen(
            "command -v pelican-server 2>/dev/null || command -v osdf-server 2>/dev/null || command -v pelican 2>/dev/null"
        )
        .read()
        .strip()
    )


def get_config() -> dict:
    """
    Run ``<pelican> config dump -o json`` parse the output.

    Raises
    ------
    subprocess.CalledProcessError
        If the config dump fails.
    json.JSONDecodeError
        If the config dump does not return valid JSON.
    TypeError
        If the config dump does not return a JSON object.
    """
    result = subprocess.run(
        [
            get_binary_name(),
            "config",
            "dump",
            "-o",
            "json",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )

    config: dict = json.loads(result.stdout)
    if not isinstance(config, dict):
        raise TypeError("config is %s, expected dict" % type(config))

    return config


def get_posix_export_dirs(origin_cfg: dict) -> list[Export]:
    """
    Parse the origin config to get the exported volumes.
    Handles both new-style (`Origin.Exports`)
    and old-style (`Origin.ExportVolumes`) volumes.

    Parameters
    ----------
    config:
        The config dump from get_config().

    Returns
    -------
    list[Export]
        A list of each export.

    Raises
    ------
    """
    exports: list[Export] = []

    # ------------------------------------------------------------------
    # New-style: Origin.Exports
    # Each entry carries its own capabilities list.
    # An entry is classified as public if "PublicReads" is in that list;
    # otherwise it is authenticated.
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


def get_s3_export_buckets(origin_cfg: dict) -> list[Export]:
    """
    Parse the origin config to get S3 exported buckets (new-style only).

    Parameters
    ----------
    origin_cfg:
        The ``Origin`` section of the config dump from get_config().

    Returns
    -------
    list[Export]
        A list of each S3 export.
    """
    exports: list[Export] = []

    for export in origin_cfg.get("Exports") or []:
        try:
            capabilities: list[str] = export["capabilities"]
            s3bucket: str = export["s3bucket"]
            federation_prefix: str = export["federationprefix"]
        except KeyError:
            # Malformed export
            continue

        exports.append(
            Export(
                s3bucket=s3bucket,
                federation_prefix=federation_prefix,
                public=("PublicReads" in capabilities),
            )
        )

    return exports


def get_dir_bytes(path: str) -> int:
    """
    Return the number of bytes used by the directory tree rooted at `path`.

    Tries CephFS's ceph.dir.rbytes extended attribute first (O(1) MDS lookup).
    Falls back to `du -bs` if:
      - the filesystem doesn't support xattrs or isn't CephFS (ENODATA/ENOTSUP/EOPNOTSUPP)
      - the xattr call fails for any other non-access reason

    Raises PermissionError/FileNotFoundError if the path itself is inaccessible,
    since `du` would fail for the same reason.
    """
    try:
        value = os.getxattr(path, "ceph.dir.rbytes")
        return int(value)
    except OSError as err:
        if err.errno in (errno.EACCES, errno.EPERM, errno.ENOENT, errno.ENOTDIR):
            raise
        # ENODATA  = xattr name not found (not CephFS)
        # ENOTSUP/EOPNOTSUPP = filesystem doesn't support xattrs at all
        # Anything else unexpected: fall through to du rather than crash

    result = subprocess.run(
        ["du", "-bs", path],
        capture_output=True,
        text=True,
        check=True,
    )
    size_str, _, _ = result.stdout.partition("\t")
    return int(size_str)


def get_required_config() -> tuple[dict, str, str]:
    """
    Load and validate the pelican config.

    Returns
    -------
    tuple of (origin_config, sitename, storagetype)

    Raises
    ------
    subprocess.CalledProcessError
        If the config command fails.
    Exception
        If the config is missing required fields.
    """
    config = get_config()
    sitename = config.get("Xrootd", {}).get("Sitename", None)
    origin_config = config.get("Origin", {})
    if not origin_config:
        raise RuntimeError("Origin config not found")
    storagetype = origin_config.get("StorageType", None)
    if not storagetype:
        raise RuntimeError("StorageType not found in Origin config")
    return origin_config, sitename, storagetype


def handle_posix(origin_config: dict, result: dict) -> None:
    """Populate result['posix'] with exports and sizes."""
    exports = get_posix_export_dirs(origin_config)
    for export in exports:
        try:
            assert (
                export.storage_prefix is not None
            ), "POSIX export missing storage_prefix (should have been caught)"
            export.size = get_dir_bytes(export.storage_prefix)
        except Exception as err:
            # Keep collecting data for other exports even if one path fails.
            print(
                f"{export.storage_prefix}: Error getting size: {err}",
                file=sys.stderr,
            )
            export.error = str(err)
    result['posix']['exports'] = [dataclasses.asdict(it) for it in exports]


def _read_key_file(path: str, label: str) -> Optional[str]:
    """Read a key file, printing a warning to stderr on failure."""
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception as err:
        print(f"{path}: Error reading {label}: {err}", file=sys.stderr)
        return None


def handle_s3(origin_config: dict, result: dict) -> None:
    """Populate ``result['s3']`` with connection metadata and export buckets."""
    result['s3']['serviceurl'] = origin_config.get("S3ServiceURL", None)
    result['s3']['region'] = origin_config.get("S3Region", None)
    access_key_file = origin_config.get("S3AccessKeyFile", None)
    secret_key_file = origin_config.get("S3SecretKeyFile", None)
    if access_key_file:
        result['s3']['accesskey'] = _read_key_file(access_key_file, "S3 access key")
    if secret_key_file:
        result['s3']['secretkey'] = _read_key_file(secret_key_file, "S3 secret key")
    exports = get_s3_export_buckets(origin_config)
    result['s3']['exports'] = [dataclasses.asdict(it) for it in exports]


def handle_scan(origin_config: dict, result: dict, scan_args: list[str]) -> None:
    """
    Implement scan mode by crawling subdirectories of the export directories
    based on provided storage:federation prefix mappings.

    Each scan argument uses ``storage_root:federation_root`` format.  They
    are both absolute paths that should be subdirectories of the
    StoragePrefixes and FederationPrefixes in the Origin config.
    """
    # To determine public/private, we need the existing POSIX exports for longest-prefix matching
    known_exports = get_posix_export_dirs(origin_config)
    scanned_exports: list[Export] = []

    for arg in scan_args:
        if ":" not in arg:
            print(
                f"Warning: skipping malformed scan argument '{arg}' (expected storage:federation)",
                file=sys.stderr,
            )
            continue
        storage_root, federation_root, match_public = (
            resolve_storage_federation_mapping(known_exports, arg)
        )

        # Now go through the top level of storage_root
        with os.scandir(storage_root) as it:
            for entry in it:
                if not entry.is_dir():
                    continue

                full_storage = os.path.join(storage_root, entry.name)
                full_federation = federation_root + "/" + entry.name

                export = Export(
                    storage_prefix=full_storage,
                    federation_prefix=full_federation,
                    public=match_public,
                )

                try:
                    export.size = get_dir_bytes(full_storage)
                except Exception as err:
                    print(
                        f"{full_storage}: Error getting size: {err}",
                        file=sys.stderr,
                    )
                    export.error = str(err)

                scanned_exports.append(export)

    result['posix']['exports'] = [dataclasses.asdict(it) for it in scanned_exports]
    result['storagetype'] = "posix"


def resolve_storage_federation_mapping(known_exports, arg):
    """
    Split out the storage_root and federation_root from the scan argument,
    and determine whether it's public or private based on longest prefix
    match of the federation root against the known exports.
    """
    storage_root, _, federation_root = arg.partition(":")
    storage_root = storage_root.rstrip("/")
    federation_root = federation_root.rstrip("/")

    match_public = False
    match_found = False
    best_len = -1
    for ex in known_exports:
        if ex.federation_prefix and federation_root.startswith(ex.federation_prefix):
            if len(ex.federation_prefix) > best_len:
                best_len = len(ex.federation_prefix)
                match_public = ex.public
                match_found = True

    if not match_found:
        raise RuntimeError(
            f"Invalid mapping {arg} -- no matching export found for "
            f"scan path {federation_root}; it must be a subpath of a "
            "configured export prefix"
        )

    return storage_root, federation_root, match_public


def main(argv=()):
    """Run mode dispatch, always emit a JSON result payload, and return exit code."""
    argv = argv or sys.argv

    result = {
        "status": "ok",
        "error": "",
        "posix": {"exports": []},
        "s3": {
            "serviceurl": None,
            "region": None,
            "accesskey": None,
            "secretkey": None,
            "exports": [],
        },
        "sitename": None,
        "time": None,
        "storagetype": None,
    }

    try:
        try:
            # Validate config first so downstream handlers can rely on required
            # values being present.
            origin_config, result['sitename'], result['storagetype'] = (
                get_required_config()
            )
        except subprocess.CalledProcessError as err:
            result['status'] = "error"
            result['error'] = f"config command failed: {err.stderr.strip()}"
            return 1
        except Exception as err:
            result['status'] = "error"
            result['error'] = str(err)
            return 1

        try:
            # Explicit scan arg takes precedence over configured storage type.
            if len(argv) > 1 and argv[1] == "scan":
                handle_scan(origin_config, result, argv[2:])
            elif result['storagetype'] == "posix":
                handle_posix(origin_config, result)
            elif result['storagetype'] == "s3":
                handle_s3(origin_config, result)
        except Exception as err:
            result['status'] = "error"
            result['error'] = str(err)
            return 1

    finally:
        # Emit timestamp and JSON even on failure so callers always receive a
        # machine-readable status payload.
        result['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        json.dump(result, sys.stdout)
        sys.stdout.flush()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
