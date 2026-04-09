#!/usr/bin/env python3


import dataclasses
import datetime
import errno
import json
import os
import pathlib
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
    """Populate result['s3'] with connection info and exports."""
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


def main(argv=()):
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
            if result['storagetype'] == "posix":
                handle_posix(origin_config, result)
            elif result['storagetype'] == "s3":
                handle_s3(origin_config, result)
        except Exception as err:
            result['status'] = "error"
            result['error'] = str(err)
            return 1

    finally:
        result['time'] = datetime.datetime.now().isoformat(timespec='seconds')
        json.dump(result, sys.stdout)
        sys.stdout.flush()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
