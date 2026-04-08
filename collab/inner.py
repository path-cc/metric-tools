#!/usr/bin/env python3


import dataclasses
import errno
import json
import os
import subprocess
import sys
from typing import Optional


@dataclasses.dataclass
class Export:
    """A storage prefix/federation prefix combo, plus whether it's public or not."""

    storage_prefix: str
    federation_prefix: str
    public: bool
    size: Optional[int] = None


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
    Run ``<pelican> config dump --json`` parse the output.

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
            "--json",
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


def get_export_dirs(config: dict) -> list[Export]:
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
    origin_cfg: dict = config.get("Origin") or {}

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


def main(argv=()):
    argv = argv or sys.argv

    result = {
        "status": "ok",
        "error": "",
        "exports": [],
        "sitename": None,
        "time": None,
    }

    try:
        config = get_config()
    except Exception as err:
        result['status'] = "error"
        result['error'] = f"error getting config: {err}"
        json.dump(result, sys.stdout)
        return 1

    result['sitename'] = config.get("Xrootd", {}).get("Sitename", None)

    try:
        exports = get_export_dirs(config)
    except Exception as err:
        result['status'] = "error"
        result['error'] = f"error getting exports: {err}"
        json.dump(result, sys.stdout)
        return 1

    for export in exports:
        try:
            export.size = get_dir_bytes(export.storage_prefix)
        except Exception as err:
            print(
                f"{export.storage_prefix}: Error getting size: {err}", file=sys.stderr
            )
            continue

    result['exports'] = [dataclasses.asdict(it) for it in exports]
    result['time'] = os.popen("date -Iseconds").read().strip()
    json.dump(result, sys.stdout)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
