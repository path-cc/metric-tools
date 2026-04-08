#!/usr/bin/env python3


import errno
import json
import os
import subprocess
import sys


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


if __name__ == "__main__":
    try:
        json.dump(
            {"status": "ok", "error": None, "bytes": get_dir_bytes(sys.argv[1])},
            sys.stdout,
        )
    except OSError as err:
        json.dump({"status": "error", "error": str(err), "bytes": None}, sys.stdout)
