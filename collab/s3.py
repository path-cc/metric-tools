import re
from typing import Optional

from helpers import run


def handle_s3_exports(s3_result: dict) -> list[dict]:
    """Stub: extract S3 exports from the inner script's 's3' result dict."""
    return s3_result['exports']


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
    ret = run(
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
                print(f"{bucket}: size from HEAD probe", flush=True)
                return int(m.group(1))

    # Step 2: Sanity probe — verify list access before the expensive full scan.
    run(
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
    ret = run(
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
    print(f"{bucket}: size from full object sum", flush=True)
    if text in ("None", ""):
        return 0
    return int(text)
