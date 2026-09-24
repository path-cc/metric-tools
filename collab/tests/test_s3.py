from unittest.mock import MagicMock, patch

from s3 import get_s3_bucket_size, handle_s3_exports


@patch("s3.run")
def test_get_s3_bucket_size(mock_run):
    # HEAD probe matching bucket-size should return that value
    mock_run.return_value = MagicMock(stdout="", stderr="x-rgw-bytes-used: 1000\n")
    assert get_s3_bucket_size("my-bucket", "http://endpoint") == 1000

    # HEAD probe no match should fall back to full object sum
    mock_run.side_effect = [
        MagicMock(stdout="", stderr="no match"),  # HEAD probe
        MagicMock(returncode=0, stdout=""),  # Sanity probe
        MagicMock(stdout="5000\n"),  # Full sum
    ]
    assert get_s3_bucket_size("my-bucket", "http://endpoint") == 5000

    # Empty sum result should return 0
    mock_run.side_effect = [
        MagicMock(stdout="", stderr="no match"),  # HEAD probe
        MagicMock(returncode=0, stdout=""),  # Sanity probe
        MagicMock(stdout="None\n"),  # Full sum
    ]
    assert get_s3_bucket_size("my-bucket", "http://endpoint") == 0


@patch("s3.get_s3_bucket_size")
def test_handle_s3_exports_populates_sizes_and_forwards_connection_info(
    mock_get_size,
):
    mock_get_size.side_effect = [1000, 2000]
    s3_result = {
        "serviceurl": "http://endpoint",
        "region": "us-east-1",
        "accesskey": "access",
        "secretkey": "secret",
        "exports": [
            {"s3bucket": "bucket-1", "federation_prefix": "/one", "public": True},
            {"s3bucket": "bucket-2", "federation_prefix": "/two", "public": False},
        ],
    }

    exports = handle_s3_exports(s3_result)

    assert exports == [
        {
            "s3bucket": "bucket-1",
            "federation_prefix": "/one",
            "public": True,
            "size": 1000,
        },
        {
            "s3bucket": "bucket-2",
            "federation_prefix": "/two",
            "public": False,
            "size": 2000,
        },
    ]
    assert mock_get_size.call_args_list == [
        (
            ("bucket-1", "http://endpoint"),
            {
                "region": "us-east-1",
                "access_key": "access",
                "secret_key": "secret",
            },
        ),
        (
            ("bucket-2", "http://endpoint"),
            {
                "region": "us-east-1",
                "access_key": "access",
                "secret_key": "secret",
            },
        ),
    ]


@patch("s3.get_s3_bucket_size")
def test_handle_s3_exports_records_failure_and_continues(mock_get_size):
    mock_get_size.side_effect = [RuntimeError("access denied"), 2000]
    s3_result = {
        "serviceurl": "http://endpoint",
        "region": None,
        "accesskey": None,
        "secretkey": None,
        "exports": [
            {"s3bucket": "unavailable", "federation_prefix": "/one", "public": True},
            {"s3bucket": "available", "federation_prefix": "/two", "public": False},
        ],
    }

    exports = handle_s3_exports(s3_result)

    assert exports[0]["size"] is None
    assert exports[0]["error"] == "RuntimeError: access denied"
    assert exports[1]["size"] == 2000
    assert mock_get_size.call_count == 2
