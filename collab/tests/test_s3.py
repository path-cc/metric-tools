from unittest.mock import MagicMock, patch

from s3 import get_s3_bucket_size


@patch("s3.run")
def test_get_s3_bucket_size(mock_run):
    # HEAD probe matching bucket-size should return that value
    mock_run.return_value = MagicMock(stdout="", stderr="bucket-size: 1000\n")
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
