from unittest.mock import patch

import pytest

from collab_types import InnerScriptError, Origin
from pelican import get_exports_for_pod


@patch("pelican.run_inner_script")
@patch("pelican.copy_inner_script_to_origin")
def test_get_exports_for_pod(mock_copy, mock_run_inner):
    origin = Origin(namespace="ns", pod_name="pod", container_name="c", context="ctx")

    # POSIX storage type should extract posix.exports
    mock_run_inner.return_value = {
        "status": "ok",
        "sitename": "site-posix",
        "storagetype": "posix",
        "posix": {"exports": [{"path": "/p1"}]},
    }
    sitename, exports = get_exports_for_pod(origin)
    assert sitename == "site-posix"
    assert exports == [{"path": "/p1"}]

    # S3 storage type should extract s3.exports
    mock_run_inner.return_value = {
        "status": "ok",
        "sitename": "site-s3",
        "storagetype": "s3",
        "s3": {"exports": [{"bucket": "b1"}]},
    }
    sitename, exports = get_exports_for_pod(origin)
    assert sitename == "site-s3"
    assert exports == [{"bucket": "b1"}]

    # Unknown storage type should return empty exports
    mock_run_inner.return_value = {
        "status": "ok",
        "sitename": "site-unknown",
        "storagetype": "unknown",
    }
    sitename, exports = get_exports_for_pod(origin)
    assert sitename == "site-unknown"
    assert exports == []

    # Error status in response should raise InnerScriptError
    mock_run_inner.return_value = {"status": "error", "error": "some error"}
    with pytest.raises(
        InnerScriptError, match="Inner script returned error: some error"
    ):
        get_exports_for_pod(origin)
