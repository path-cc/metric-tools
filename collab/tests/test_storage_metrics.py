import json
from unittest.mock import MagicMock, patch

import pytest

from collab_types import Error, InnerScriptError, Origin
from k8s import _check_namespace_access, _is_origin_container, _namespace_for_context, examine_pod
from output import print_exports_table
from s3 import get_s3_bucket_size
from storage_metrics import _parse_args, get_exports_for_pod

# ---------------------------------------------------------------------------
# Pure function tests - No mocks, testing logic directly
# ---------------------------------------------------------------------------


def test_is_origin_container():
    # Matching images for pelican origin or osdf-origin
    assert (
        _is_origin_container(
            {"image": "hub.opensciencegrid.org/pelican/osdf-origin:latest"}
        )
        is True
    )
    assert (
        _is_origin_container({"image": "hub.opensciencegrid.org/pelican/origin:v1.0.0"})
        is True
    )

    # Non-matching images should return False
    assert (
        _is_origin_container({"image": "hub.opensciencegrid.org/pelican/cache:latest"})
        is False
    )
    assert _is_origin_container({"image": "nginx:latest"}) is False

    # Images with fewer than 3 / separated parts (no registry)
    # The implementation does parts = re.split(r"[:@/]", full_image); image = parts[2]
    # So "pelican/origin" -> ["pelican", "origin"] -> IndexError
    assert _is_origin_container({"image": "pelican/origin"}) is False
    assert _is_origin_container({"image": "origin"}) is False

    # Missing image key should return False
    assert _is_origin_container({}) is False


def test_examine_pod():
    # Mock _current_context and _namespace_for_context to avoid subprocess calls
    with (
        patch("k8s._current_context", return_value="my-context"),
        patch("k8s._namespace_for_context", return_value="my-ns"),
    ):

        # Pod with origin container should be recognized and returned
        pod_origin = {
            "metadata": {"name": "pod-1-2"},
            "spec": {
                "containers": [
                    {
                        "name": "c1",
                        "image": "hub.opensciencegrid.org/pelican/osdf-origin:latest",
                    }
                ]
            },
        }
        origin = examine_pod(pod_origin)
        assert origin == Origin(
            namespace="my-ns",
            pod_name="pod-1-2",
            container_name="c1",
            context="my-context",
        )

        # Pod with non-origin container should return None
        pod_no_origin = {
            "metadata": {"name": "pod-1-2"},
            "spec": {"containers": [{"name": "c2", "image": "nginx:latest"}]},
        }
        assert examine_pod(pod_no_origin) is None

        # Pod missing metadata should return None
        assert examine_pod({}) is None


def test_print_exports_table(tmp_path, capsys):
    # Test printing exports table in various formats and unit systems
    jsonl_file = tmp_path / "test.jsonl"

    # Normal test data with various sizes
    data = [
        {
            "sitename": "site1",
            "exports": [
                {"federation_prefix": "/fed1", "public": True, "size": 2**40},  # 1 TiB
                {
                    "federation_prefix": "/fed2",
                    "public": False,
                    "size": 2**39,
                },  # 0.5 TiB
            ],
        },
        {
            "sitename": "site2",
            "exports": [
                {
                    "federation_prefix": "/fed3",
                    "public": True,
                    "size": None,
                },  # Should be skipped
                {"federation_prefix": "/fed4", "public": True, "size": 10**12},  # 1 TB
            ],
        },
    ]

    with open(jsonl_file, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")

    # Test default output (TiB unit)
    print_exports_table(str(jsonl_file), si=False)
    captured = capsys.readouterr().out
    assert "federation_prefix" in captured
    assert "size (TiB)" in captured
    assert "/fed1" in captured
    assert "1.00" in captured
    assert "/fed2" in captured
    assert "0.50" in captured
    assert "/fed4" in captured
    assert "0.91" in captured  # 10^12 / 2^40 approx 0.909
    assert "/fed3" not in captured  # None sizes are skipped

    # Test SI output (TB unit)
    print_exports_table(str(jsonl_file), si=True)
    captured = capsys.readouterr().out
    assert "size (TB)" in captured
    assert "1.10" in captured  # 2^40 / 10^12 = 1.0995
    assert "0.55" in captured  # 2^39 / 10^12 = 0.5497
    assert "1.00" in captured  # 10^12 / 10^12 = 1.00

    # Test empty data handling
    with open(jsonl_file, "w") as f:
        f.write("\n")
    print_exports_table(str(jsonl_file))
    captured = capsys.readouterr().out
    assert "(no data)" in captured


# ---------------------------------------------------------------------------
# Mocked function tests - Tests that use mocks for external dependencies
# ---------------------------------------------------------------------------


@patch("k8s._run")
def test_namespace_for_context(mock_run):
    # Active context with * marker should parse correctly
    mock_run.return_value = MagicMock(stdout="*  ctx-1  cluster-1  user-1  ns-1\n")
    assert _namespace_for_context("ctx-1") == "ns-1"

    # Non-active context should also parse correctly
    mock_run.return_value = MagicMock(stdout="   ctx-2  cluster-2  user-2  ns-2\n")
    assert _namespace_for_context("ctx-2") == "ns-2"

    # Unparseable output should raise Error
    mock_run.return_value = MagicMock(stdout="bad line\n")
    with pytest.raises(Error, match="Could not determine namespace"):
        _namespace_for_context("ctx-3")


@patch("k8s._run")
def test_check_namespace_access(mock_run, capsys):
    # Both checks passing should return True
    mock_run.return_value = MagicMock(returncode=0, stdout="yes")
    assert _check_namespace_access("cluster", "context", "ns") is True

    # First check failing should return False and print error
    mock_run.side_effect = [
        MagicMock(returncode=1, stdout="", stderr="forbidden"),
        MagicMock(returncode=0, stdout="yes"),
    ]
    assert _check_namespace_access("cluster", "context", "ns") is False
    captured = capsys.readouterr().err
    assert "ERROR: insufficient permissions" in captured
    assert "forbidden" in captured


@patch("s3._run")
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


@patch("storage_metrics.run_inner_script")
@patch("storage_metrics.copy_inner_script_to_origin")
def test_get_exports_for_pod(mock_copy, mock_run_inner):
    # Helper to create test Origin object
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


def test_parse_args(tmp_path, monkeypatch):
    # Create config file with multiple clusters
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.ini"
    config_file.write_text("[nautilus]\ncontext = c1\n[tiger]\ncontext = c2\n")

    # No cluster flags: runs all clusters present in config
    args, clusters, sub_ns_map = _parse_args([])
    assert len(clusters) == 2
    assert clusters[0][0] == "nautilus"
    assert clusters[1][0] == "tiger"
    assert isinstance(sub_ns_map, dict)

    # Single cluster flag should only run that cluster
    args, clusters, sub_ns_map = _parse_args(["--nautilus"])
    assert len(clusters) == 1
    assert clusters[0][0] == "nautilus"

    # -s and --pod together should raise parser error (mutually exclusive)
    with pytest.raises(SystemExit):
        _parse_args(["-s", "10", "--pod", "some-pod"])
