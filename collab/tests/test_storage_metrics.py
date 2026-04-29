import argparse
import json
from unittest.mock import MagicMock, patch

import pytest

from collab_types import Error, InnerScriptError, Origin
from k8s import (
    check_namespace_access,
    examine_pod,
    is_origin_container,
    namespace_for_context,
)
from output import match_collab, print_exports_table
from pelican import get_exports_for_pod
from s3 import get_s3_bucket_size
from storage_metrics import _process_namespace, main, parse_args

# ---------------------------------------------------------------------------
# Pure function tests - No mocks, testing logic directly
# ---------------------------------------------------------------------------


def test_is_origin_container():
    # Matching images for pelican origin or osdf-origin
    assert (
        is_origin_container(
            {"image": "hub.opensciencegrid.org/pelican/osdf-origin:latest"}
        )
        is True
    )
    assert (
        is_origin_container({"image": "hub.opensciencegrid.org/pelican/origin:v1.0.0"})
        is True
    )

    # Non-matching images should return False
    assert (
        is_origin_container({"image": "hub.opensciencegrid.org/pelican/cache:latest"})
        is False
    )
    assert is_origin_container({"image": "nginx:latest"}) is False

    # Images with fewer than 3 / separated parts (no registry)
    # The implementation does parts = re.split(r"[:@/]", full_image); image = parts[2]
    # So "pelican/origin" -> ["pelican", "origin"] -> IndexError
    assert is_origin_container({"image": "pelican/origin"}) is False
    assert is_origin_container({"image": "origin"}) is False

    # Missing image key should return False
    assert is_origin_container({}) is False


def test_examine_pod():
    # Mock _current_context and _namespace_for_context to avoid subprocess calls
    with (
        patch("k8s._current_context", return_value="my-context"),
        patch("k8s.namespace_for_context", return_value="my-ns"),
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


def test_print_exports_table_with_collab_map(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "sitename": "site1",
            "exports": [
                {"federation_prefix": "/EHT/public", "public": True, "size": 2**40},
                {"federation_prefix": "/ospool/other", "public": True, "size": 2**39},
            ],
        }
    ]
    with open(jsonl_file, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")

    collab_map = {"EHT": ["/EHT/public", "/EHT/private"]}

    # Collab column is prepended; unmatched prefix shows (unknown)
    print_exports_table(str(jsonl_file), collab_map=collab_map)
    captured = capsys.readouterr().out
    assert "collab" in captured
    assert "EHT" in captured
    assert "(unknown)" in captured
    assert "/EHT/public" in captured
    assert "/ospool/other" in captured

    # Empty collab_map: collab column still present
    print_exports_table(str(jsonl_file), collab_map={})
    captured = capsys.readouterr().out
    assert "collab" in captured
    assert "(unknown)" in captured
    assert "federation_prefix" in captured


# ---------------------------------------------------------------------------
# Mocked function tests - Tests that use mocks for external dependencies
# ---------------------------------------------------------------------------


@patch("k8s.run")
def test_namespace_for_context(mock_run):
    # Active context with * marker should parse correctly
    mock_run.return_value = MagicMock(stdout="*  ctx-1  cluster-1  user-1  ns-1\n")
    assert namespace_for_context("ctx-1") == "ns-1"

    # Non-active context should also parse correctly
    mock_run.return_value = MagicMock(stdout="   ctx-2  cluster-2  user-2  ns-2\n")
    assert namespace_for_context("ctx-2") == "ns-2"

    # Unparseable output should raise Error
    mock_run.return_value = MagicMock(stdout="bad line\n")
    with pytest.raises(Error, match="Could not determine namespace"):
        namespace_for_context("ctx-3")


@patch("k8s.run")
def test_check_namespace_access(mock_run, capsys):
    # Both checks passing should return True
    mock_run.return_value = MagicMock(returncode=0, stdout="yes")
    assert check_namespace_access("cluster", "context", "ns") is True

    # First check failing should return False and print error
    mock_run.side_effect = [
        MagicMock(returncode=1, stdout="", stderr="forbidden"),
        MagicMock(returncode=0, stdout="yes"),
    ]
    assert check_namespace_access("cluster", "context", "ns") is False
    captured = capsys.readouterr().err
    assert "ERROR: insufficient permissions" in captured
    assert "forbidden" in captured


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


@patch("pelican.run_inner_script")
@patch("pelican.copy_inner_script_to_origin")
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
    args, clusters, sub_ns_map, collab_map = parse_args([])
    assert len(clusters) == 2
    assert clusters[0][0] == "nautilus"
    assert clusters[1][0] == "tiger"
    assert isinstance(sub_ns_map, dict)
    assert collab_map == {}
    assert args.verbose is False
    assert args.table is False
    assert args.input == []

    # Single cluster flag should only run that cluster
    args, clusters, sub_ns_map, collab_map = parse_args(["--nautilus"])
    assert len(clusters) == 1
    assert clusters[0][0] == "nautilus"

    # -s and --pod together should raise parser error (mutually exclusive)
    with pytest.raises(SystemExit):
        parse_args(["-s", "10", "--pod", "some-pod"])


def test_parse_args_collab_map(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.ini"

    # Single prefix per collab
    config_file.write_text("[collabs]\nEHT = /EHT/public\nREDTOP = /REDTOP/public\n")
    _, _, _, collab_map = parse_args([])
    assert collab_map == {"EHT": ["/EHT/public"], "REDTOP": ["/REDTOP/public"]}

    # Multiple space-separated prefixes
    config_file.write_text(
        "[collabs]\nEvent_Horizon_Telescope = /EHT/public /EHT/private\n"
    )
    _, _, _, collab_map = parse_args([])
    assert collab_map == {"Event_Horizon_Telescope": ["/EHT/public", "/EHT/private"]}

    # Empty [collabs] section
    config_file.write_text("[collabs]\n")
    _, _, _, collab_map = parse_args([])
    assert collab_map == {}


def test_match_collab():
    collab_map = {
        "EHT": ["/EHT/public", "/EHT/private"],
        "REDTOP": ["/REDTOP/public"],
    }

    # Exact prefix match
    assert match_collab("/EHT/public", collab_map) == "EHT"

    # Sub-path match (prefix is a prefix of the federation path)
    assert match_collab("/EHT/public/data/file.root", collab_map) == "EHT"

    # Match on second prefix of same collab
    assert match_collab("/EHT/private", collab_map) == "EHT"

    # Match on different collab
    assert match_collab("/REDTOP/public", collab_map) == "REDTOP"

    # No match
    assert match_collab("/ospool/uc-shared", collab_map) is None

    # Empty map
    assert match_collab("/EHT/public", {}) is None


@patch("storage_metrics.get_exports_for_pod")
@patch("storage_metrics.find_pelican_origin_pods")
@patch("storage_metrics.check_namespace_access")
def test_process_namespace_verbose(
    mock_access, mock_find, mock_exports, tmp_path, capsys
):
    mock_access.return_value = True
    origin = Origin(
        namespace="ns",
        pod_name="collab-shared-osdf-pelican-origin-abc",
        container_name="c",
        context="ctx",
    )
    mock_find.return_value = [origin]
    mock_exports.return_value = ("site1", [])

    sub_ns_map = {
        "tiger:collab-shared-osdf-pelican-origin": [
            ("/mnt/origin/public", "/ospool/uw-shared/public")
        ]
    }
    out_file = tmp_path / "out.jsonl"

    # verbose=True: before and after messages on stderr, nothing on stdout
    args = argparse.Namespace(n=None, s=0, pod=[], verbose=True)
    with open(out_file, "w") as fh:
        _process_namespace("tiger", "ctx", "ns", fh, args, sub_ns_map, 0, 0)
    captured = capsys.readouterr()
    assert "tiger" in captured.err
    assert "collab-shared-osdf-pelican-origin-abc" in captured.err
    assert captured.out == ""

    # verbose=False: no stderr progress output
    args.verbose = False
    with open(out_file, "w") as fh:
        _process_namespace("tiger", "ctx", "ns", fh, args, sub_ns_map, 0, 0)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


@patch("storage_metrics.print_exports_table")
def test_main_input_flag(mock_table, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text(
        "[nautilus]\ncontext = c1\nnamespaces = ns1\nfile = out.jsonl\n"
    )
    f1 = tmp_path / "a.jsonl"
    f2 = tmp_path / "b.jsonl"
    f1.write_text("")
    f2.write_text("")

    # -i alone prints one table per file; clusters are not queried
    main(["-i", str(f1), "-i", str(f2)])
    assert mock_table.call_count == 2
    calls = [c[0][0] for c in mock_table.call_args_list]
    assert calls == [str(f1), str(f2)]


@patch("storage_metrics.print_exports_table")
@patch("storage_metrics._process_namespace")
def test_main_table_flag(mock_process, mock_table, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out_file = str(tmp_path / "nautilus.jsonl")
    (tmp_path / "config.ini").write_text(
        f"[nautilus]\ncontext = c1\nnamespaces = ns1\nfile = {out_file}\n"
    )
    mock_process.return_value = (1, 0, 1, 0)

    # --table triggers print_exports_table after the cluster
    main(["--nautilus", "--table"])
    mock_table.assert_called_once_with(out_file, collab_map={})

    # Without --table, no table is printed
    mock_table.reset_mock()
    main(["--nautilus"])
    mock_table.assert_not_called()


@patch("storage_metrics.get_exports_for_pod")
@patch("storage_metrics.find_pelican_origin_pods")
@patch("storage_metrics.check_namespace_access")
def test_process_namespace_exclude(mock_access, mock_find, mock_exports, tmp_path):
    mock_access.return_value = True
    mock_exports.return_value = ("site1", [])

    origin_excl = Origin(namespace="ns", pod_name="nsdf-origin-abc-def", container_name="c", context="ctx")
    origin_kept = Origin(namespace="ns", pod_name="my-origin-abc-def", container_name="c", context="ctx")
    mock_find.return_value = [origin_excl, origin_kept]

    args = argparse.Namespace(n=None, s=0, pod=[], verbose=False)
    out_file = tmp_path / "out.jsonl"

    # nsdf-origin matches the glob; my-origin does not
    with open(out_file, "w") as fh:
        count, _, eligible, excluded = _process_namespace(
            "nautilus", "ctx", "ns", fh, args, {}, 0, 0, exclude_globs=["nsdf-origin"]
        )
    assert eligible == 2
    assert excluded == 1
    assert count == 1
    assert mock_exports.call_count == 1

    # -p selects the excluded pod explicitly: exclusion does not apply
    mock_exports.reset_mock()
    args_p = argparse.Namespace(n=None, s=0, pod=["nsdf-origin"], verbose=False)
    with open(out_file, "w") as fh:
        count, _, eligible, excluded = _process_namespace(
            "nautilus", "ctx", "ns", fh, args_p, {}, 0, 0, exclude_globs=["nsdf-origin"]
        )
    assert excluded == 0
    assert count == 1


@patch("storage_metrics.print_exports_table")
@patch("storage_metrics._process_namespace")
def test_main_all_pods_skipped(mock_process, mock_table, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    out_file = str(tmp_path / "nautilus.jsonl")
    (tmp_path / "config.ini").write_text(
        f"[nautilus]\ncontext = c1\nnamespaces = ns1\nfile = {out_file}\nexclude_origins = some-origin\n"
    )

    # All eligible pods excluded: warning printed
    mock_process.return_value = (0, 0, 3, 3)
    main(["--nautilus"])
    assert "All pods for nautilus skipped." in capsys.readouterr().err

    # Only some excluded: no warning
    mock_process.return_value = (1, 0, 3, 2)
    main(["--nautilus"])
    assert "All pods for nautilus skipped." not in capsys.readouterr().err

    # No eligible pods at all: no warning
    mock_process.return_value = (0, 0, 0, 0)
    main(["--nautilus"])
    assert "All pods for nautilus skipped." not in capsys.readouterr().err
