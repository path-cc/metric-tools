import argparse
from unittest.mock import patch

import pytest

from collab_types import Origin
from storage_metrics import _process_namespace, main, parse_args, read_config


def test_parse_args():
    # Default flags
    args = parse_args([])
    assert args.verbose is False
    assert args.table is False
    assert args.input == []
    assert args.nautilus is False
    assert args.tiger is False
    assert args.tempest is False

    # Cluster flags are set correctly
    args = parse_args(["--nautilus"])
    assert args.nautilus is True
    assert args.tiger is False

    # -s and --pod together should raise parser error (mutually exclusive)
    with pytest.raises(SystemExit):
        parse_args(["-s", "10", "--pod", "some-pod"])


def test_read_config_clusters(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.ini"
    config_file.write_text("[nautilus]\ncontext = c1\n[tiger]\ncontext = c2\n")

    # No cluster flags: returns all clusters present in config
    args = parse_args([])
    config = read_config(args)
    assert len(config.clusters) == 2
    assert config.clusters[0][0] == "nautilus"
    assert config.clusters[1][0] == "tiger"
    assert isinstance(config.sub_ns_map, dict)
    assert config.collab_ns_map == {}
    assert config.exclude_ns_globs == []

    # Single cluster flag: only that cluster
    args = parse_args(["--nautilus"])
    config = read_config(args)
    assert len(config.clusters) == 1
    assert config.clusters[0][0] == "nautilus"


def test_read_config_collab_map(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.ini"

    # Single prefix per collab
    config_file.write_text("[collab_namespaces]\nEHT = /EHT/public\nREDTOP = /REDTOP/public\n")
    config = read_config(parse_args([]))
    assert config.collab_ns_map == {"EHT": ["/EHT/public"], "REDTOP": ["/REDTOP/public"]}

    # Multiple space-separated prefixes
    config_file.write_text(
        "[collab_namespaces]\nEvent_Horizon_Telescope = /EHT/public /EHT/private\n"
    )
    config = read_config(parse_args([]))
    assert config.collab_ns_map == {
        "Event_Horizon_Telescope": ["/EHT/public", "/EHT/private"]
    }

    # Empty [collab_namespaces] section
    config_file.write_text("[collab_namespaces]\n")
    config = read_config(parse_args([]))
    assert config.collab_ns_map == {}


def test_read_config_exclude_namespaces(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.ini"

    # Absent section: empty list
    config_file.write_text("")
    config = read_config(parse_args([]))
    assert config.exclude_ns_globs == []

    # Single key with one glob
    config_file.write_text(
        "[exclude_namespaces]\nmigrated = /ospool/uc-shared/*/HepSim\n"
    )
    config = read_config(parse_args([]))
    assert config.exclude_ns_globs == ["/ospool/uc-shared/*/HepSim"]

    # Multiple keys and multiple space-separated globs per key: all collected flat
    config_file.write_text(
        "[exclude_namespaces]\n"
        "migrated = /ospool/uc-shared/*/HepSim /ospool/uc-shared/*/REDTOP\n"
        "untracked = /jkb-lab /ndp/*\n"
    )
    config = read_config(parse_args([]))
    assert set(config.exclude_ns_globs) == {
        "/ospool/uc-shared/*/HepSim",
        "/ospool/uc-shared/*/REDTOP",
        "/jkb-lab",
        "/ndp/*",
    }


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
    mock_table.assert_called_once_with(out_file, collab_ns_map={}, exclude_ns_globs=[])

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

    origin_excl = Origin(
        namespace="ns",
        pod_name="nsdf-origin-abc-def",
        container_name="c",
        context="ctx",
    )
    origin_kept = Origin(
        namespace="ns", pod_name="my-origin-abc-def", container_name="c", context="ctx"
    )
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
