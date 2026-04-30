import json

from output import match_collab, print_exports_table


def test_print_exports_table(tmp_path, capsys):
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


def test_print_exports_table_exclude_ns_globs(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "sitename": "site1",
            "exports": [
                {
                    "federation_prefix": "/ospool/uc-shared/project/HepSim",
                    "public": True,
                    "size": 2**40,
                },
                {"federation_prefix": "/EHT/public", "public": True, "size": 2**39},
                {
                    "federation_prefix": "/ospool/uc-shared/project/REDTOP",
                    "public": True,
                    "size": 2**38,
                },
            ],
        }
    ]
    with open(jsonl_file, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")

    # Glob matching: HepSim and REDTOP excluded, EHT kept
    print_exports_table(
        str(jsonl_file),
        exclude_ns_globs=["/ospool/uc-shared/*/HepSim", "/ospool/uc-shared/*/REDTOP"],
    )
    captured = capsys.readouterr().out
    assert "/EHT/public" in captured
    assert "HepSim" not in captured
    assert "REDTOP" not in captured

    # Empty exclude list: nothing excluded
    print_exports_table(str(jsonl_file), exclude_ns_globs=[])
    captured = capsys.readouterr().out
    assert "/ospool/uc-shared/project/HepSim" in captured
    assert "/EHT/public" in captured
    assert "/ospool/uc-shared/project/REDTOP" in captured

    # None exclude list: nothing excluded
    print_exports_table(str(jsonl_file), exclude_ns_globs=None)
    captured = capsys.readouterr().out
    assert "/ospool/uc-shared/project/HepSim" in captured


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

    collab_ns_map = {"EHT": ["/EHT/public*", "/EHT/private*"]}

    # Collab column is prepended; unmatched prefix shows (unknown)
    print_exports_table(str(jsonl_file), collab_ns_map=collab_ns_map)
    captured = capsys.readouterr().out
    assert "collab" in captured
    assert "EHT" in captured
    assert "(unknown)" in captured
    assert "/EHT/public" in captured
    assert "/ospool/other" in captured
    # Sorted by collab then federation_prefix; '(' < 'E' in ASCII
    assert captured.index("(unknown)") < captured.index("EHT")

    # Empty collab_ns_map: collab column still present
    print_exports_table(str(jsonl_file), collab_ns_map={})
    captured = capsys.readouterr().out
    assert "collab" in captured
    assert "(unknown)" in captured
    assert "federation_prefix" in captured


def test_match_collab():
    collab_ns_map = {
        "EHT": ["/EHT/public*", "/EHT/private*"],
        "REDTOP": ["/REDTOP/public*"],
    }

    # Exact match (glob matches the path itself)
    assert match_collab("/EHT/public", collab_ns_map) == "EHT"

    # Sub-path match via glob wildcard
    assert match_collab("/EHT/public/data/file.root", collab_ns_map) == "EHT"

    # Match on second glob of same collab
    assert match_collab("/EHT/private", collab_ns_map) == "EHT"

    # Match on different collab
    assert match_collab("/REDTOP/public", collab_ns_map) == "REDTOP"

    # No match
    assert match_collab("/ospool/uc-shared", collab_ns_map) is None

    # Without wildcard, sub-path does not match
    assert match_collab("/EHT/public/data", {"EHT": ["/EHT/public"]}) is None

    # Empty map
    assert match_collab("/EHT/public", {}) is None
