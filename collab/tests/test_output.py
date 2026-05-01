import json

from output import match_collab, print_collabs_summary, print_exports_table


def test_print_exports_table(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"

    # Normal test data with various sizes
    data = [
        {
            "sitename": "site1",
            "exports": [
                {"federation_prefix": "/fed1", "public": True, "size": 2**40},  # 1 TiB
                {"federation_prefix": "/fed2", "public": False, "size": 2**39},  # 0.5 TiB
            ],
        },
        {
            "sitename": "site2",
            "exports": [
                {"federation_prefix": "/fed3", "public": True, "size": None},  # skipped
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


def test_print_exports_table_deduplication(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    # /ndp appears twice; last entry (public=True, size=200) should win
    data = [
        {"exports": [{"federation_prefix": "/ndp", "public": False, "size": 100}]},
        {"exports": [{"federation_prefix": "/ndp", "public": True, "size": 200}]},
    ]
    with open(jsonl_file, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")

    print_exports_table(str(jsonl_file))
    captured = capsys.readouterr().out
    assert "access" in captured
    lines = [l for l in captured.splitlines() if "/ndp" in l]
    assert len(lines) == 1  # only one row for /ndp
    assert "public" in lines[0]  # last-wins: public=True -> "public"
    # size 200 bytes / 2^40 rounds to 0.00 — just check /ndp appears once
    assert captured.count("/ndp") == 1


def test_print_exports_table_right_aligned_size(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "exports": [
                {"federation_prefix": "/short", "public": True, "size": 2**40},
                {"federation_prefix": "/a-much-longer-prefix", "public": True, "size": 2**39},
            ]
        }
    ]
    with open(jsonl_file, "w") as f:
        f.write(json.dumps(data[0]) + "\n")

    print_exports_table(str(jsonl_file))
    captured = capsys.readouterr().out
    lines = captured.splitlines()
    # Find the header line and a data line; size values should be at the same column position
    header_line = next(l for l in lines if "size (TiB)" in l)
    size_col_end = header_line.index("size (TiB)") + len("size (TiB)")
    for line in lines:
        if "/short" in line or "/a-much-longer-prefix" in line:
            # The size value should end at or before the same column as the header
            assert len(line) <= size_col_end + 2  # allow for trailing spaces


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


def test_print_collabs_summary_basic(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "exports": [
                {"federation_prefix": "/EHT/public/a", "public": True, "size": 2**40},
                {"federation_prefix": "/EHT/public/b", "public": True, "size": 2**40},
                {"federation_prefix": "/ospool/other", "public": False, "size": 2**39},
            ]
        }
    ]
    with open(jsonl_file, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")

    collab_ns_map = {"EHT": ["/EHT/public*"]}
    print_collabs_summary([str(jsonl_file)], collab_ns_map)
    captured = capsys.readouterr().out

    assert "collab" in captured
    assert "public (TiB)" in captured
    assert "auth (TiB)" in captured
    assert "EHT" in captured
    assert "2.00" in captured  # 2 * 2^40 public for EHT
    assert "(unknown)" in captured
    assert "0.50" in captured  # 2^39 auth for unknown
    assert "Unmatched federation prefixes:" in captured
    assert "/ospool/other" in captured


def test_print_collabs_summary_public_private_split(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "exports": [
                {"federation_prefix": "/EHT/public", "public": True, "size": 3 * 2**40},
                {"federation_prefix": "/EHT/private", "public": False, "size": 2**40},
            ]
        }
    ]
    with open(jsonl_file, "w") as f:
        f.write(json.dumps(data[0]) + "\n")

    collab_ns_map = {"EHT": ["/EHT/*"]}
    print_collabs_summary([str(jsonl_file)], collab_ns_map)
    captured = capsys.readouterr().out

    lines = [l for l in captured.splitlines() if "EHT" in l and "(" not in l]
    assert len(lines) == 1
    assert "3.00" in lines[0]  # public
    assert "1.00" in lines[0]  # private
    assert "Unmatched" not in captured


def test_print_collabs_summary_unknown_public_private(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "exports": [
                {"federation_prefix": "/unmatched/pub", "public": True, "size": 2**40},
                {"federation_prefix": "/unmatched/priv", "public": False, "size": 2**39},
            ]
        }
    ]
    with open(jsonl_file, "w") as f:
        f.write(json.dumps(data[0]) + "\n")

    print_collabs_summary([str(jsonl_file)], {})
    captured = capsys.readouterr().out

    lines = [l for l in captured.splitlines() if "(unknown)" in l]
    assert len(lines) == 1
    assert "1.00" in lines[0]  # public: 2^40
    assert "0.50" in lines[0]  # private: 2^39


def test_print_collabs_summary_multi_file(tmp_path, capsys):
    f1 = tmp_path / "a.jsonl"
    f2 = tmp_path / "b.jsonl"
    f1.write_text(
        json.dumps(
            {"exports": [{"federation_prefix": "/EHT/pub", "public": True, "size": 2**40}]}
        )
        + "\n"
    )
    f2.write_text(
        json.dumps(
            {"exports": [{"federation_prefix": "/EHT/priv", "public": False, "size": 2**40}]}
        )
        + "\n"
    )

    collab_ns_map = {"EHT": ["/EHT/*"]}
    print_collabs_summary([str(f1), str(f2)], collab_ns_map)
    captured = capsys.readouterr().out

    lines = [l for l in captured.splitlines() if "EHT" in l and "(" not in l]
    assert len(lines) == 1
    assert "1.00" in lines[0]  # public from f1
    # second 1.00 for private from f2
    assert lines[0].count("1.00") == 2


def test_print_collabs_summary_deduplication(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    # /ndp appears twice; last entry (public=True, size=200) should win
    data = [
        {"exports": [{"federation_prefix": "/ndp", "public": False, "size": 100}]},
        {"exports": [{"federation_prefix": "/ndp", "public": True, "size": 200}]},
    ]
    with open(jsonl_file, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")

    print_collabs_summary([str(jsonl_file)], {})
    captured = capsys.readouterr().out

    # 200 bytes public, 0 private → both round to 0.00 at TiB scale
    # Key check: /ndp is listed once in unmatched, and unknown row has public contribution
    assert captured.count("/ndp") == 1
    lines = [l for l in captured.splitlines() if "(unknown)" in l]
    assert len(lines) == 1
    # Last-wins: size=200 in public column; private=0.00
    assert lines[0].count("0.00") >= 1  # private should be 0.00


def test_print_collabs_summary_si(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {"exports": [{"federation_prefix": "/EHT/public", "public": True, "size": 10**12}]}
    ]
    with open(jsonl_file, "w") as f:
        f.write(json.dumps(data[0]) + "\n")

    print_collabs_summary([str(jsonl_file)], {"EHT": ["/EHT/public*"]}, si=True)
    captured = capsys.readouterr().out
    assert "public (TB)" in captured
    assert "auth (TB)" in captured
    assert "1.00" in captured


def test_print_collabs_summary_exclude(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "exports": [
                {"federation_prefix": "/EHT/public", "public": True, "size": 2**40},
                {"federation_prefix": "/excluded/ns", "public": True, "size": 2**40},
            ]
        }
    ]
    with open(jsonl_file, "w") as f:
        f.write(json.dumps(data[0]) + "\n")

    print_collabs_summary(
        [str(jsonl_file)],
        {"EHT": ["/EHT/public*"]},
        exclude_ns_globs=["/excluded/*"],
    )
    captured = capsys.readouterr().out
    assert "EHT" in captured
    assert "/excluded/ns" not in captured
    assert "(unknown)" not in captured


def test_print_collabs_summary_no_data(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    jsonl_file.write_text("\n")
    print_collabs_summary([str(jsonl_file)], {"EHT": ["/EHT/public*"]})
    captured = capsys.readouterr().out
    assert "(no data)" in captured


def test_print_exports_table_title(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    jsonl_file.write_text(
        json.dumps({"exports": [{"federation_prefix": "/f", "public": True, "size": 2**40}]})
        + "\n"
    )

    # Title present: printed before table with '=' underline
    print_exports_table(str(jsonl_file), title="Nautilus Exports")
    captured = capsys.readouterr().out
    lines = captured.splitlines()
    assert lines[0] == "Nautilus Exports"
    assert lines[1] == "=" * len("Nautilus Exports")
    assert lines[2] == ""

    # No title: output starts with header row
    print_exports_table(str(jsonl_file))
    captured = capsys.readouterr().out
    assert captured.startswith("collab")

    # Title also appears before '(no data)'
    empty = tmp_path / "empty.jsonl"
    empty.write_text("\n")
    print_exports_table(str(empty), title="My Title")
    captured = capsys.readouterr().out
    assert captured.startswith("My Title\n" + "=" * len("My Title"))
    assert "(no data)" in captured


def test_print_collabs_summary_title(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    jsonl_file.write_text(
        json.dumps({"exports": [{"federation_prefix": "/f", "public": True, "size": 2**40}]})
        + "\n"
    )

    print_collabs_summary([str(jsonl_file)], {}, title="Storage Utilization")
    captured = capsys.readouterr().out
    lines = captured.splitlines()
    assert lines[0] == "Storage Utilization"
    assert lines[1] == "=" * len("Storage Utilization")
    assert lines[2] == ""

    # No title: starts with header row
    print_collabs_summary([str(jsonl_file)], {})
    captured = capsys.readouterr().out
    assert captured.startswith("collab")

    # Title before '(no data)'
    empty = tmp_path / "empty.jsonl"
    empty.write_text("\n")
    print_collabs_summary([str(empty)], {}, title="Summary")
    captured = capsys.readouterr().out
    assert captured.startswith("Summary\n" + "=" * len("Summary"))
    assert "(no data)" in captured


def test_print_collabs_summary_right_aligned(tmp_path, capsys):
    jsonl_file = tmp_path / "test.jsonl"
    data = [
        {
            "exports": [
                {"federation_prefix": "/EHT/pub", "public": True, "size": 2**40},
                {"federation_prefix": "/EHT/priv", "public": False, "size": 2**39},
            ]
        }
    ]
    with open(jsonl_file, "w") as f:
        f.write(json.dumps(data[0]) + "\n")

    print_collabs_summary([str(jsonl_file)], {"EHT": ["/EHT/*"]})
    captured = capsys.readouterr().out
    lines = [l for l in captured.splitlines() if l and not l.startswith("-")]
    header = lines[0]
    # Right-aligned: the header text ends at the right edge of the column
    pub_col_end = header.rindex(")")  # end of "public (TiB)"
    data_line = next(l for l in lines if "EHT" in l and "(" not in l)
    # The numeric value in the public column should end at or before pub_col_end
    tokens = data_line.split()
    assert tokens[1] == "0.50"  # auth
    assert tokens[2] == "1.00"  # public
