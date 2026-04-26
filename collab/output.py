import json


def print_exports_table(path: str, *, si: bool = False) -> None:
    """
    Read a .jsonl file produced by this script and print a table of exports.

    Columns printed: federation_prefix, public, size (in TiB by default).
    Exports where any of those three fields is missing or null are skipped.

    Parameters
    ----------
    path:
        Path to the .jsonl file (e.g. ``"nautilus.jsonl"``).
    si:
        If True, display size in SI terabytes (10^12 bytes) instead of TiB (2^40 bytes).
    """
    divisor = 1e12 if si else 2**40
    size_header = "size (TB)" if si else "size (TiB)"

    rows: list[tuple[str, str, str]] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            for exp in entry.get("exports") or []:
                fed = exp.get("federation_prefix")
                pub = exp.get("public")
                size = exp.get("size")
                if fed is None or pub is None or size is None:
                    continue
                rows.append((fed, str(pub), f"{size / divisor:.2f}"))

    if not rows:
        print("(no data)")
        return

    headers = ("federation_prefix", "public", size_header)
    col_widths = [
        max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(headers)
    ]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    sep = "  ".join("-" * w for w in col_widths)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*row))
