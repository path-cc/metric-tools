import json
from typing import Optional


def match_collab(fed_prefix: str, collab_map: dict[str, list[str]]) -> Optional[str]:
    """
    Return the collab name whose prefix list contains a startswith match, else None.

    Parameters
    ----------
    fed_prefix:
        The federation prefix to look up.
    collab_map:
        A mapping of collaboration name to list of prefixes.

    Returns
    -------
    str | None
        The name of the collaboration (if found), else None.
    """
    for collab, patterns in collab_map.items():
        if any(fed_prefix.startswith(p) for p in patterns):
            return collab
    return None


def print_exports_table(
    data_path: str,
    *,
    si: bool = False,
    collab_map: Optional[dict[str, list[str]]] = None,
) -> None:
    """
    Read a .jsonl file produced by this script and print a table of exports.

    Columns printed: federation_prefix, public, size (in TiB by default).
    Exports where any of those three fields is missing or null are skipped.

    Parameters
    ----------
    data_path:
        Path to the data file to get the numbers from (e.g. ``"nautilus.jsonl"``).
    si:
        If True, display size in SI terabytes (10^12 bytes) instead of TiB (2^40 bytes).
    collab_map:
        A collaboration-to-namespace pattern mapping.
    """
    divisor = 1e12 if si else 2**40
    size_header = "size (TB)" if si else "size (TiB)"

    use_collab = bool(collab_map)
    rows: list[tuple] = []
    with open(data_path) as fh:
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
                if use_collab:
                    assert collab_map is not None  # shut the type checker up
                    collab = match_collab(fed, collab_map) or "(unknown)"
                    rows.append((collab, fed, str(pub), f"{size / divisor:.2f}"))
                else:
                    collab = "(unknown)"
                    rows.append((collab, fed, str(pub), f"{size / divisor:.2f}"))

    if not rows:
        print("(no data)")
        return

    headers = ("collab", "federation_prefix", "public", size_header)
    col_widths = [
        max(len(h), max(len(r[i]) for r in rows)) for i, h in enumerate(headers)
    ]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    sep = "  ".join("-" * w for w in col_widths)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*row))
