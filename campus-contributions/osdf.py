"""
osdf_facilities.py

Lists unique institutions that have a registered cache or origin in OSDF.

Sources:
  Registry    : https://osdf-registry.osg-htc.org/api/v1.0/registry_ui/servers
  Institutions: https://topology-institutions.osg-htc.org/api/institution_ids

Usage:
  python osdf_facilities.py               # all (cache + origin)
  python osdf_facilities.py --type cache  # cache only
  python osdf_facilities.py --type origin # origin only
"""

import argparse
import json
import sys
import urllib.request
import urllib.error


REGISTRY_URL     = "https://osdf-registry.osg-htc.org/api/v1.0/registry_ui/servers"
INSTITUTIONS_URL = "https://topology-institutions.osg-htc.org/api/institution_ids"


def fetch_json(url: str) -> list | dict:
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as e:
        print(f"ERROR: could not reach {url}\n  {e}", file=sys.stderr)
        sys.exit(1)


def build_institution_map(institutions: list) -> dict:
    """Map institution ID URL -> human-readable name."""
    return {
        inst["id"]: inst["name"]
        for inst in institutions
        if inst.get("id") and inst.get("name")
    }


def get_facilities(server_type: str | None) -> list[str]:
    """
    Return a sorted, deduplicated list of institution names
    that have at least one approved cache or origin registration.

    server_type: None = both, "cache" = caches only, "origin" = origins only
    """
    registry     = fetch_json(REGISTRY_URL)
    institutions = fetch_json(INSTITUTIONS_URL)

    institution_map = build_institution_map(institutions)

    seen = set()
    for server in registry:
        is_cache  = server.get("is_cache", False)
        is_origin = server.get("is_origin", False)

        # Apply type filter
        if server_type == "cache" and not is_cache:
            continue
        if server_type == "origin" and not is_origin:
            continue
        if not is_cache and not is_origin:
            continue

        # Walk registrations to find an approved one with an institution
        for reg in server.get("registration", []):
            meta   = reg.get("admin_metadata") or {}
            status = meta.get("status", "")

            if status.lower() != "approved":
                continue

            institution_id = meta.get("institution", "")
            institution    = institution_map.get(institution_id, "").strip()

            if institution:
                seen.add(institution)
                break  # one match per server is enough

    return sorted(seen, key=str.casefold)


def main():
    parser = argparse.ArgumentParser(
        description="List OSDF institutions with registered cache or origin servers."
    )
    parser.add_argument(
        "--type",
        choices=["cache", "origin"],
        default=None,
        metavar="TYPE",
        help="Filter by server type: 'cache' or 'origin' (default: all)",
    )
    args = parser.parse_args()

    type_label = args.type.capitalize() if args.type else "Cache & Origin"

    facilities = get_facilities(args.type)

    print(f"{len(facilities)} OSDF Facilities:")
    for name in facilities:
        print(f"- {name}")


if __name__ == "__main__":
    main()
