#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prints list of campuses with active researchers in the given date range, along with whether they're CC* or not.

See https://opensciencegrid.atlassian.net/browse/SOFTWARE-4396 for
the definition of 'active researcher'.

"""

from argparse import ArgumentParser
import csv
import opensearchpy
from opensearchpy import Search, A, Q
import datetime
import sys
from typing import List, Optional, Set
import urllib3
import xml.etree.ElementTree as ET


GRACC = "https://gracc.opensciencegrid.org/q"
TOPOLOGY_RGSUMMARY = "https://topology.opensciencegrid.org/rgsummary/xml"


def safe_elem_text(elem: Optional[ET.Element]) -> str:
    """Return the stripped text of an element if available.  If not available, return the empty string"""
    text = getattr(elem, "text", "")
    return text.strip()


def get_topology_resourcegroups() -> List[ET.Element]:
    """Return the ElementTree Elements of the active, enabled ResourceGroups in the rgsummary XML"""
    http = urllib3.PoolManager()
    r = http.request(
        "GET",
        TOPOLOGY_RGSUMMARY,
        # active=on turns on filtering by the 'active' field;
        # active_value=1 means the output should only contain active resources.
        # (Without active=on, topology ignores active_value.)
        # disable and disable_value work the same way.
        fields={
            "active": "on",
            "active_value": "1",
            "disable": "on",
            "disable_value": "0",
        },
    )
    root = ET.fromstring(r.data.decode("utf-8", errors="replace"))
    return root.findall("./ResourceGroup")


def is_resourcegroup_ccstar(resourcegroup: ET.Element) -> bool:
    return "CC*" in get_resourcegroup_tags(resourcegroup)


def get_resourcegroup_facility(resourcegroup: ET.Element) -> str:
    """Return the name of the facility the ResourceGroup is in"""
    return safe_elem_text(resourcegroup.find("./Facility/Name"))


def get_resourcegroup_tags(resourcegroup: ET.Element) -> Set[str]:
    """Get the union of the tags of all the Resources in a ResourceGroup"""
    return set(
        safe_elem_text(e)
        for e in resourcegroup.findall("./Resources/Resource/Tags/Tag")
    )


def get_ccstar_facilities() -> Set[str]:
    """Get a set of the names of the facilities that have the CC* tag"""
    return set(
        get_resourcegroup_facility(rg)
        for rg in get_topology_resourcegroups()
        if is_resourcegroup_ccstar(rg)
    )


def get_organizations_with_active_researchers(
    starttime: datetime.datetime, endtime: datetime.datetime
) -> Set[str]:
    """Query GRACC to get a set of the names of the organizations that have active researchers in the given date range.
    The field OIM_Organization is taken from the Organizations in the projects YAML files.

    """
    es = opensearchpy.OpenSearch(
        [GRACC],
        timeout=300,
        use_ssl=True,
        verify_certs=True,
    )

    MAXSZ = 2 ** 30
    index = "gracc.osg.summary"
    s = Search(using=es, index=index)
    # Starttime and endtime are both datetime objects
    s = s.query(
        "bool",
        filter=[
            Q("range", EndTime={"gte": starttime, "lt": endtime})
            & Q("term", ResourceType="Payload")
        ],
    )

    bkt = s.aggs
    bkt = bkt.bucket("Organization", "terms", size=MAXSZ, field="OIM_Organization")

    response = s.execute()

    return {f["key"] for f in response.aggregations["Organization"]["buckets"]}


def get_organizations_with_active_researchers__dates(
    startdate: str, enddate: str, parser: ArgumentParser
) -> Set[str]:
    try:
        starttime = datetime.datetime.strptime(startdate, "%Y-%m-%d")
    except ValueError:
        parser.error("Cannot parse start date")
    try:
        endtime = datetime.datetime.strptime(
            enddate, "%Y-%m-%d"
        ) + datetime.timedelta(hours=23, minutes=59, seconds=59)
    except ValueError:
        parser.error("Cannot parse end date")
    if starttime > endtime:
        parser.error("Start date can't be after end date")

    runtime = datetime.datetime.now()
    return get_organizations_with_active_researchers(starttime, endtime)


def main(argv):
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "startdate",
        metavar="START_DATE",
        help="First day in the date range in YEAR-MONTH-DAY format",
    )
    parser.add_argument(
        "enddate",
        metavar="END_DATE",
        help="Last day in the date range in YEAR-MONTH-DAY format",
    )
    parser.add_argument("--csv", action="store_true", help="Print in CSV format")

    args = parser.parse_args(argv[1:])

    runtime = datetime.datetime.now()
    # SOFTWARE-5361: "New" means, "In the list when run for the past 30 days,
    # but NOT in the list when run for 2020-10-01 thru 2022-09-30".
    old_active_orgs = get_organizations_with_active_researchers__dates(
        "2020-10-01", "2022-09-30", parser
    )
    active_organizations = get_organizations_with_active_researchers__dates(
        args.startdate, args.enddate, parser
    )
    new_active_orgs = active_organizations - old_active_orgs
    ccstar_facilities = get_ccstar_facilities()

    # TODO: Project Organizations do not necessarily match Topology Facilities.
    # This may result in errors in the CC* column (false negatives being more likely).
    # Not sure where the place to fix it is -- or if the comparison is even meaningful.
    if args.csv:
        nowts = runtime.strftime("%F at %H:%M")
        dateinfo = "(Generated on {} for {} through {})".format(nowts, args.startdate, args.enddate)
        writer = csv.writer(sys.stdout, dialect="unix")
        writer.writerow(("CC*", "New", "Organization", dateinfo))
        for organization in sorted(active_organizations):
            ccstar = str(organization in ccstar_facilities)
            new    = str(organization in new_active_orgs)
            writer.writerow((ccstar, new, organization))
    else:
        fmt_string = "%-6s%-4s%s"
        yes_if = ("", "yes")
        print(fmt_string % ("CC*", "New", "Organization"))
        print(fmt_string % ("-----", "---", "---------"))
        for organization in sorted(active_organizations):
            ccstar = yes_if[organization in ccstar_facilities]
            new    = yes_if[organization in new_active_orgs]
            print(fmt_string % (ccstar, new, organization))


if __name__ == "__main__":
    main(sys.argv)
