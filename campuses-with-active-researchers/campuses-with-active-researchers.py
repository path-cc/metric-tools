#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prints list of campuses with active researchers in the given date range, along with whether they're CC* or not.

See https://opensciencegrid.atlassian.net/browse/SOFTWARE-4396 for
the definition of 'active researcher'.

"""

from argparse import ArgumentParser
import csv
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A, Q
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


def get_facilities_with_active_researchers(
    starttime: datetime.datetime, endtime: datetime.datetime
) -> Set[str]:
    """Query GRACC to get a set of the names of the facilities that have active researchers in the given date range"""
    es = Elasticsearch(
        [GRACC],
        timeout=300,
        use_ssl=True,
        verify_certs=True,
        ca_certs="/etc/ssl/certs/ca-bundle.crt",
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
    bkt = bkt.bucket("Facility", "terms", size=MAXSZ, field="OIM_Facility")

    response = s.execute()

    return {f["key"] for f in response.aggregations["Facility"]["buckets"]}


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

    try:
        starttime = datetime.datetime.strptime(args.startdate, "%Y-%m-%d")
    except ValueError:
        parser.error("Cannot parse start date")
    try:
        endtime = datetime.datetime.strptime(
            args.enddate, "%Y-%m-%d"
        ) + datetime.timedelta(hours=23, minutes=59, seconds=59)
    except ValueError:
        parser.error("Cannot parse end date")
    if starttime > endtime:
        parser.error("Start date can't be after end date")

    active_facilities = get_facilities_with_active_researchers(starttime, endtime)
    ccstar_facilities = get_ccstar_facilities()

    if args.csv:
        writer = csv.writer(sys.stdout, dialect="unix")
        writer.writerow(("CC*", "Facility"))
        for facility in sorted(active_facilities):
            writer.writerow(("True" if facility in ccstar_facilities else "False", facility))
    else:
        fmt_string = "%-6s%s"
        print(fmt_string % ("CC*", "Facility"))
        print(fmt_string % ("----- ", "---------"))
        for facility in sorted(active_facilities):
            print(fmt_string % ("yes" if facility in ccstar_facilities else "", facility))


if __name__ == "__main__":
    main(sys.argv)
