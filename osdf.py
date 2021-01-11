#!/usr/bin/env python

from __future__ import print_function

import collections
import operator
import sys
import os

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

import xml.etree.ElementTree as et

_topology_host = "topology.opensciencegrid.org"
_rgsummary_url = 'https://{host}/rgsummary/xml'.format(host=_topology_host)

_osdf_service_types = [
    "XRootD cache server",
    "XRootD origin server",
]


def getxml():
    return urlopen(_rgsummary_url).read()


def get_osdf_facilities(xmltxt):
    xmltree = et.fromstring(xmltxt)
    return set(
        rg.find("Facility").find("Name").text
        for rg in xmltree.findall('ResourceGroup')
        for r in rg.find('Resources').findall('Resource')
        for s in r.find("Services").findall("Service")
        if s.find("Name").text in _osdf_service_types
    )


def main():
    xmltxt = getxml()
    facilities = sorted(get_osdf_facilities(xmltxt))
    n = len(facilities)
    print("%d OSDF Facilit%s:" % (n, "y" if n == 1 else "ies"))
    for f in sorted(facilities):
        print(" - %s" % f)


if __name__ == '__main__':
    main()

