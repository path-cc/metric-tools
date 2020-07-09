#!/usr/bin/python

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
_topology_itb_host = "topology-itb.opensciencegrid.org"
_rgsummary_url = 'https://{host}/rgsummary/xml'
_ce_params = [
    ('gridtype',     'on'),
    ('gridtype_1',   'on'),
    ('service',      'on'),
    ('service_1',    'on')
]
_ces_url = "%s?%s" % (_rgsummary_url, '&'.join(map('='.join, _ce_params)))


def rg_cc_star_fqdns(rg):
    return [
        r.find('FQDN').text
        for r in rg.find('Resources').findall('Resource')
        for tag in r.find('Tags').findall('Tag')
        if tag.text == 'CC*'
    ]


def get_cc_star_fqdns(xmltxt):
    xmltree = et.fromstring(xmltxt)
    return sorted( fqdn for rg in xmltree.findall('ResourceGroup')
                        for fqdn in rg_cc_star_fqdns(rg) )


def get_cc_star_fqdns_from(host):
    ces_url = _ces_url.replace("{host}", host, 1)
    xmltxt = urlopen(ces_url).read()
    return get_cc_star_fqdns(xmltxt)


def get_cc_star_fqdns_prod():
    return get_cc_star_fqdns_from(_topology_host)


def get_cc_star_fqdns_itb():
    return get_cc_star_fqdns_from(_topology_itb_host)


def usage():
    print("Usage: %s [--itb|--host HOST]" % os.path.basename(__file__))
    sys.exit(0)


def main(args):
    host = _topology_host
    if args == ['--itb']:
        host = _topology_itb_host
    elif len(args) == 2 and args[0] == "--host":
        host = args[1]
    elif args:
        usage()

    for fqdn in get_cc_star_fqdns_from(host):
        print(fqdn)


if __name__ == '__main__':
    main(sys.argv[1:])


