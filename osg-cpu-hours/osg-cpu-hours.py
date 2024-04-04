#!/usr/bin/env python3

# osg-cpu-hours (the artist formerly known as "bumbly")

# generates various Core Hours metrics including those in the dashboard panels
# at the bottom of https://gracc.opensciencegrid.org/

import re
import sys
import json
import time
import datetime
import collections
import opensearchpy
from opensearchpy import Search, A, Q

import cc_star_fqdns

gracc_url = 'https://gracc.opensciencegrid.org/q'

es = opensearchpy.OpenSearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True)

jobs_raw_index = 'gracc.osg.raw-*'
jobs_summary_index = 'gracc.osg.summary'

CC_star_fqdns = cc_star_fqdns.get_cc_star_fqdns_prod()



# now for queries matching gracc dashboard

osg_connect = (
       Q('term', ResourceType='Payload')
    &  Q('term', ReportableVOName='osg')
    &  Q('wildcard', OIM_Organization='*')
)

multi_inst = (
       Q('term',  ResourceType='Payload')
    &  Q('terms', ReportableVOName=['SBGrid', 'des', 'dune', 'fermilab',
                                    'gluex', 'icecube', 'ligo', 'lsst'])
    &  Q('wildcard', OIM_Organization='*')
)

campus_orgs = (
       Q('term', ResourceType='Batch')
    &  Q('terms', VOName=['hcc', 'glow', 'suragrid'])
)

gpu_usage = (
       Q('term',  ResourceType='Payload')
    &  Q('range', GPUs={'gte': 1})
)

cc_star_gpu_usage = (
       Q('term',  ResourceType='Payload')
    &  Q('range', GPUs={'gte': 1})
    &  Q('terms', OIM_FQDN=CC_star_fqdns)
)


amnh_usage = (
       Q('term',  ResourceType='Batch')
    &  Q('term',  OIM_Site='AMNH')
    &  Q('terms', OIM_FQDN=CC_star_fqdns)
)

cc_star_usage = (
       Q('term',  ResourceType='Batch')
    &  Q('terms', OIM_FQDN=CC_star_fqdns)
)


def cpu_hours_for_window_filters(days, extra_filters, want_fqdns=False):
    s = Search(using=es, index=jobs_summary_index)
    #endtime = datetime.datetime.now() - datetime.timedelta(1)
    endtime = datetime.datetime.date(datetime.datetime.now()) # midnight today
    starttime = endtime - datetime.timedelta(days)

    filters = (
            Q('range', EndTime={'gte': starttime, 'lt': endtime })
         &  extra_filters
    )

    s = s.query('bool', filter=[filters])

    s.aggs.bucket('CoreHours', 'sum', field='CoreHours')
    s.aggs.bucket('FQDN_count', 'cardinality', field='OIM_FQDN')
    if want_fqdns:
        s.aggs.bucket('FQDNs',     'terms', field='OIM_FQDN',     size=1000) \
              .bucket('Resources', 'terms', field='OIM_Resource', size=1000)

    resp = s.execute()
    aggs = resp.aggregations
    if want_fqdns:
        fqdns = sorted( "%s (%s)" % (resource.key, fqdn.key)
                        for fqdn in aggs.FQDNs.buckets
                        for resource in fqdn.Resources.buckets )
    else:
        fqdns = []
    return int(aggs.CoreHours.value), aggs.FQDN_count.value, fqdns


HoursCount = collections.namedtuple("HoursCount", ['hours', 'count', 'fqdns'])


def get_panel_row(extra_filters, want_fqdns=False):
    windows = [1, 30, 365]
    def cpu_hours_for_window(d):
        return cpu_hours_for_window_filters(d, extra_filters, want_fqdns)

    hours, count, fqdns = zip(*map(cpu_hours_for_window, windows))
    return HoursCount(list(map("{:,}".format, hours)), count, fqdns)

def m2():
    amnh        = get_panel_row(amnh_usage, want_fqdns=True)
    cc_star     = get_panel_row(cc_star_usage, want_fqdns=True)
    cc_star_gpu = get_panel_row(cc_star_gpu_usage, want_fqdns=True)

    all_non_lhc = osg_connect | multi_inst | campus_orgs

    return dict(
        osg_connect = get_panel_row(osg_connect).hours,
        multi_inst  = get_panel_row(multi_inst).hours,
        campus_orgs = get_panel_row(campus_orgs).hours,
        gpu_usage   = get_panel_row(gpu_usage).hours,
        all_non_lhc = get_panel_row(all_non_lhc).hours,
        amnh_usage  = amnh.hours,
        amnh_count  = amnh.count,
        amnh_fqdns  = amnh.fqdns,
        cc_star_usage = cc_star.hours,
        cc_star_count = cc_star.count,
        cc_star_fqdns = cc_star.fqdns,
        cc_star_gpu_usage = cc_star_gpu.hours,
        cc_star_gpu_count = cc_star_gpu.count,
        cc_star_gpu_fqdns = cc_star_gpu.fqdns,
    )


def main(args):
    unix_ts = int(time.time())
    human_ts = time.strftime("%F %H:%M", time.localtime(unix_ts))
    data = dict(
        last_update = unix_ts,
        last_update_str = human_ts,
        **m2()
    )

    if len(args) == 2 and args[0] == '-o':
        out = open(args[1], "w")
    else:
        out = sys.stdout

    print(json.dumps(data, indent=1, sort_keys=True), file=out)


if __name__ == '__main__':
    main(sys.argv[1:])

