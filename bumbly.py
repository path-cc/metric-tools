#!/usr/bin/python

import re
import sys
import json
import time
import datetime
import collections
import elasticsearch
from elasticsearch_dsl import Search, A, Q

import cc_star_fqdns

gracc_url = 'https://gracc.opensciencegrid.org/q'

es = elasticsearch.Elasticsearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True,
                ca_certs='/etc/ssl/certs/ca-bundle.crt')

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

def cpu_hours_for_window_filters2(days, extra_filters, want_fqdns=False):
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
        s.aggs.bucket('FQDNs', 'terms', field='OIM_FQDN', size=100)
        s.aggs.bucket('Resources', 'terms', field='OIM_Resource', size=100)
        s2 = s.aggs.bucket('FQDNs2', 'terms', field='OIM_FQDN', size=100)
        s2.bucket('Resources', 'terms', field='OIM_Resource', size=100)

    resp = s.execute()
    return resp

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
        s.aggs.bucket('FQDNs', 'terms', field='OIM_FQDN', size=100)
        s.aggs.bucket('Resources', 'terms', field='OIM_Resource', size=100)

        s2 = s.aggs.bucket('FQDNs2', 'terms', field='OIM_FQDN', size=100)
        s2.bucket('Resources', 'terms', field='OIM_Resource', size=100)

    resp = s.execute()
    aggs = resp.aggregations
    if want_fqdns:
        fqdns = sorted( x.key for x in aggs.FQDNs.buckets )
        resources = sorted( x.key for x in aggs.Resources.buckets )
        fqdn_resources = sorted( (fqdn.key, resource.key)
                                 for fqdn in aggs.FQDNs2.buckets
                                 for resource in fqdn.Resources.buckets )
        fqdn_resources = [ "%s (%s)" % fr for fr in fqdn_resources ]
    else:
        fqdns, resources, fqdn_resources = [], [], []
    return (int(aggs.CoreHours.value), aggs.FQDN_count.value,
            fqdns, resources, fqdn_resources)


HoursCount = collections.namedtuple("HoursCount",
        ['hours', 'count', 'fqdns', 'resources', 'fqdn_resources'])


def testy():
    return cpu_hours_for_window_filters2(1, cc_star_gpu_usage, want_fqdns=True)

def get_panel_row(extra_filters, want_fqdns=False):
    windows = [1, 30, 365]
    def cpu_hours_for_window(d):
        return cpu_hours_for_window_filters(d, extra_filters, want_fqdns)

    hours, count, fqdns, resources, fqdn_resources = \
            zip(*map(cpu_hours_for_window, windows))
    return HoursCount(map("{:,}".format, hours), count,
                      fqdns, resources, fqdn_resources)

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
        amnh_resources  = amnh.resources,
        amnh_fqdn_resources  = amnh.fqdn_resources,
        cc_star_usage = cc_star.hours,
        cc_star_count = cc_star.count,
        cc_star_fqdns = cc_star.fqdns,
        cc_star_resources = cc_star.resources,
        cc_star_fqdn_resources = cc_star.fqdn_resources,
        cc_star_gpu_usage = cc_star_gpu.hours,
        cc_star_gpu_count = cc_star_gpu.count,
        cc_star_gpu_fqdns = cc_star_gpu.fqdns,
        cc_star_gpu_resources = cc_star_gpu.resources,
        cc_star_gpu_fqdn_resources = cc_star_gpu.fqdn_resources,
    )


def list_collapse(m):
    return json.dumps(json.loads(m.group()))

def prettyd(d):
    dat = json.dumps(d, indent=1, sort_keys=True)
    #return re.sub(r'(?<=: )\[[^]{}[]*\]', list_collapse, dat)
    return re.sub(r'\[[^]{}[]*\]', list_collapse, dat)


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

    print >>out, prettyd(data)


if __name__ == '__main__':
    main(sys.argv[1:])

