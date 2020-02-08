#!/usr/bin/python

import json
import time
import datetime
import elasticsearch
from elasticsearch_dsl import Search, A, Q

gracc_url = 'https://gracc.opensciencegrid.org/q'

es = elasticsearch.Elasticsearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True,
                ca_certs='/etc/ssl/certs/ca-bundle.crt')

jobs_raw_index = 'gracc.osg.raw-*'
jobs_summary_index = 'gracc.osg.summary'

CC_star_fqdns = [
    "hosted-ce22.grid.uchicago.edu",
    "hosted-ce30.grid.uchicago.edu",
    "pearc-ce-2.grid.uchicago.edu",
    "hosted-ce33.grid.uchicago.edu"
]

#CC_star_probenames = [ "*:%s" % f for f in CC_star_fqdns ]
#
#WC = reduce((lambda a,b:a|b),
#            ( Q('wildcard', ProbeName=p) for p in CC_star_probenames ))

def cpu_hours_for_window(days):
    s = Search(using=es, index=jobs_summary_index)
    endtime = datetime.datetime.date(datetime.datetime.now()) # midnight today
    starttime =  endtime - datetime.timedelta(days)

    filters = (
            Q('range', EndTime={'gte': starttime, 'lt': endtime })
         &  Q('term',  ResourceType='Batch')
         &  Q('terms', OIM_FQDN=CC_star_fqdns)
         & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
         & ~Q('terms', VOName=['Unknown', 'unknown', 'other'])
    )

    s = s.query('bool', filter=[filters])

    s.aggs.bucket('CoreHours', 'sum', field='CoreHours')
    s.aggs.bucket('FQDN_count', 'cardinality', field='OIM_FQDN')

    resp = s.execute()
    aggs = resp.aggregations
    return int(aggs.CoreHours.value), aggs.FQDN_count.value

def gpu_hours_for_window(days):
    s = Search(using=es, index=jobs_summary_index)
    endtime = datetime.datetime.date(datetime.datetime.now()) # midnight today
    starttime =  endtime - datetime.timedelta(days)

    filters = (
            Q('range', EndTime={'gte': starttime, 'lt': endtime })
         &  Q('range', GPUs={'gte': 1})
         &  Q('term',  ResourceType='Batch')
         &  Q('terms', OIM_FQDN=CC_star_fqdns)
         & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
         & ~Q('terms', VOName=['Unknown', 'unknown', 'other'])
    )

    s = s.query('bool', filter=[filters])

    curBucket = s.aggs.bucket('GPUs', 'terms', field='GPUs')
    curBucket.bucket('WallDuration', 'sum', field='WallDuration')
    s.aggs.bucket('FQDN_count', 'cardinality', field='OIM_FQDN')

    resp = s.execute()
    aggs = resp.aggregations

    gpu_hours = int(sum( b.WallDuration.value * int(b.key) / 3600.0
                         for b in aggs.GPUs.buckets ))

    return gpu_hours, aggs.FQDN_count.value

def get_table_data(fn):
    hours, fqdn_counts = zip(*map(fn, [30, 90, 365]))
    return {'hours': map("{:,}".format, hours), 'fqdn_counts': fqdn_counts}


# now for queries matching gracc dashboard

osg_connect = (
       Q('term', ResourceType='Payload')
    &  Q('term', ReportableVOName='osg')
)

multi_inst = (
       Q('term',  ResourceType='Payload')
    &  Q('terms', ReportableVOName=['SBGrid', 'des', 'dune', 'fermilab',
                                    'gluex', 'icecube', 'ligo', 'lsst'])
)

campus_orgs = (
       Q('term', ResourceType='Batch')
    &  Q('terms', VOName=['hcc', 'glow', 'suragrid'])
)

gpu_usage = (
       Q('term',  ResourceType='Payload')
    &  Q('range', GPUs={'gte': 1})
)

def cpu_hours_for_window_filters(days, extra_filters):
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

    resp = s.execute()
    aggs = resp.aggregations
    return int(aggs.CoreHours.value)

def get_panel_row(extra_filters):
    windows = [1, 30, 365]
    d1 = [ cpu_hours_for_window_filters(d, extra_filters) for d in windows ]
    return map("{:,}".format, d1)

def m2():
    return dict(
        osg_connect = get_panel_row(osg_connect),
        multi_inst  = get_panel_row(multi_inst),
        campus_orgs = get_panel_row(campus_orgs),
        gpu_usage   = get_panel_row(gpu_usage),
        all_non_lhc = get_panel_row(osg_connect | multi_inst | campus_orgs),
    )

def main():
    windows = [30, 90, 365]
    hours_all, fqdn_counts_all = zip(*map(cpu_hours_for_window, windows))
    hours_gpu, fqdn_counts_gpu = zip(*map(gpu_hours_for_window, windows))
    unix_ts = int(time.time())
    data = dict(
        hours_all = map("{:,}".format, hours_all),
        hours_gpu = map("{:,}".format, hours_gpu),
        fqdn_counts_all = fqdn_counts_all,
        fqdn_counts_gpu = fqdn_counts_gpu,
        last_update = unix_ts,
        **m2()
    )
    print json.dumps(data)


if __name__ == '__main__':
    main()

