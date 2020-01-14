#!/usr/bin/python

import json
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

def cpu_hours_for_window(days, extra_filters=None):
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

    if extra_filters is not None:
        filters = filters & extra_filters

    s = s.query('bool', filter=[filters])

    #curBucket =
    s.aggs.bucket('CoreHours', 'sum', field='CoreHours')
    s.aggs.bucket('FQDN_count', 'cardinality', field='OIM_FQDN')

    resp = s.execute()
    aggs = resp.aggregations
    return int(aggs.CoreHours.value), aggs.FQDN_count.value

def cpu_hours_for_window_gpus(days):
    extra_filters = Q('range', GPUs={'gte': 1})
    return cpu_hours_for_window(days, extra_filters)

def get_table_data(fn):
    hours, fqdn_counts = zip(*map(fn, [30, 90, 365]))
    return {'hours': map("{:,}".format, hours), 'fqdn_counts': fqdn_counts}

def main():
    windows = [30, 90, 365]
    hours_all, fqdn_counts_all = zip(*map(cpu_hours_for_window, windows))
    hours_gpu, fqdn_counts_gpu = zip(*map(cpu_hours_for_window_gpus, windows))
    data = {
        'hours_all': map("{:,}".format, hours_all),
        'hours_gpu': map("{:,}".format, hours_gpu),
        'fqdn_counts_all': fqdn_counts_all,
        'fqdn_counts_gpu': fqdn_counts_gpu
    }
    print json.dumps(data)

if __name__ == '__main__':
    main()

