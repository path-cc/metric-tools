#!/usr/bin/python

import datetime
import elasticsearch
from elasticsearch_dsl import Search, A, Q

gracc_url = 'https://gracc.opensciencegrid.org/q'

es = elasticsearch.Elasticsearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True,
                ca_certs='/etc/ssl/certs/ca-bundle.crt')

jobs_raw_index = 'gracc.osg.raw-*'
jobs_summary_index = 'gracc.osg.summary'

def cpu_hours_for_window(days):

    s = Search(using=es, index=jobs_summary_index)

    #endtime = datetime.datetime(2020, 1, 8)
    endtime = datetime.datetime.date(datetime.datetime.now())  # midnight today

    starttime =  endtime - datetime.timedelta(days)
    s = s.query('bool',
            filter=[
             Q('range', EndTime={'gte': starttime, 'lt': endtime })
          &  Q('term',  ResourceType='Batch')
          & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
          & ~Q('terms', VOName=['Unknown', 'unknown', 'other'])
        ]
    )

    curBucket = s.aggs.bucket('CoreHours', 'sum', field='CoreHours')

    resp = s.execute()
    return int(resp.aggregations.CoreHours.value) 

for x in (7,30,90):
    print x, cpu_hours_for_window(x)
