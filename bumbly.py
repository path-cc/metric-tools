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

def cpu_hours_for_window(days):
    s = Search(using=es, index=jobs_summary_index)
    endtime = datetime.datetime.date(datetime.datetime.now()) # midnight today
    starttime =  endtime - datetime.timedelta(days)

    s = s.query('bool',
            filter=[
             Q('range', EndTime={'gte': starttime, 'lt': endtime })
          &  Q('term',  ResourceType='Batch')
          &  Q('terms', OIM_FQDN=CC_star_fqdns)
          & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
          & ~Q('terms', VOName=['Unknown', 'unknown', 'other'])
        ]
    )

    curBucket = s.aggs.bucket('CoreHours', 'sum', field='CoreHours')

    resp = s.execute()
    return int(resp.aggregations.CoreHours.value) 

def main():
    hours = map(cpu_hours_for_window, [30, 90, 365])
    print json.dumps(map("{:,}".format, hours))

if __name__ == '__main__':
    main()

