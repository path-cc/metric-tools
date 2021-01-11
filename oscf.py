#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import datetime
import elasticsearch
from   elasticsearch_dsl import Search, A, Q

gracc_url = 'https://gracc.opensciencegrid.org/q'

es = elasticsearch.Elasticsearch(
                [gracc_url], timeout=300, use_ssl=True, verify_certs=True,
                ca_certs='/etc/ssl/certs/ca-bundle.crt')

jobs_raw_index = 'gracc.osg.raw-*'
jobs_summary_index = 'gracc.osg.summary'

def getdate(datestr):
    return datetime.datetime.strptime(datestr, "%Y-%m-%d")


_min_CoreHours = 1.0
def get_oscf_facilities(first_day, last_day):
    s = Search(using=es, index=jobs_summary_index)
    start = getdate(first_day)
    end   = getdate(last_day)
    filters = (
            Q('range', EndTime={'gte': start, 'lte': end })
         &  Q('term',  ResourceType='Batch')
    )
    q = s.query('bool', filter=[filters])
    q.aggs.bucket('Facility',  'terms', size=9999, field='OIM_Facility') \
          .bucket('CoreHours', 'sum',              field='CoreHours')
    resp = q.execute()
    aggs = resp.aggregations
    return set(
        facility.key
        for facility in aggs.Facility.buckets
        if facility.CoreHours.value >= _min_CoreHours
    )


def main(first_day, last_day):
    facilities = sorted(get_oscf_facilities(first_day, last_day))
    n = len(facilities)
    print("%d OSCF Facilit%s for %s through %s:"
          % (n, "y" if n == 1 else "ies", first_day, last_day))
    for f in sorted(facilities):
        print(" - %s" % f)


def usage():
    print("usage: %s BEGIN_DATE END_DATE" % os.path.basename(__file__),
          file=sys.stderr)


if __name__ == '__main__':
    try:
        main(*sys.argv[1:])
    except ValueError:
        usage()

