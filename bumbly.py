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

CC_star_fqdns = [
    "hosted-ce22.grid.uchicago.edu",
    "hosted-ce30.grid.uchicago.edu",
    "pearc-ce-2.grid.uchicago.edu",
    "hosted-ce33.grid.uchicago.edu"
]

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
    last30d = cpu_hours_for_window(30)
    last90d = cpu_hours_for_window(90)
    last365d = cpu_hours_for_window(365)

    html = """\
<!DOCTYPE html>
<html>
<head>
<title>OSG CPU Hours</title>
<style>
table {{font-family: monospace}}
td {{text-align: center}}
</style>
</head>
<body>
<h2>OSG CPU Hours for CC*</h2>
<table border=1>
<tr>
<th>Last 30 Days</th>
<th>Last 90 Days</th>
<th>Last 365 Days</th>
</tr>
<tr>
<td>{last30d:,}</td>
<td>{last90d:,}</td>
<td>{last365d:,}</td>
</tr>
</table>
</body>
</html>""".format(**locals())

    print(html)

if __name__ == '__main__':
    main()

