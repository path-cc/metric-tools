#!/usr/bin/env python3

# osg-cpu-hours (the artist formerly known as "bumbly")

# generates various Core Hours metrics including those in the dashboard panels
# at the bottom of https://gracc.opensciencegrid.org/

import calendar
import re
import sys
import json
import time
import datetime
import collections
import argparse
import elasticsearch
from elasticsearch_dsl import Search, A, Q
from dateutil import parser, relativedelta




def calculate_users(endtime, months):

    gracc_url = 'https://gracc.opensciencegrid.org/q'

    es = elasticsearch.Elasticsearch(
                    [gracc_url], timeout=300, use_ssl=True, verify_certs=True)

    osdf_index = 'xrd-stash*'

    s = Search(using=es, index=osdf_index)

    # For the last 6 months, aggregate by user
    endtimeDate = parser.parse(endtime)
    
    # Round to end of the endtime month
    endtimeDate = endtimeDate.replace(day=calendar.monthrange(endtimeDate.year, endtimeDate.month)[1], hour=23, minute=59, second=59)
    starttimeDate = endtimeDate - relativedelta.relativedelta(months=months)
    s = s.filter('range', **{'@timestamp': {'gte': starttimeDate, 'lte': endtimeDate}})
    s = s.query('match', dirname2='/osgconnect/public')
    bkt = s.aggs
    bkt = bkt.bucket('timestamp', A('date_histogram', field="@timestamp", calendar_interval="1M"))
    bkt = bkt.bucket('logical_dirname', 'terms', field='logical_dirname.keyword', size=1000)
    response = s.execute()
    #print(response.aggregations.to_dict())
    months = {}
    for month in response.aggregations.timestamp.buckets:
        months[month.key_as_string] = set()
        for user in month.logical_dirname.buckets:
            months[month.key_as_string].add(user.key)

    # Sort the months and put in an array
    def sort_months(x):
        return parser.parse(x).timestamp()
    monthsSorted = sorted(months.keys(), key=sort_months)
    #print(monthsSorted)

    # For the most recent month, find users that have not be in the previous months
    lastMonth = monthsSorted[len(monthsSorted)-1]
    #print("Active users in last month: {}".format(len(months[lastMonth])))
    activeUsers = len(months[lastMonth])

    # Calculate the new users by setting the most recent
    # month and subtracting the previous months
    newUsers = months[lastMonth]
    for month in monthsSorted[:-1]:
        newUsers -= months[month]
    
    output = {
        "Number of new users": len(newUsers),
        "New Users (directory paths)": list(newUsers),
        "Active Users": activeUsers,
        "Date Range:": "{} - {}".format(
            endtimeDate.strftime("01 %b %Y"),
            endtimeDate.strftime("%d %b %Y")
        )
    }
    return output



def add_args():

    nowtime = datetime.datetime.now()
    nowtime = nowtime.replace(day=calendar.monthrange(nowtime.year, nowtime.month)[1])
    defaultTime = nowtime.strftime("%Y-%m-%d")
    argsparser = argparse.ArgumentParser(description='Calculate waittime for users')
    argsparser.add_argument("--outputfile", "-o", type=str, help="Output File", default="output.txt")
    argsparser.add_argument("--endtime", type=str, help="End Time, for example {}, will be rounded to the nearest month".format(defaultTime), default=defaultTime)
    argsparser.add_argument("--months", type=int, help="Number of months to look back.  If a user is found in the previous months, they do not count as a new user for the month.", default=6)
    return argsparser

def main():
    args = add_args().parse_args()
    output = calculate_users(args.endtime, args.months)

    with open(args.outputfile, 'w') as outfile:
        outfile.write(json.dumps(output, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
