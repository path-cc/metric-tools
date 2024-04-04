#!/usr/bin/env python3

# connect-origin-users

# Calculate the number of new and active users for the OSG Connect origin

import calendar
import re
import sys
import json
import time
import datetime
import collections
import argparse
import opensearchpy
from opensearchpy import Search, A, Q
from dateutil import parser, relativedelta




def calculate_users(endtime, months):

    gracc_url = 'https://gracc.opensciencegrid.org/q'

    es = opensearchpy.OpenSearch(
                    [gracc_url], timeout=300, use_ssl=True, verify_certs=True)

    osdf_index = 'xrd-stash*'

    s = Search(using=es, index=osdf_index)

    # For the last 6 months, aggregate by user
    endtimeDate = parser.parse(endtime)
    
    # Round to end of the endtime month
    endtimeDate = endtimeDate.replace(day=calendar.monthrange(endtimeDate.year, endtimeDate.month)[1], hour=23, minute=59, second=59)
    starttimeDate = endtimeDate - relativedelta.relativedelta(months=months)
    #print("Start time: {}".format(starttimeDate))
    #print("End time: {}".format(endtimeDate))
    s = s.filter('range', **{'@timestamp': {'gte': starttimeDate, 'lte': endtimeDate}})

    # Have to use the odd dirname1__keyword so that it matches exactly '/ospool', otherwise
    # it can match substrings such as just ospool
    # Also, remove the monitoring directory
    q = Q('match', dirname1__keyword='/ospool') & ~Q('match', dirname2__keyword='/ospool/monitoring')
    s = s.query(q)
    bkt = s.aggs
    bkt = bkt.bucket('timestamp', A('date_histogram', field="@timestamp", calendar_interval="1M"))
    bkt = bkt.bucket('logical_dirname', 'terms', field='logical_dirname.keyword', size=10000)
    #print(s.to_dict())
    response = s.execute()
    #print(response.aggregations.to_dict())
    monthSets = {}
    for month in response.aggregations.timestamp.buckets:
        monthSets[month.key_as_string] = set()
        for user in month.logical_dirname.buckets:
            monthSets[month.key_as_string].add(user.key)

    # Sort the months and put in an array
    def sort_months(x):
        return parser.parse(x).timestamp()
    monthsSorted = sorted(monthSets.keys(), key=sort_months)
    #print(monthsSorted)

    # For the most recent month, find users that have not be in the previous months
    lastMonth = monthsSorted[len(monthsSorted)-1]
    #print("Last month: {}".format(lastMonth))
    #print("Active users in last month: {}".format(len(months[lastMonth])))
    activeUsers = len(monthSets[lastMonth])

    # Calculate the new users by setting the most recent
    # month and subtracting the previous months
    #print(monthSets)
    newUsers = monthSets[lastMonth]
    for month in monthsSorted[:-1]:
        newUsers -= monthSets[month]
    
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
