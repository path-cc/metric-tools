#!/usr/bin/env python3

import opensearchpy
from opensearchpy import Search, A, Q
import datetime
import pandas as pd
import dateutil.parser as parser
import argparse
import collections
import statistics

GRACC = "https://gracc.opensciencegrid.org/q"

HOUR = 3600
MINUTE = 60

# A mapping of usernames to projects
usernameToProject = collections.defaultdict(set)

def getUsersPerDay(starttime: datetime.datetime, endtime: datetime.datetime):
    """
    Get a table of user's usage by day.  The table will have the format:
        Users       day     day+1   day+2   ...     day+n
        <user>      10      0       0       ...     31
    
    Where the usage is in CoreHours.
    """

    # 6 months back
    starttime = starttime - datetime.timedelta(days=15)
    es = opensearchpy.OpenSearch(
        [GRACC],
        timeout=300,
        use_ssl=True,
        verify_certs=True
    )

    MAXSZ = 2 ** 30
    index = "gracc.osg.summary"
    s = Search(using=es, index=index)
    # Starttime and endtime are both datetime objects
    s = s.query(
        "bool",
        filter=[
            Q("range", EndTime={"gte": starttime, "lt": endtime})
            & Q("term", ResourceType="Payload")
            & Q("term", VOName="osg")
            & (Q("term", ProbeName="condor-ap:login04.osgconnect.net") | 
               Q("term", ProbeName="condor-ap:login05.osgconnect.net") |
               Q("term", ProbeName="condor-ap:ap20.uc.osg-htc.org") | 
               Q("term", ProbeName="condor-ap:ap21.uc.osg-htc.org") | 
               Q("term", ProbeName="condor-ap:ap22.uc.osg-htc.org") | 
               Q("term", ProbeName="condor-ap:ap23.uc.osg-htc.org") | 
               Q("term", ProbeName="condor-ap:ap40.uw.osg-htc.org")
            )
        ],
    )

    bkt = s.aggs
    bkt = bkt.bucket("EndTime", A('date_histogram', field="EndTime", calendar_interval="1d"))
    bkt = bkt.bucket("DN", A("terms", size=MAXSZ, field="DN"))
    bkt = bkt.bucket("ProjectName", A("terms", size=MAXSZ, field="ProjectName"))
    bkt.metric("CoreHours", 'sum', field="CoreHours", missing=0)

    response = s.execute()

    results_dict = {}
    for bucket in response.aggregations["EndTime"]["buckets"]:
        date = bucket["key"]
        results_dict[date] = {}
        for user in bucket['DN']['buckets']:
            username = user['key']
            for project in user['ProjectName']['buckets']:
                usernameToProject[username].add(project['key'])
                results_dict[date][username] = project['CoreHours']['value']
        #print(bucket.to_dict())

    df = pd.DataFrame(results_dict)
    df = df.fillna(0)
    df.columns = pd.to_datetime([x*1000000 for x in df.columns])
    return df
    #print(response.aggregations.to_dict())

    #return {f["key"] for f in response.aggregations["Organization"]["buckets"]}

class UserAttributes:
    def __init__(self):
        self.njobs = 0
        self.queuetime = 0
        self.walltime = 0
        self.corehours = 0
        self.starttime = None
        self.endtime = None
        self.idledays = 0
        self.project = ""
        self.username = ""
        self.maxqueue = 0
        self.std = 0
        self.quantiles = []

def getIdleUsers(perDay: pd.DataFrame):
    """
    Find the users that had more than 14 days of 0 usage before 1000 hours of usage.

    returns: list(UserAttributes)
    """
    user_days = []
    # Loop through each row (person), searching for 2 weeks (14 days) of 0's, then some usage.
    for user, row in perDay.iterrows():
        numZeros = 0
        tempUsage = 0
        days = []
        # For each day in the search period
        for day, value in row.iteritems():
            if value == 0:
                numZeros += 1
            elif numZeros > 14:
                # Found sudden usage after more than 2 weeks of no usage
                
                tempUsage += value
                days.append(day)
                if tempUsage >= 1000:
                    userAttr = UserAttributes()
                    userAttr.username = user
                    userAttr.idledays = numZeros
                    userAttr.starttime = days[0]
                    userAttr.endtime = days[-1]
                    user_days.append(userAttr)
                    tempUsage = 0
                    numZeros = 0
                    days = []
        
    return user_days

def generateRawQuery(user, starttime, endtime):
    """
    Generate the raw query to get all usage for a user between starttime and endtime.
    """
    es = opensearchpy.OpenSearch(
        [GRACC],
        timeout=300,
        use_ssl=True,
        verify_certs=True
    )
    endtime = endtime + datetime.timedelta(days=1)
    MAXSZ = 2 ** 30
    index = "gracc.osg.raw*"
    s = Search(using=es, index=index)
    # Starttime and endtime are both datetime objects
    print("Querying for user {} between {} and {}".format(user, starttime, endtime))
    s = s.query(
        "bool",
        filter=[
            Q("range", EndTime={"gte": starttime, "lt": endtime})
            & Q("term", ResourceType="Payload")
            & Q("term", DN=user) 
        ],
    )
    s = s.sort("StartTime")
    #print(s.to_dict())
    return s




def getQueueTimes(users):
    """
    Loop through the raw usage to find the first 1000 hours during the interesting "days" for each user.
    Calculate the average time in queue as (EndTime - QueueTime) - WallTime
    """
    # Create a mapping of user to userAttributes
    userAttrDict = {}
    for userAttr in users:
        if userAttr.starttime < parser.parse("2021-03-09"):
            #print("Before QueueTime available")
            continue
        found1000 = False
        s = generateRawQuery(userAttr.username, userAttr.starttime, userAttr.endtime)
        s = s.params(preserve_order=True)
        queuetimes = []
        for record in s.scan():
            userAttr.njobs += 1
            userAttr.walltime += record['WallDuration']
            userAttr.corehours += record['CoreHours']
            queuetime = 0
            if 'QueueTime' in record:
                queuetime = (parser.parse(record['EndTime']).timestamp() - parser.parse(record['QueueTime']).timestamp()) - record['WallDuration']
                userAttr.queuetime += queuetime
            else:
                print("QueueTime not found when it should be for probe:{}")

            queuetimes.append(queuetime)
            # check if we have 1000 hours
            if userAttr.corehours > 1000:
                found1000 = True
                userAttr.maxqueue = max(queuetimes)
                if (userAttr.njobs > 1):
                    userAttr.std = statistics.stdev(queuetimes)
                    userAttr.quantiles = statistics.quantiles(queuetimes, n=10)
                break
        if found1000:
            print("Found the user: {} with QueueTime (hours): {} for CoreHours: {}".format(userAttr.username, userAttr.queuetime/HOUR, userAttr.corehours))
        else:
            print("Did not find 1000 hours in {} jobs of usage for user: {} in set of days: {}-{}".format(userAttr.njobs, userAttr.username, userAttr.starttime, userAttr.endtime))

    return users



def add_args():

    argsparser = argparse.ArgumentParser(description='Calculate waittime for users')
    argsparser.add_argument("outputfile", type=str, help="Output File")
    argsparser.add_argument("starttime", type=str, help="Start Time, for example 2021-03-01")
    argsparser.add_argument("endtime", type=str, help="End Time, for example 2021-03-31")
    return argsparser

def main():
    argsparser = add_args()
    args = argsparser.parse_args()

    startDatetime = parser.parse(args.starttime)
    endDatetime = parser.parse(args.endtime)

    # Find all user with any usage in the last 1 month
    perDay = getUsersPerDay(startDatetime, endDatetime)

    # Gather per-day statistics for each user
    users = getIdleUsers(perDay)

    # Queue times
    queueTimes = getQueueTimes(users)
    columnNames = ["Username", 
                   "ProjectName", 
                   "Start Time", 
                   "End Time", 
                   "Days of Zero Usage", 
                   "Aggregate Hours In Queue", 
                   "Max Minutes In Queue", 
                   "Average Minutes In Queue", 
                   "Standard Deviation in Minutes",
                   "90% Queue Time in Minutes", 
                   "Aggregate Core Hours", 
                   "Number of Jobs"]
    df = pd.DataFrame(columns = columnNames)
    for i, userAttr in enumerate(queueTimes):
        df.loc[i] = [userAttr.username, 
                     ",".join(usernameToProject[userAttr.username]), 
                     userAttr.starttime, 
                     userAttr.endtime, 
                     userAttr.idledays, 
                     userAttr.queuetime/HOUR, 
                     userAttr.maxqueue/MINUTE, 
                     (userAttr.queuetime/userAttr.njobs)/MINUTE,  # "Average Minutes In Queue", 
                     userAttr.std / MINUTE, # "Standard Deviation in Minutes"
                     userAttr.quantiles[8]/MINUTE if userAttr.njobs > 1 else 0,  # "90% Queue Time in Minutes"
                     userAttr.corehours, 
                     userAttr.njobs]
    
    df = df.loc[df["Aggregate Core Hours"] > 0]

    def replaceCN(user):
        return user.replace("/OU=LocalUser/CN=", "")
    df["Username"] = df["Username"].apply(replaceCN)

    with open(args.outputfile, "w") as output_csv:
        output_csv.write(df.to_csv(index=False))



if __name__ == "__main__":
    main()
