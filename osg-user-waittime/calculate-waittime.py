from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A, Q
import datetime
import pandas as pd
import dateutil.parser as parser

GRACC = "https://gracc.opensciencegrid.org/q"

HOUR = 3600
def getProjectsPerDay():

    endtime = datetime.datetime.now()
    # 6 months back
    starttime = endtime - datetime.timedelta(days=182)
    es = Elasticsearch(
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
            & (Q("term", ProbeName="condor:login04.osgconnect.net") | 
               Q("term", ProbeName="condor:login05.osgconnect.net"))
        ],
    )

    bkt = s.aggs
    bkt = bkt.bucket("EndTime", A('date_histogram', field="EndTime", calendar_interval="1d"))
    bkt = bkt.bucket("ProjectName", A("terms", size=MAXSZ, field="ProjectName"))
    bkt.metric("CoreHours", 'sum', field="CoreHours", missing=0)

    print(s.to_dict())
    response = s.execute()

    results_dict = {}
    for bucket in response.aggregations["EndTime"]["buckets"]:
        date = bucket["key"]
        results_dict[date] = {}
        for project in bucket['ProjectName']['buckets']:
            projectname = project['key']
            results_dict[date][projectname] = project['CoreHours']['value']
        #print(bucket.to_dict())

    df = pd.DataFrame(results_dict)
    df = df.fillna(0)
    df.columns = pd.to_datetime([x*1000000 for x in df.columns])
    return df
    #print(response.aggregations.to_dict())

    #return {f["key"] for f in response.aggregations["Organization"]["buckets"]}

class ProjectAttributes:
    def __init__(self):
        self.njobs = 0
        self.queuetime = 0
        self.walltime = 0
        self.corehours = 0
        self.starttime = None
        self.endtime = None
        self.idledays = 0
        self.project = ""

def getIdleProjects(perDay: pd.DataFrame):
    """
    Find the projects that had more than 14 days of 0 usage before 1000 hours of usage.

    returns: list(ProjectAttributes)
    """
    project_days = []
    # Loop through each row (person), searching for 2 weeks (14 days) of 0's, then some usage.
    for project, row in perDay.iterrows():
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
                    projectAttr = ProjectAttributes()
                    projectAttr.project = project
                    projectAttr.idledays = numZeros
                    projectAttr.starttime = days[0]
                    projectAttr.endtime = days[-1]
                    project_days.append(projectAttr)
                    tempUsage = 0
                    numZeros = 0
                    days = []
        
    return project_days

def generateRawQuery(project, starttime, endtime):
    """
    Generate the raw query to get all usage for a project between starttime and endtime.
    """
    es = Elasticsearch(
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
    print("Querying for project {} between {} and {}".format(project, starttime, endtime))
    s = s.query(
        "bool",
        filter=[
            Q("range", EndTime={"gte": starttime, "lt": endtime})
            & Q("term", ResourceType="Payload")
            & Q("term", ProjectName=project) 
        ],
    )
    #print(s.to_dict())
    return s




def getQueueTimes(projects):
    """
    Loop through the raw usage to find the first 1000 hours during the interesting "days" for each project.
    Calculate the average time in queue as (EndTime - QueueTime) - WallTime
    """
    # Create a mapping of project to projectAttributes

    projectAttrDict = {}
    for projectAttr in projects:
        if projectAttr.starttime < parser.parse("2021-03-09"):
            #print("Before QueueTime available")
            continue
        found1000 = False
        s = generateRawQuery(projectAttr.project, projectAttr.starttime, projectAttr.endtime)
        for record in s.scan():
            projectAttr.njobs += 1
            projectAttr.walltime += record['WallDuration']
            projectAttr.corehours += record['CoreHours']
            if 'QueueTime' in record:
                projectAttr.queuetime += (parser.parse(record['EndTime']).timestamp() - parser.parse(record['QueueTime']).timestamp()) - record['WallDuration']
            else:
                print("QueueTime not found when it should be for probe:{}")
            # check if we have 1000 hours
            if projectAttr.corehours > 1000:
                found1000 = True
                break
        if found1000:
            print("Found the project: {} with QueueTime (hours): {} for CoreHours: {}".format(projectAttr.project, projectAttr.queuetime/HOUR, projectAttr.corehours))
        else:
            print("Did not find 1000 hours of usage for project: {} in set of days: {}-{}".format(projectAttr.project, setDays[0], setDays[-1]))

    return projects




def main():
    # Find all projects with any usage in the last 1 month
    perDay = getProjectsPerDay()

    # Gather per-day statistics for each project
    projects = getIdleProjects(perDay)

    # Queue times
    queueTimes = getQueueTimes(projects)
    columnNames = ["ProjectName", "Start Time", "End Time", "Hours In Queue", "Core Hours", "Idle Days"]
    df = pd.DataFrame(columns = columnNames)
    i = 0
    for projectAttr in queueTimes:
        df.loc[i] = [projectAttr.project, projectAttr.starttime, projectAttr.endtime, projectAttr.queuetime/HOUR, projectAttr.corehours, projectAttr.idledays]
        i+= 1
    
    df = df.loc[df["Core Hours"] > 0]

    with open("output.csv", "w") as output_csv:
        output_csv.write(df.to_csv(index=False))
    print(df.to_csv(index=False))



if __name__ == "__main__":
    main()