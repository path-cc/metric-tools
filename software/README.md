# HTCSS Sofware Metrics

Tools for generating PATh software metric. These scripts leverage data provided
in our public Jira system.

Our metrics are recorded on a weekly basis in a shared Google Drive spreadsheet
called **PATh Metrics Tracking** (please ask any of the PATh staff if you need
access to this spreadsheet). At the end of each week, it is expected that 
someone will run the scripts below and then manually enter the results into
the appropriate column under the "NSF Weekly Metrics" sheet. At the end of
each month, we are responsible for aggregating the weekly data into the
"NSF Metrics" sheet.

## Setup

These scripts can be run from any computer with an internet connection.

Requirements:
* Python 3.6 or higher
* Python Jira module, can be installed with: `pip3 install jira`


## M1.1: 80% of distinct changes (as measured by individual tickets) are code reviewed.

The **m1-1-code-reviews.py** script measures our percent of distinct changes,
taking three input arguments:
* **--startdate**: Beginning of the date range in YYYY-MM-DD format
* **--enddate**: End of the date range in YYYY-MM-DD format
* **--detailed**: (optional) Outputs detailed information about which issues
were counted and which ones were code-reviewed.

Example:
```
$ ./m1-1-code-reviews.py --startdate 2021-02-08 --enddate 2021-02-12

Between 2021-02-08 and 2021-02-12:

3 HTCONDOR issues were marked Done
2 of these completed issues were code reviewed
Code review rate: 66.6%
```


## M1.2: 3 new HPC sites supported by Annex technology per year during Years 1 and 2.

This metric is not automated and will need to be updated manually.


## M1.3: At least 30% of the Software Team’s effort goes into the development of new features.

The **m1-3-new-features.py** script measures what percertage of the Software
Team's effort goes into development of new features. It takes four input
arguments:
* **--startdate**: Beginning of the date range in YYYY-MM-DD format
* **--enddate**: End of the date range in YYYY-MM-DD format
* **--efforthouts**: The total number of effort hours worked by the software
team in the date range provided. This is calculated manually by taking the 
number of developers, multipled by their indivdual % effort working on software
during a 40-hour work week, minus any time off for vacations or statuatory
holidays.
* **--detailed**: (optional) Outputs detailed information about how much time
by logged by which developer, on which date and on what issue.

Example:
```
$ ./m1-3-new-features.py --startdate 2021-02-08 --enddate 2021-02-12 --efforthours 440

Between 2021-02-08 and 2021-02-12:

Mark Coatsworth logged 3.0 hours
Carl Edquist logged 0 hours
Jaime Frey logged 30.75 hours
John (TJ) Knoeller logged 22.5 hours
Brian Lin logged 0 hours
Todd L Miller logged 14.08 hours
Zach Miller logged 0 hours
Jason Patton logged 2.0 hours
Mat Selmeci logged 1.5 hours
Todd Tannenbaum logged 12.0 hours
Greg Thain logged 1.0 hours
Tim Theisen logged 20.0 hours

Total hours logged to HTCONDOR Improvement issues: 106.83
Total effort hours worked during this time period: 440
Percent effort logged to Improvement issues: 24.28%

```


## M1.4: Less than 5% of active developer tickets that have not been updated within 10 days.

The **m1-4-stale-issues.py** script measures what percertage of the active
developer tickets have not been updated withint 10 days. It takes two input
arguments:
* **--date**: The date by which issues not updated within the previous 10 days
are considered stale.
* **--detailed**: (optional) Outputs detailed information about how which 
tickets are still open and which ones are considered stale.

Example:
```
$ ./m1-4-stale-issues.py --date 2021-02-12

As of 2021-02-12:

61 issues are open
27 open issues have not been updated in the last 10 days
Percent open issues that are stale: 44.26%
```


## M1.5: Number of new message threads in the ‘htcondor-users’ email list that have not received an initial response within 3 days should be below 10%.

This metric needs to be calculated manually. We have a spreadsheet titled
**PATh htcondor-users Email Metrics** that tracks all htcondor-users emails
along with received/response dates and which ones are counted towards this
metric. Some general rules:
* Announcements are other transactional emails should be not be counted toward
this metric.
* The 3-day window includes weekends; so if an email was received on a Friday
and not replied by Monday evening, it counts as a non-reply.
* The initial reply does not need to come from a member of the PATh team.