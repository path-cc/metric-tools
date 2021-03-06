OSG Project Waittime
=================

**Metric description:** Aggregate wait time for jobs comprising the first 1,000 (100) core (GPU) hours consumed in the Open Science Pool for projects with no consumption in the previous two weeks.

Process for calculating Queue Time
----------------------------------

1. Query GRACC for the last 6 months of payload data for login*.osgconnect.net hosts by Project and day.
2. For each project with usage in the last 6 months, search for 14 days of zero usage before 1000 hours.  Mark those days that total 1000 hours after 14 days of 0 usage.
3. For the marked days in #2, query the raw GRACC data from those marked days, ordered by **StartTime**.
4. For the first 1000 hours in the marked days, calculate and aggregate the jobs queue time and core hours.


Math behind the metric
----------------------

Metrics:
* EndTime: Timestamp that the job ended
* QueueTime: Timestamp that the job entered the queue (was submitted)
* WallDuration: Number of seconds that the job executed.
* CoreHours: WallDuration * 3600 * Processors allocated

A simple job's lifetime:

    ---  In Queue
    +++  Job Running

                             WallDuration
                      | - - - - - - - - - - - -|
    ------------------++++++++++++++++++++++++++
    \                 \                         \
     Job submitted     Job starts                Job exits
      (QueueTime)                                (EndTime)

A complicated job's lifetime:

                          WallDuration                        WallDuration (added to previous)
                      | - - - - - - - - - -|               | - - - - - - - - - |
    ------------------++++++++++++++++++++++---------------+++++++++++++++++++++
    \                 \                     \               \                   \
     Job submitted     Job starts            Job Preempted   Job starts          Job exists
      (QueueTime)                                                                (EndTime)



The calculation for the total time a job was in queue is:

    TimeInQueue = EndTime - QueueTime - WallDuration

Breaking down the calculation:

    TimeInSystem = EndTime - QueueTime

Total time that the job was in the system.  Subtracting the time that the job is spent executing should leave the amount of time that the job was in queue.

This calculation may be inaccurate if the job is in the hold state during it's lifetime.  In these calcualations, time in hold state will be counted as in queue.

