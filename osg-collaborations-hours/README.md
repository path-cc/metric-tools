OSG Collaboration Hours
=======================

**Metric description:** CPU and GPU hours consumed on the OSPool (and collaboration-owned resources) by collaboration-level OSPool projects, broken out by pool.

Process for calculating Collaboration Hours
--------------------------------------------

1. Query Elasticsearch for completed jobs within the reporting period, excluding DAGMan and scheduler universe jobs, and excluding jobs without any runtime.
2. For each collaboration, filter jobs by `ProjectName` to the known project name variants for that collaboration.
3. For collaborations that also run jobs on dedicated or non-OSPool resources (CLAS12, EIC/ePIC, GlueX, IceCube, SPT-3G, XENON), attribute jobs to the appropriate pool based on resource name and OSPool job classification.
4. Sum CPU hours and GPU hours separately per pool..
5. Write results to a CSV file with one row per collaboration-pool combination.

Collaborations reported
-----------------------

| Collaboration | Pools |
|---|---|
| CLAS12 | OSPool, JLAB |
| EIC/ePIC | OSPool, JLAB |
| Event Horizon Telescope | OSPool |
| Future Colliders | OSPool |
| GlueX | OSPool, JLAB |
| IceCube | OSPool, IceCube |
| KOTO | OSPool |
| LIGO-OSG | OSPool |
| MOLLER | OSPool |
| REDTOP | OSPool |
| RNO-G | OSPool |
| Super CDMS | OSPool |
| SoLID | OSPool |
| SPT-3G | OSPool, SPT |
| XENON | OSPool, XENON |

Math behind the metric
-----------------------

Metrics (from HTCondor job ClassAds):
* `RemoteWallClockTime`: Total wall-clock seconds the job executed.
* `RequestCpus` / `CpusProvisioned`: Number of CPUs requested or provisioned
* `RequestGpus` / `GpusProvisioned`: Number of GPUs requested or provisioned

**CPU Hours:**

    CPUs = min(RequestCpus, CpusProvisioned)  [defaults to 1 if absent]
    CPU Hours = CPUs × RemoteWallClockTime / 3600

**GPU Hours** (only for jobs with `RequestGpus > 0`):

    GPUs = min(RequestGpus, GpusProvisioned)  [defaults to 1 if absent]
    GPU Hours = GPUs × RemoteWallClockTime / 3600

OSPool job classification
--------------------------

A job is classified as an OSPool job if:

* Its `ScheddName` matches a known OSPool access point **and** it has no `LastRemotePool`, **or**
* Its `LastRemotePool` matches a known OSPool collector, **and**
* It does not have a `TargetAnnexName` or a `ResourceName` of `"Local Job"` or `"2"`.

For collaborations with dedicated resources, jobs running on known collaboration-specific resource names are attributed to the collaboration's own pool even if they flowed through OSPool infrastructure.

Usage
-----

```
usage: fetch_collab_metrics.py [-h] [--es-host ES_HOST] [--es-url-prefix ES_URL_PREFIX]
                                [--es-index ES_INDEX] [--es-user ES_USER]
                                [--es-password-file ES_PASSWORD_FILE] [--es-use-https]
                                [--es-ca-certs ES_CA_CERTS] [--es-timeout ES_TIMEOUT]
                                [--es-config-file ES_CONFIG_FILE]
                                [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                outfile

positional arguments:
  outfile               Output CSV file path.

options:
  --start YYYY-MM-DD    Start of the reporting period (inclusive). Defaults to yesterday.
  --end YYYY-MM-DD      End of the reporting period (exclusive). Defaults to one day after --start.
  --es-config-file      JSON file containing an object that sets ES connection options.
```

Output CSV columns: `Project, Pool, CPU Hours, CPU Jobs, GPU Hours, GPU Jobs`
