"""
Fetch CPU and GPU usage metrics for collaboration-level OSPool projects from Elasticsearch.

Output is CSV with columns: Project, Pool, CPU Hours, CPU Jobs, GPU Hours, GPU Jobs.

NOTE: Currently requires direct access to the internal Elasticsearch instance.
Will be updated to support a Tiger-based Elasticsearch service.
"""

from __future__ import annotations

import csv
import sys
import json
import argparse

from datetime import datetime, timedelta
from pathlib import Path
from copy import deepcopy

import elasticsearch
from elasticsearch_dsl import Search, A, Q

from metric_functions import get_ospool_aps, OSPOOL_COLLECTORS, print_es_error, short, valid_date, connect


PICKLED_AP_COLLECTOR_HOSTS_CACHE_FILE = "ospool_ap_collectorhost.pickle"

ELASTICSEARCH_ARGS = {
    "--es-host": {"default": "localhost:9200"},
    "--es-url-prefix": {},
    "--es-index": {},
    "--es-user": {},
    "--es-password-file": {"type": Path},
    "--es-use-https": {"action": "store_true"},
    "--es-ca-certs": {},
    "--es-timeout": {"type": int, "default": 120},
    "--es-config-file": {
        "type": Path,
        "help": "JSON file containing an object that sets above ES options",
    }
}

RESOURCE_NAME_SCRIPT_SRC = """
String res;
if (doc.containsKey("MachineAttrGLIDEIN_ResourceName0") && doc["MachineAttrGLIDEIN_ResourceName0.keyword"].size() > 0) {
    res = doc["MachineAttrGLIDEIN_ResourceName0.keyword"].value;
    emit(res);
} else if (doc.containsKey("MATCH_EXP_JOBGLIDEIN_ResourceName") && doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].size() > 0) {
    res = doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].value;
    emit(res);
}
"""

PROJECT_NAME_SCRIPT_SRC = """
String proj;
if (doc.containsKey("ProjectName") && doc["ProjectName.keyword"].size() > 0) {
    proj = doc["ProjectName.keyword"].value.toLowerCase();
    emit(proj);
}
"""

CPU_HOURS_SCRIPT_SRC = """
long cpus = 1;
long wallclocktime = 0;
if (doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0) {
    cpus = doc["RequestCpus"].value;
}
if (doc.containsKey("CpusProvisioned") && doc["CpusProvisioned"].size() > 0 && doc["CpusProvisioned"].value < cpus) {
    cpus = doc["CpusProvisioned"].value;
}
if (doc.containsKey("RemoteWallClockTime") && doc["RemoteWallClockTime"].size() > 0) {
    wallclocktime = doc["RemoteWallClockTime"].value;
}
emit((double)cpus * ((double)wallclocktime / (double)3600));
"""


GPU_HOURS_SCRIPT_SRC = """
long gpus = 1;
long wallclocktime = 0;
if (doc.containsKey("RequestGpus") && doc["RequestGpus"].size() > 0) {
    gpus = doc["RequestGpus"].value;
}
if (doc.containsKey("GpusProvisioned") && doc["GpusProvisioned"].size() > 0 && doc["GpusProvisioned"].value < gpus) {
    gpus = doc["GpusProvisioned"].value;
}
if (doc.containsKey("RemoteWallClockTime") && doc["RemoteWallClockTime"].size() > 0) {
    wallclocktime = doc["RemoteWallClockTime"].value;
}
emit((double)gpus * ((double)wallclocktime / (double)3600));
"""

PROJECT_NAMES = {
    "clas12": ["clas12"],
    "eic": ["eic", "epic"],
    "eht": ["eht"],
    "futurecolliders": ["futurecolliders"],
    "gluex": ["gluex"],
    "icecube": ["icecube"],
    "koto": ["koto"],
    "ligo": ["ligo_orientation", "igwn_staff"],
    "moller": ["moller"],
    "redtop": ["redtop"],
    "rnog": ["rnog"],
    "scdms": ["scdms"],
    "solid": ["solid"],
    "spt": ["spt", "spt.all"],
    "xenon": ["xenon"],
}

CPU_PROJECT_SPECIFIC_RESOURCES = {
    "icecube": [
        "CA_SFU_T2",
        "DESY-ZN",
        "FZK-LCG2",
        "NBI_T3",
        "PDX-Coeus-CE1",
        "UCSDT2",
        "UKI-NORTHGRID-MAN-HEP",
        "wuppertalprod",
    ],
    "spt": ["UIUC-ICC-SPT"],
    "xenon": [
        "IN2P3-CC",
        "INFN-T1",
        "NIKHEF-ELPROD",
        "SURFsara",
    ]
}

GPU_PROJECT_SPECIFIC_RESOURCES = {
    "icecube": [
        "PDX-Coeus-CE1",
        "SDSC-Cloud",
        "UKI-LT2-QMUL",
    ]
}

IS_OSPOOL_JOB_FILTER = (
    (
        (
            Q("terms", ScheddName__keyword=list(get_ospool_aps(include_jupyter_aps=False, pickled_ap_collector_hosts_cache=PICKLED_AP_COLLECTOR_HOSTS_CACHE_FILE))) &
            ~Q("exists", field="LastRemotePool")
        ) | (
            Q("terms", LastRemotePool__keyword=list(OSPOOL_COLLECTORS))
        )
    ) &
    ~(
        Q("exists", field="TargetAnnexName") |
        Q("terms", ResourceName=["Local Job", "2"])
    )
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch CPU/GPU usage metrics for collaboration-level OSPool projects.\n"
            "Output is CSV: Project, Pool, CPU Hours, CPU Jobs, GPU Hours, GPU Jobs.\n\n"
            "NOTE: Currently requires direct access to the internal Elasticsearch instance"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)

    parser.add_argument("--start", type=valid_date, metavar="YYYY-MM-DD",
                        help="Start of the reporting period (inclusive). Defaults to yesterday.")
    parser.add_argument("--end", type=valid_date, metavar="YYYY-MM-DD",
                        help="End of the reporting period (exclusive). Defaults to one day after --start.")
    parser.add_argument("outfile", type=Path,
                        help="Output CSV file path.")

    return parser.parse_args()


def get_base_query(
        index: str,
        start: datetime,
        end: datetime
    ) -> Search:

    runtime_mappings = {
        "ResourceName": {
            "type": "keyword",
            "script": {
                "source": RESOURCE_NAME_SCRIPT_SRC,
            }
        },
        "ProjectNameLower": {
            "type": "keyword",
            "script": {
                "source": PROJECT_NAME_SCRIPT_SRC,
            }
        },
    }

    query = Search(index=index) \
                .extra(size=0, track_scores=False, track_total_hits=True) \
                .extra(runtime_mappings=runtime_mappings) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("range", RemoteWallClockTime={"gt": 0}) \
                .query(~Q("terms", JobUniverse=[7, 12]))

    return query


def add_cpu_hours(base: Search) -> Search:
    """Add a CpuHours runtime mapping and sum aggregation to a query."""
    query = deepcopy(base)
    runtime_mappings = query.to_dict().get("runtime_mappings", {})
    runtime_mappings["CpuHours"] = {
        "type": "double",
        "script": {
            "source": CPU_HOURS_SCRIPT_SRC
        }
    }
    query = query.extra(runtime_mappings=runtime_mappings)

    cpu_hours_agg = A(
        "sum",
        field="CpuHours",
    )
    query.aggs.metric("cpu_hours", cpu_hours_agg)
    return query


def add_gpu_hours(base: Search) -> Search:
    """Add a GpuHours runtime mapping and sum aggregation to a query. Filters to jobs that requested GPUs."""
    query = deepcopy(base).filter("range", RequestGpus={"gt": 0})
    runtime_mappings = query.to_dict().get("runtime_mappings", {})
    runtime_mappings["GpuHours"] = {
        "type": "double",
        "script": {
            "source": GPU_HOURS_SCRIPT_SRC
        }
    }
    query = query.extra(runtime_mappings=runtime_mappings)

    gpu_hours_agg = A(
        "sum",
        field="GpuHours",
    )
    query.aggs.metric("gpu_hours", gpu_hours_agg)
    return query


def get_usage(client: elasticsearch.Elasticsearch, q: Search, gpu: bool = False) -> dict:
    """Execute a query and return a dict with job count and CPU or GPU hours.

    Returns an empty dict on error.
    """
    usage = {}
    try:
        r = q.using(client).execute()
        if gpu:
            label = "gpu_hours"
            hours = r.aggregations.gpu_hours.value
        else:
            label = "cpu_hours"
            hours = r.aggregations.cpu_hours.value
        usage = {
            "jobs": r.hits.total.value,
            label: hours,
        }
    except elasticsearch.exceptions.ElasticsearchException as e:
        print_es_error(e, file=sys.stderr)
    except (KeyError, AttributeError, NameError):
        print(json.dumps(r, indent=2), file=sys.stderr)
    return usage


### PROJECTS


def ospool_cpu_only(client: elasticsearch.Elasticsearch, q: Search, project: str) -> dict:
    """Return OSPool CPU usage for projects whose jobs run exclusively on OSPool."""
    usage = {}

    q = add_cpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES[project]))

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER)
    usage["OSPool"] = get_usage(client, ospool_q)

    return usage


def clas12_cpu(client: elasticsearch.Elasticsearch, q: Search) -> dict:
    usage = {}

    q = add_cpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES["clas12"]))

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER)
    usage["OSPool"] = get_usage(client, ospool_q)

    jlab_q = q.query(~IS_OSPOOL_JOB_FILTER)
    usage["JLAB"] = get_usage(client, jlab_q)

    return usage


def eic_cpu(client: elasticsearch.Elasticsearch, q: Search) -> dict:
    usage = {}

    q = add_cpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES["eic"]))

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER)
    usage["OSPool"] = get_usage(client, ospool_q)

    jlab_q = q.query(~IS_OSPOOL_JOB_FILTER)
    usage["JLAB"] = get_usage(client, jlab_q)

    return usage


def gluex_cpu(client: elasticsearch.Elasticsearch, q: Search) -> dict:
    usage = {}

    q = add_cpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES["gluex"]))

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER)
    usage["OSPool"] = get_usage(client, ospool_q)

    jlab_q = q.query(~IS_OSPOOL_JOB_FILTER)
    usage["JLAB"] = get_usage(client, jlab_q)

    return usage


def icecube_cpu(client: elasticsearch.Elasticsearch, q: Search) -> dict:
    usage = {}

    q = add_cpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES["icecube"]))
    icecube_pool_filter = Q("terms", ResourceName=CPU_PROJECT_SPECIFIC_RESOURCES["icecube"])

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER & ~icecube_pool_filter)
    usage["OSPool"] = get_usage(client, ospool_q)

    icecube_q = q.query(icecube_pool_filter | ~IS_OSPOOL_JOB_FILTER)
    usage["IceCube"] = get_usage(client, icecube_q)

    return usage


def icecube_gpu(client: elasticsearch.Elasticsearch, q: Search) -> dict:
    usage = {}

    q = add_gpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES["icecube"]))
    icecube_pool_filter = Q("terms", ResourceName=GPU_PROJECT_SPECIFIC_RESOURCES["icecube"] + CPU_PROJECT_SPECIFIC_RESOURCES["icecube"])

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER & ~icecube_pool_filter)
    usage["OSPool"] = get_usage(client, ospool_q, gpu=True)

    icecube_q = q.query(IS_OSPOOL_JOB_FILTER & icecube_pool_filter)
    usage["IceCube"] = get_usage(client, icecube_q, gpu=True)

    return usage


def spt_cpu(client: elasticsearch.Elasticsearch, q: Search) -> dict:
    usage = {}

    q = add_cpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES["spt"]))
    spt_pool_filter = Q("terms", ResourceName=CPU_PROJECT_SPECIFIC_RESOURCES["spt"])

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER & ~spt_pool_filter)
    usage["OSPool"] = get_usage(client, ospool_q)

    spt_q = q.query(IS_OSPOOL_JOB_FILTER & spt_pool_filter)
    usage["SPT"] = get_usage(client, spt_q)

    return usage


def xenon_cpu(client: elasticsearch.Elasticsearch, q: Search) -> dict:
    usage = {}

    q = add_cpu_hours(q.filter("terms", ProjectNameLower=PROJECT_NAMES["xenon"]))
    xenon_pool_filter = Q("terms", ResourceName=CPU_PROJECT_SPECIFIC_RESOURCES["xenon"])

    ospool_q = q.query(IS_OSPOOL_JOB_FILTER & ~xenon_pool_filter)
    usage["OSPool"] = get_usage(client, ospool_q)

    xenon_q = q.query(IS_OSPOOL_JOB_FILTER & xenon_pool_filter)
    usage["XENON"] = get_usage(client, xenon_q)

    return usage


###


def write_csv_rows(writer: csv.writer, project: str, cpu_usage: dict, gpu_usage: dict | None = None) -> None:
    """Write one CSV row per pool for a project, merging CPU and GPU usage by pool name."""
    gpu_usage = gpu_usage or {}
    pools = list(cpu_usage) + [p for p in gpu_usage if p not in cpu_usage]
    for pool in pools:
        cpu = cpu_usage.get(pool, {})
        gpu = gpu_usage.get(pool, {})
        cpu_hours = short(cpu["cpu_hours"]) if "cpu_hours" in cpu else ""
        cpu_jobs = short(cpu["jobs"]) if "cpu_hours" in cpu else ""
        gpu_hours = short(gpu["gpu_hours"]) if "gpu_hours" in gpu else ""
        gpu_jobs = short(gpu["jobs"]) if "gpu_hours" in gpu else ""
        writer.writerow([project, pool, cpu_hours, cpu_jobs, gpu_hours, gpu_jobs])


def main():
    args = parse_args()

    es_args = {}
    if args.es_config_file:
        es_args = json.load(args.es_config_file.open())
    else:
        es_args = {arg: v for arg, v in vars(args).items() if arg.startswith("es_")}
    if es_args.get("es_password_file"):
        es_args["es_pass"] = es_args.pop("es_password_file").open().read().rstrip()
    index = es_args.pop("es_index", "osg-schedd-*")

    if args.start is None:
        args.start = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if args.end is None:
        args.end = args.start + timedelta(days=1)

    es = connect(**es_args)
    es.info()

    q = get_base_query(index, args.start, args.end)

    with args.outfile.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Project", "Pool", "CPU Hours", "CPU Jobs", "GPU Hours", "GPU Jobs"])

        write_csv_rows(writer, "CLAS12", clas12_cpu(es, q))
        write_csv_rows(writer, "EIC/ePIC", eic_cpu(es, q))
        write_csv_rows(writer, "Event Horizon Telescope", ospool_cpu_only(es, q, "eht"))
        write_csv_rows(writer, "Future Colliders", ospool_cpu_only(es, q, "futurecolliders"))
        write_csv_rows(writer, "GlueX", gluex_cpu(es, q))
        write_csv_rows(writer, "IceCube", icecube_cpu(es, q), icecube_gpu(es, q))
        write_csv_rows(writer, "KOTO", ospool_cpu_only(es, q, "koto"))
        write_csv_rows(writer, "LIGO-OSG", ospool_cpu_only(es, q, "ligo"))
        write_csv_rows(writer, "MOLLER", ospool_cpu_only(es, q, "moller"))
        write_csv_rows(writer, "REDTOP", ospool_cpu_only(es, q, "redtop"))
        write_csv_rows(writer, "RNO-G", ospool_cpu_only(es, q, "rnog"))
        write_csv_rows(writer, "Super CDMS", ospool_cpu_only(es, q, "scdms"))
        write_csv_rows(writer, "SoLID", ospool_cpu_only(es, q, "solid"))
        write_csv_rows(writer, "SPT-3G", spt_cpu(es, q))
        write_csv_rows(writer, "XENON", xenon_cpu(es, q))


if __name__ == "__main__":
    main()
