import json
import re
import subprocess
import sys
from collections.abc import Generator
from typing import Optional

from collab_types import Error, Origin
from helpers import _run

# Image name substrings that identify a Pelican Origin container.
ORIGIN_IMAGE_NAMES: tuple[str, ...] = ("osdf-origin", "origin")


def _run_in_origin(
    origin: Origin,
    args: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess:

    cmd = [
        "kubectl",
        "--context",
        origin.context,
        "exec",
        "--namespace",
        origin.namespace,
        "--container",
        origin.container_name,
        origin.pod_name,
    ]
    return _run(cmd + ["--"] + args, check=check)


def _check_namespace_access(cluster: str, context: str, namespace: str) -> bool:
    """
    Return True if we have permission to get pods and exec into pods in *namespace*.
    Prints an error to stderr and returns False if either check fails.
    """
    checks = [
        [
            "kubectl",
            "--context",
            context,
            "auth",
            "can-i",
            "get",
            "pods",
            "--namespace",
            namespace,
        ],
        [
            "kubectl",
            "--context",
            context,
            "auth",
            "can-i",
            "create",
            "pods/exec",
            "--namespace",
            namespace,
        ],
    ]
    for cmd in checks:
        ret = _run(cmd, check=False)
        if ret.returncode != 0 or ret.stdout.strip() != "yes":
            print(
                f"ERROR: insufficient permissions in cluster={cluster!r} "
                f"namespace={namespace!r}: {' '.join(cmd[5:])} -> "
                f"{ret.stdout.strip() or ret.stderr.strip()}",
                file=sys.stderr,
            )
            return False
    return True


def _current_context() -> str:
    """Return the current Kubernetes context, or raise Error if it cannot be determined."""
    ret = _run(["kubectl", "config", "current-context"])
    context = ret.stdout.strip()
    if not context:
        raise Error("Could not determine current Kubernetes context")
    return context


def _namespace_for_context(context: Optional[str] = None) -> str:
    """
    Return the namespace configured for *context*, or raise Error if it cannot
    be determined.  If *context* is not given, uses the current context.
    """
    if context is None:
        context = _current_context()
    ret = _run(["kubectl", "config", "get-contexts", "--no-headers", context])
    for line in ret.stdout.splitlines():
        parts = re.split(r"\s+", line.strip())
        # Strip leading '*' marker for the active context
        if parts and parts[0] == "*":
            parts = parts[1:]
        # Columns: NAME CLUSTER AUTHINFO [NAMESPACE]
        if len(parts) >= 4:
            return parts[3]
    raise Error(f"Could not determine namespace for context {context!r}")


def _is_origin_container(container: dict) -> bool:
    """
    Return True if *container* (a Kubernetes container spec dict) looks like a
    Pelican Origin container based on its image name (one of ORIGIN_IMAGE_NAMES).
    """
    full_image: str = container.get("image", "")
    # This assumes that image names always have the registry
    parts = re.split(r"[:@/]", full_image)
    try:
        image = parts[2]
        return image in ORIGIN_IMAGE_NAMES
    except IndexError:
        return False


def examine_pod(
    pod: dict,
    context: Optional[str] = None,
    namespace: Optional[str] = None,
) -> Optional[Origin]:
    """
    Examine a single pod manifest (as returned by the Kubernetes API / kubectl)
    and determine whether it hosts a Pelican Origin container.

    If it does, return a :class:`Origin`.  Returns *None* if the pod does not
    contain a recognised Pelican Origin container.

    Parameters
    ----------
    pod:
        A dict representing the pod's JSON manifest (e.g. from
        ``kubectl get pod <n> -o json``).
    context:
        The Kubernetes context.  Defaults to the current context.
    namespace:
        The Kubernetes namespace.  Defaults to the namespace configured for
        *context*.

    Returns
    -------
    Origin | None
    """
    if context is None:
        context = _current_context()
    if namespace is None:
        namespace = _namespace_for_context(context)

    try:
        pod_name: str = pod["metadata"]["name"]
        containers: list[dict] = pod["spec"]["containers"]
    except KeyError:
        return None

    for container in containers:
        if not _is_origin_container(container):
            continue

        container_name: str = container["name"]

        return Origin(
            namespace=namespace,
            pod_name=pod_name,
            container_name=container_name,
            context=context,
        )

    return None


def find_pelican_origin_pods(
    context: Optional[str] = None,
    namespace: Optional[str] = None,
) -> Generator[Origin]:
    """
    List all pods in *namespace* and return information about every pod that
    contains a Pelican Origin container with a discoverable Pelican Server
    binary.

    Parameters
    ----------
    context:
        The Kubernetes context to use.  Defaults to the current context.
    namespace:
        The Kubernetes namespace to search.  Defaults to the namespace
        configured for *context*.

    Yields
    ------
    Origin
        The info about one pod: the namespace, name, container name, and path
        to pelican-server binary inside the container.

    Raises
    ------
    Error
        If the context or namespace cannot be determined.
    subprocess.CalledProcessError
        If the initial ``kubectl get pods`` call fails (e.g. bad namespace,
        missing credentials).
    json.JSONDecodeError
        If kubectl returns unexpected output.
    """
    if context is None:
        context = _current_context()
    if namespace is None:
        namespace = _namespace_for_context(context)

    result = _run(
        [
            "kubectl",
            "--context",
            context,
            "get",
            "pods",
            "--namespace",
            namespace,
            "--output",
            "json",
        ]
    )

    pod_list: dict = json.loads(result.stdout)
    pods: list[dict] = pod_list.get("items", [])

    for pod in pods:
        pod_name = pod.get("metadata", {}).get("name", "<unknown>")
        info = examine_pod(pod, context, namespace)
        if info is not None:
            phase = pod.get("status", {}).get("phase", "<unknown>")
            if phase == "Running":
                yield info
            else:
                print(
                    f"Origin {pod_name!r}: not Running (phase={phase!r})",
                    file=sys.stderr,
                    flush=True,
                )


def interactive_exec(origin: Origin, cmd=("bash",)) -> int:
    """
    Debugging function: interactively exec into an origin to take a look around.

    Parameters
    ----------
    origin
        The origin to exec into

    cmd
        The command to run

    Returns
    -------
    int
        The return code of the process.
    """
    if isinstance(cmd, str):
        cmd = [cmd]
    proc = subprocess.Popen(
        [
            "kubectl",
            "--context",
            origin.context,
            "exec",
            "--namespace",
            origin.namespace,
            "--container",
            origin.container_name,
            "-it",
            origin.pod_name,
            "--",
        ]
        + list(cmd)
    )
    return proc.wait()
