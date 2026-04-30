from unittest.mock import MagicMock, patch

import pytest

from collab_types import Error, Origin
from k8s import (
    check_namespace_access,
    examine_pod,
    is_origin_container,
    namespace_for_context,
)


def test_is_origin_container():
    # Matching images for pelican origin or osdf-origin
    assert (
        is_origin_container(
            {"image": "hub.opensciencegrid.org/pelican/osdf-origin:latest"}
        )
        is True
    )
    assert (
        is_origin_container({"image": "hub.opensciencegrid.org/pelican/origin:v1.0.0"})
        is True
    )

    # Non-matching images should return False
    assert (
        is_origin_container({"image": "hub.opensciencegrid.org/pelican/cache:latest"})
        is False
    )
    assert is_origin_container({"image": "nginx:latest"}) is False

    # Images with fewer than 3 / separated parts (no registry)
    # The implementation does parts = re.split(r"[:@/]", full_image); image = parts[2]
    # So "pelican/origin" -> ["pelican", "origin"] -> IndexError
    assert is_origin_container({"image": "pelican/origin"}) is False
    assert is_origin_container({"image": "origin"}) is False

    # Missing image key should return False
    assert is_origin_container({}) is False


def test_examine_pod():
    # Mock _current_context and _namespace_for_context to avoid subprocess calls
    with (
        patch("k8s._current_context", return_value="my-context"),
        patch("k8s.namespace_for_context", return_value="my-ns"),
    ):

        # Pod with origin container should be recognized and returned
        pod_origin = {
            "metadata": {"name": "pod-1-2"},
            "spec": {
                "containers": [
                    {
                        "name": "c1",
                        "image": "hub.opensciencegrid.org/pelican/osdf-origin:latest",
                    }
                ]
            },
        }
        origin = examine_pod(pod_origin)
        assert origin == Origin(
            namespace="my-ns",
            pod_name="pod-1-2",
            container_name="c1",
            context="my-context",
        )

        # Pod with non-origin container should return None
        pod_no_origin = {
            "metadata": {"name": "pod-1-2"},
            "spec": {"containers": [{"name": "c2", "image": "nginx:latest"}]},
        }
        assert examine_pod(pod_no_origin) is None

        # Pod missing metadata should return None
        assert examine_pod({}) is None


@patch("k8s.run")
def test_namespace_for_context(mock_run):
    # Active context with * marker should parse correctly
    mock_run.return_value = MagicMock(stdout="*  ctx-1  cluster-1  user-1  ns-1\n")
    assert namespace_for_context("ctx-1") == "ns-1"

    # Non-active context should also parse correctly
    mock_run.return_value = MagicMock(stdout="   ctx-2  cluster-2  user-2  ns-2\n")
    assert namespace_for_context("ctx-2") == "ns-2"

    # Unparseable output should raise Error
    mock_run.return_value = MagicMock(stdout="bad line\n")
    with pytest.raises(Error, match="Could not determine namespace"):
        namespace_for_context("ctx-3")


@patch("k8s.run")
def test_check_namespace_access(mock_run, capsys):
    # Both checks passing should return True
    mock_run.return_value = MagicMock(returncode=0, stdout="yes")
    assert check_namespace_access("cluster", "context", "ns") is True

    # First check failing should return False and print error
    mock_run.side_effect = [
        MagicMock(returncode=1, stdout="", stderr="forbidden"),
        MagicMock(returncode=0, stdout="yes"),
    ]
    assert check_namespace_access("cluster", "context", "ns") is False
    captured = capsys.readouterr().err
    assert "ERROR: insufficient permissions" in captured
    assert "forbidden" in captured
