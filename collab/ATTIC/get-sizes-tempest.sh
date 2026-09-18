#!/bin/bash


# shellcheck disable=SC2016
echo "*** Project ***"
kubectl --context=tempest --namespace=osgprod exec deploy/uc-project-osdf-pelican-origin -c pelican-origin -- sh -c 'cd /run/pelican/xrootd/origin/export/ospool/uc-shared/project || exit; for dir in */; do size=$(getfattr -n ceph.dir.rbytes --only-values "$dir"); printf "%-40s %15s %8s\n" "$(basename "$dir")" "$size" "$(( size >> 30 ))"; done'

echo "*** Public ***"
kubectl --context=tempest --namespace=osgprod exec deploy/uc-public-osdf-pelican-origin -c pelican-origin -- sh -c 'cd /ospool/uc-shared/public || exit; for dir in */; do size=$(getfattr -n ceph.dir.rbytes --only-values "$dir"); printf "%-40s %15s %8s\n" "$(basename "$dir")" "$size" "$(( size >> 30 ))"; done'
