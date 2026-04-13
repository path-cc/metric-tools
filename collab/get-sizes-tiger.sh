#!/bin/bash

do_exec () {
    kubectl --context=tiger --namespace=osdf-prod exec deploy/collab-shared-osdf-pelican-origin -c pelican-origin -- sh -c "$*"
}


(
echo "started REDTOP/public"
result=$(do_exec 'du -bs /mnt/origin/REDTOP/public') &&
# Don't print anything until we have the results.
echo "*** REDTOP/public ***" &&
echo "$result"
) &

(
echo "started public"
result=$(do_exec 'du -bs /mnt/origin/public/*') &&
echo "*** public ***" &&
echo "$result"
) &

(
echo "started collaborations"
result=$(do_exec 'du -bs /mnt/origin/collaborations/*') &&
echo "*** collaborations ***" &&
echo "$result"
) &

wait
