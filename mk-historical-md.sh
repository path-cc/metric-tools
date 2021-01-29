#!/bin/bash
set -e

echo "Historical Metric Results"
echo "========================="
for f in $(ls -r historical); do
  echo
  echo "[\`$f\`]($f)"
done

