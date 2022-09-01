#!/bin/bash
set -ex

# the following env vars should be set from the github workflow:

# GITHUB_REPOSITORY GITHUB_WORKFLOW GITHUB_RUN_ID GITHUB_RUN_NUMBER TZ


cd "$(dirname "$0")"


# venv setup

python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt


# run metrics

START_DATE=$(date -d "30 days ago" +%F)
END_DATE=$(date -d yesterday +%F)

cd campuses-with-active-researchers
{ ./campuses-with-active-researchers.py --csv $START_DATE $END_DATE
} > ../campuses-with-active-researchers.csv

cd ../campus-contributions
{ ./campus-contributions --json $START_DATE $END_DATE
} > ../campus-contributions.json

cd ../osg-cpu-hours
./osg-cpu-hours.py -o ../osg-cpu-hours.json

cd ../osg-project-waittime
./calculate-waittime.py ../osg-waittime.csv $START_DATE $END_DATE

cd ../software
./due-date-changes.py -o ../software-due-date-changes.csv

cd ../connect-origin-users
./connect-origin-users.py -o ../connect-origin-users.json

cd ..


# Commit files

OUTFILES=(
  campus-contributions.json
  campuses-with-active-researchers.csv
  osg-cpu-hours.json
  osg-waittime.csv
  software-due-date-changes.csv
  connect-origin-users.json
)
git clone --depth=1 https://github.com/path-cc/metrics
mv ${OUTFILES[@]} metrics
cd metrics
git config --local user.email "help@opensciencegrid.org"
git config --local user.name "Automatic metrics publish"
mkdir -p historical

NOWS=$(date +%s)
NOW=$(date -d @$NOWS +"%F at %H:%M %Z")
NOW2=$(date -d @$NOWS +"%F_%H%M")

for f in ${OUTFILES[@]}; do
  git add $f
  cp $f historical/$NOW2.$f
  git add historical/$NOW2.$f
done

sed -i "/Last generated:/s/:.*/: $NOW/" README.md
git add README.md

../mk-historical-md.sh > historical-results.md
git add historical-results.md

git commit -m "nightly metrics update (from ${GITHUB_REPOSITORY} ${GITHUB_WORKFLOW} ${GITHUB_RUN_ID}.${GITHUB_RUN_NUMBER})"

