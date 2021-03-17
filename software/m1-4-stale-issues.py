#!/usr/bin/env python3

import argparse
import re
import sys

from datetime import datetime, timedelta
from jira import JIRA


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def print_help(stream=sys.stderr):
    help_msg = """Usage: {0} --date YYYY-MM-DD [--detailed]
"""
    stream.write(help_msg.format(sys.argv[0]))


def parse_args():

    # The only syntax that is acceptable is:
    # <this> --date YYYY-MM-DD [--detailed]

    if len(sys.argv) not in [3, 4]:
        print_help()
        sys.exit(-1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date to compare against for stale issues")
    parser.add_argument("--detailed", help="Show detailed results", action="store_true")
    args = parser.parse_args()

    target_datetime = args.date

    # Validate input
    if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', target_datetime):
        print("Error: -date argument must take YYYY-MM-DD format")
        sys.exit(-1)
    try:
        target_datetime = datetime.strptime(target_datetime, "%Y-%m-%d")
    except:
        print(f"Error: {target_datetime} is not a valid date")
        sys.exit(-1)

    return {
        "target_datetime": target_datetime,
        "detailed": args.detailed
    }


def main():

    # Parse arguments. Assume any error handling happens in parse_args()
    try:
        args = parse_args()
    except Exception as err:
        print(f"Failed to parse arguments: {err}", file=sys.stderr)

    target_datetime = args["target_datetime"]
    detailed = args["detailed"]

    # Connect to Jira
    options = {"server": "https://opensciencegrid.atlassian.net"}
    jira = JIRA(options)

    num_open_issues = 0
    stale_datetime = target_datetime - timedelta(days=10)
    stale_issues = []
    # Iterate over all open issues by HTCSS software developers
    issues = jira.search_issues(f"project = HTCONDOR AND type in (Improvement, Bug, Documentation, Subtask, Sub-task) AND status not in (Backlog, Done, Abandoned, Blocked) AND assignee in (\"Greg Thain\", \"Jaime Frey\", \"Mark Coatsworth\", \"Tim Theisen\", \"John (TJ) Knoeller\", \"Todd L Miller\", \"Todd Tannenbaum\", \"Zach Miller\") AND createdDate <= {target_datetime.strftime('%Y-%m-%d')}", maxResults=False)
    for issue in issues:
        updated = issue.fields.updated[0:issue.fields.updated.rfind("-")]
        updated_datetime = datetime.strptime(updated, "%Y-%m-%dT%H:%M:%S.%f")
        if detailed is True:
            print(f"{issue.key}: {issue.fields.summary}, Updated: {datetime.strftime(updated_datetime, '%Y-%m-%d %H:%M:%S')}")
        has_active_subtasks = False
        if len(issue.fields.subtasks) > 0:
            for subtask in issue.fields.subtasks:
                subtask_status = str(subtask.fields.status)
                if detailed is True:
                    print(f"\tSubtask {subtask.key} status: {subtask_status}")
                if subtask_status not in ["Backlog", "Done", "Abandoned", "Blocked"]:
                    has_active_subtasks = True
                    continue
        if has_active_subtasks is False:
            num_open_issues += 1
            if updated_datetime < stale_datetime:
                if detailed is True:
                    print("\tThis issue is stale!")
                stale_issues.append(issue.key)
        else:
            if detailed is True:
                print(f"\tThis issue has active subtasks, skipping it.")

    # All done! Output results
    print(f"\nAs of {target_datetime.strftime('%Y-%m-%d')}:\n")

    print(f"{num_open_issues} issues are open")
    print(f"{len(stale_issues)} open issues have not been updated in the last 10 days")
    print(f"Percent open issues that are stale: {round(len(stale_issues)*100/num_open_issues, 2)}%\n")


if __name__ == "__main__":
    main()