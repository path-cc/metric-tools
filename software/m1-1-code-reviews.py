#!/usr/bin/env python3

import argparse
import json
import re
import sys
import time

from datetime import datetime
from jira import JIRA


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def print_help(stream=sys.stderr):
    help_msg = """Usage: {0} --startdate YYYY-MM-DD --enddate YYYY-MM-DD [--detailed]
"""
    stream.write(help_msg.format(sys.argv[0]))


def parse_args():

    # The only syntax that is acceptable is:
    # <this> --startdate YYYY-MM-DD --enddate YYYY-MM-DD [--detailed]

    if len(sys.argv) not in [5, 6]:
        print_help()
        sys.exit(-1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--startdate", help="Start date")
    parser.add_argument("--enddate", help="End date")
    parser.add_argument("--detailed", help="Show detailed results", action="store_true")
    args = parser.parse_args()

    start_datetime = args.startdate
    end_datetime = args.enddate

    # Validate input
    if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', start_datetime):
        print("Error: --startdate argument must take YYYY-MM-DD format")
        sys.exit(-1)
    if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', end_datetime):
        print("Error: --enddate argument must take YYYY-MM-DD format")
        sys.exit(-1)
    try:
        start_datetime = datetime.strptime(start_datetime + " 00:00:01", "%Y-%m-%d %H:%M:%S")
    except:
        print(f"Error: Start date {start_datetime} is not a valid date")
        sys.exit(-1)
    try:
        end_datetime = datetime.strptime(end_datetime + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    except:
        print(f"Error: End date {end_datetime} is not a valid date")
        sys.exit(-1)

    return {
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "detailed": args.detailed
    }


def main():

    # Parse arguments. Assume any error handling happens in parse_args()
    try:
        args = parse_args()
    except Exception as err:
        print(f"Failed to parse arguments: {err}", file=sys.stderr)

    start_datetime = args["start_datetime"]
    end_datetime = args["end_datetime"]
    detailed = args["detailed"]

    num_issues_done = 0
    num_done_issues_code_reviewed = 0

    # Connect to Jira
    options = {"server": "https://opensciencegrid.atlassian.net"}
    jira = JIRA(options)

    # We need to find the key of the "Development" field, to determine if an issue has commits
    development_field_key = None
    fields = jira.fields()
    name_map = { field['name']:field['id'] for field in fields }
    if "Development" in name_map:
        development_field_key = name_map["Development"]

    # Iterate over all completed issues in the HTCONDOR project
    issues = jira.search_issues("project = HTCONDOR AND type in (Improvement, Bug) AND status = Done", expand="changelog", maxResults=False)
    for issue in issues:
        issue_changelog = issue.changelog.histories
        issue_isdone = False
        for change in issue_changelog:
            for item in change.items:
                # Issues can be marked Done multiple times, so break after first occurrence
                if issue_isdone:
                    break
                # Check if this change was setting the status to Done
                if item.field == "status" and item.toString == "Done":
                    # Check if this issue has any code commits. If not, don't count it.
                    development_data = getattr(issue.fields, development_field_key)
                    if development_data == "{}":
                        continue
                    # Strip the time offset from the change.created string
                    changed = change.created[0:change.created.rfind("-")]
                    # Was this issue set to Done status between the provided end and start dates?
                    changed_datetime = datetime.strptime(changed, "%Y-%m-%dT%H:%M:%S.%f")
                    if changed_datetime > start_datetime and changed_datetime < end_datetime:
                        if detailed is True:
                            print(f"{issue.key}: {issue.fields.summary}, Marked Done: {changed_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
                        num_issues_done += 1
                        issue_isdone = True
                        # Now check the issue comments for a "code review" text entry
                        comments = jira.comments(issue)
                        if len(comments) > 0:
                            for comment in comments:
                                if "code review" in comment.body.lower()[0:20]:
                                    num_done_issues_code_reviewed += 1
                                    if detailed is True:
                                        print("\tThis issue was code reviewed")
                                    break

    print(f"\nBetween {start_datetime.strftime('%Y-%m-%d')} and {end_datetime.strftime('%Y-%m-%d')}:\n")
    print(f"{num_issues_done} HTCONDOR issues were marked Done")
    print(f"{num_done_issues_code_reviewed} of these completed issues were code reviewed")
    if num_issues_done > 0:
        print(f"Code review rate: {round(num_done_issues_code_reviewed*100/num_issues_done, 2)}%\n")
    else:
        print(f"No issues marked Done between the dates specified.")


if __name__ == "__main__":
    main()