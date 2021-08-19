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
    help_msg = """Usage: {0} [--projects] LIST,OF,PROJECTS
"""
    stream.write(help_msg.format(sys.argv[0]))


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("--projects", 
        help="Comma-separated list of project names (ie. HTCONDOR,SOFTWARE)",
        default="HTCONDOR,SOFTWARE")
    args = parser.parse_args()

    projects = args.projects

    return {
        "projects": projects
    }


def main():

    # Parse arguments. Assume any error handling happens in parse_args()
    try:
        args = parse_args()
    except Exception as err:
        print(f"Failed to parse arguments: {err}", file=sys.stderr)

    projects = args["projects"]

    # Connect to Jira
    options = {"server": "https://opensciencegrid.atlassian.net"}
    jira = JIRA(options)

    # Output CSV headers
    print("Issue key,Assignee,Original Due Date,Current Due Date,Number of Due Date Changes")

    # Now iterate over the projects and list their issues
    for project in projects.split(","):
        issues = jira.search_issues(f"project = {project} AND status != Backlog", expand="changelog", maxResults=False)
        #issues = jira.search_issues(f"key = HTCONDOR-344", expand="changelog", maxResults=False)
        for issue in issues:
            duedate_current = issue.fields.duedate
            duedate_original = issue.fields.duedate
            duedate_changes = 0
            issue_changelog = issue.changelog.histories
            # Iterate over the changelog in reverse so we get the oldest changes first
            for change in issue_changelog[::-1]:
                for item in change.items:
                    if item.field == "duedate":
                        if duedate_changes == 0:
                            duedate_original = getattr(item, "from")
                        duedate_changes += 1
                        duedate_current = item.to
            print(f"{issue.key},{issue.fields.assignee},{duedate_original},{duedate_current},{duedate_changes}")


if __name__ == "__main__":
    main()
