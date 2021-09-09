#!/usr/bin/env python3

import argparse
import json
import re
import sys
import time

from datetime import datetime
from jira import JIRA


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("-projects", 
        help="Comma-separated list of project names (ie. HTCONDOR,SOFTWARE)",
        default="HTCONDOR,SOFTWARE")
    parser.add_argument("-o", 
        help="Output file",
        default=None)
    args = parser.parse_args()

    return {
        "projects": args.projects,
        "output_file": args.o
    }


def main():

    args = parse_args()
    projects = args["projects"]
    output_file = args["output_file"]

    # Connect to Jira
    options = {"server": "https://opensciencegrid.atlassian.net"}
    jira = JIRA(options)

    # Output CSV headers
    output_data = "Issue key,Assignee,Original Due Date,Current Due Date,Number of Due Date Changes\n"

    # Now iterate over the projects and list their issues
    for project in projects.split(","):
        issues = jira.search_issues(f"project = {project} AND statusCategory != \"To Do\"", expand="changelog", maxResults=False)
        for issue in issues:
            duedate_current = issue.fields.duedate
            duedate_original = issue.fields.duedate
            duedate_changes = 0
            issue_changelog = issue.changelog.histories
            # Iterate over the changelog in reverse so we get the oldest changes first
            for change in reversed(issue_changelog):
                for item in change.items:
                    if item.field == "duedate":
                        if duedate_changes == 0:
                            duedate_original = getattr(item, "from")
                        duedate_changes += 1
                        duedate_current = item.to
            output_data += f"{issue.key},{issue.fields.assignee},{duedate_original},{duedate_current},{duedate_changes}\n"

    # Write results to output_file (or stdout is no output file defined)
    if output_file is None:
        print(output_data)
    else:
        file = open(output_file, "w")
        file.write(output_data)
        file.close()

if __name__ == "__main__":
    main()
