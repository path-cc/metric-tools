#!/bin/bash

. ~/venv/bin/activate

exec python "$(dirname "$0")"/bumbly.py "$@"
